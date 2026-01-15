#!/usr/bin/env bash
set -euo pipefail

IMAGE="ghcr.io/trentten/project_dragon"
COMPOSE_FILE="deploy/portainer/docker-compose.yml"
VERSION_FILE="src/project_dragon/_version.py"

die() { echo "ERROR: $*" >&2; exit 1; }

latest_tag() {
  git tag -l 'v*.*.*' --sort=-v:refname | head -n 1 || true
}

parse_tag() {
  local t="$1"
  [[ "$t" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] || die "Tag '$t' doesn't match vMAJOR.MINOR.PATCH"
  echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]}"
}

bump() {
  local mode="$1" t="$2"
  read -r major minor patch < <(parse_tag "$t")

  case "$mode" in
    patch) echo "v${major}.${minor}.$((patch + 1))" ;;
    minor) echo "v${major}.$((minor + 1)).0" ;;
    major) echo "v$((major + 1)).0.0" ;;
    *) die "Unknown bump mode: $mode" ;;
  esac
}

write_version_file() {
  local tag="$1"
  local ver="${tag#v}"  # strip leading v
  mkdir -p "$(dirname "$VERSION_FILE")"
  cat > "$VERSION_FILE" <<EOV
__version__ = "${ver}"
EOV
}

MODE="patch"
MSG="release: update"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --major) MODE="major"; shift ;;
    --minor) MODE="minor"; shift ;;
    --patch) MODE="patch"; shift ;;
    -m|--message) MSG="${2:-}"; [[ -n "$MSG" ]] || die "Missing message"; shift 2 ;;
    *) MSG="$1"; shift ;;
  esac
done

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Not in a git repo"
[[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"

echo "== Git status =="
git status --porcelain || true

# Commit current changes (if any)
if [[ -n "$(git status --porcelain)" ]]; then
  git add -A
  git commit -m "$MSG"
else
  echo "No uncommitted changes."
fi

git push

CUR="$(latest_tag)"
if [[ -z "$CUR" ]]; then
  CUR="v0.1.0"
  echo "No existing semver tags found. Starting at $CUR"
fi

NEXT="$(bump "$MODE" "$CUR")"
echo "== Next version: $NEXT (mode: $MODE) =="

# Update sidebar/version file and commit it (only if changed)
write_version_file "$NEXT"
if ! git diff --quiet -- "$VERSION_FILE"; then
  git add "$VERSION_FILE"
  git commit -m "chore: bump version to ${NEXT}"
  git push
fi

echo "== Tagging release: $NEXT =="
git tag "$NEXT"
git push origin "$NEXT"

echo "== Updating compose to use ${IMAGE}:${NEXT} =="
perl -0777 -i -pe "s|${IMAGE}:v\\d+\\.\\d+\\.\\d+|${IMAGE}:${NEXT}|g" "$COMPOSE_FILE"
grep -q "${IMAGE}:${NEXT}" "$COMPOSE_FILE" || die "Compose file didn't get updated. Check IMAGE/COMPOSE_FILE."

git add "$COMPOSE_FILE"
git commit -m "deploy: bump image to ${NEXT}"
git push

cat <<OUT

Done ✅

Next steps:
1) Wait for GitHub Actions to finish building/pushing ${IMAGE}:${NEXT}
2) In Portainer: Stack -> "Re-pull image and redeploy"

OUT
