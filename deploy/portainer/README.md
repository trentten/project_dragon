# Project Dragon - Portainer deployment

## Portainer (Git Stack)
Portainer supports deploying a stack from a Git repository.
- Stacks -> Add stack -> Repository (Git)
- Compose path: deploy/portainer/docker-compose.yml
- For Git-based stacks, edit the compose file in Git (not Portainer UI).

## Required Stack environment variables
- DRAGON_MASTER_KEY   (required)
- DRAGON_POSTGRES_PASSWORD (required)
- DRAGON_IMAGE        (optional, default: ghcr.io/YOUR_GH_OWNER/YOUR_REPO:stable)
- DRAGON_USER_EMAIL   (optional)

## Postgres connection
The compose file sets:
- DRAGON_DATABASE_URL=postgresql://dragon:${DRAGON_POSTGRES_PASSWORD}@postgres:5432/dragon

## Portainer vs Codespaces/dev
- Portainer: deploy docker-compose.yml only (no Postgres host port exposed).
- Codespaces/dev (host access for pytest):
  - cd deploy/portainer
  - docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
  - export DRAGON_DATABASE_URL="postgresql://dragon:${DRAGON_POSTGRES_PASSWORD}@127.0.0.1:5432/dragon"

Containers should always use @postgres:5432. Host shell uses @127.0.0.1:5432 only when the dev override is enabled.

## Migrations
- Stack startup runs migrations automatically before services start.
- Manual run (if needed):
  - docker compose exec dragon_ui bash -lc 'python -m project_dragon.db_migrate'

## Persistent data (QNAP bind mounts)
This compose uses:
  /share/Container/dragon-test/config
  /share/Container/dragon-test/logs
Delete /share/Container/dragon-test to wipe the test environment.

Postgres data is stored in the named volume:
- dragon_postgres_data
