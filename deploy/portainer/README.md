# Project Dragon - Portainer deployment

## Portainer (Git Stack)
Portainer supports deploying a stack from a Git repository.
- Stacks -> Add stack -> Repository (Git)
- Compose path: deploy/portainer/docker-compose.yml
- For Git-based stacks, edit the compose file in Git (not Portainer UI).

## Required Stack environment variables
- DRAGON_MASTER_KEY   (required)
- DRAGON_IMAGE        (optional, default: ghcr.io/YOUR_GH_OWNER/YOUR_REPO:stable)
- DRAGON_USER_EMAIL   (optional)

## Persistent data (QNAP bind mounts)
This compose uses:
  /share/Container/dragon-test/data
  /share/Container/dragon-test/config
  /share/Container/dragon-test/logs
Delete /share/Container/dragon-test to wipe the test environment.
