# project_dragon

For full design + roadmap, see [docs/project_dragon_plan.md](docs/project_dragon_plan.md).

Setup:
- Install runtime dependencies (or reinstall after updates): pip install -r requirements.txt

Postgres (required):
- Set DRAGON_DATABASE_URL (example): postgresql://dragon:dragon@localhost:5432/dragon

Migrations (two modes):
A) Recommended (default): migrate-once using db_migrate service
- docker compose -f deploy/portainer/docker-compose.yml up -d
- UI/workers never run DDL at runtime.

B) Dev convenience (optional): auto-migrate in app runtime
- Set DRAGON_ENV=dev and DRAGON_AUTO_MIGRATE=1
- Intended for local dev only; ignored in prod.

UI performance:
- Runs grids use server-side paging; grid filter/sort re-queries Postgres and only returns the current page.

Candle cache (P1 performance):
- Worker-local in-memory LRU cache reduces repeated candle loads.
- Range requests are keyed by chunked time windows to avoid unique per-run keys.

Env knobs:
- `DRAGON_CANDLE_CACHE_MAX_ITEMS` (default `64`)
- `DRAGON_CANDLE_CACHE_MB` (default `256`)
- `DRAGON_CANDLE_CACHE_CHUNK_SEC` (default `21600` = 6h)
- `DRAGON_CANDLE_CACHE_PREFETCH` (default `1`)
- `DRAGON_CANDLE_CACHE_PREFETCH_QUEUE_SIZE` (default `64`)
- `DRAGON_CANDLE_CACHE_PREFETCH_TOP_N` (default `2`)

Tests:
- Codespaces/dev bringup:
	docker compose -f deploy/portainer/docker-compose.yml -f deploy/portainer/docker-compose.dev.yml up -d
- Run tests on host (with dev override exposing Postgres on 127.0.0.1:5432):
	export DRAGON_DATABASE_URL="postgresql://dragon:${DRAGON_POSTGRES_PASSWORD}@127.0.0.1:5432/dragon"
	PYTHONPATH=src pytest -q

Manual migrations (if needed):
- docker compose -f deploy/portainer/docker-compose.yml run --rm db_migrate

Live dry-run checklist (WooX):
- Set WOOX_API_KEY/SECRET in your env (or accept stub mode for offline checks).
- Open the Streamlit Live Bots tab → Live Test Tools.
- Run the connectivity check against your target symbol/timeframe.
- Run the one-bar dry-run (no orders) to verify strategy + event plumbing before going live.

WooX maker path notes:
- BBO queueing uses BID/ASK types with bidAskLevel (1-5) and no price field; orders stay maker by design.
- Fallback maker path uses POST_ONLY with postOnlyAdjusted to avoid crossing, using top-of-book bid/ask as the limit price.

## Icons (local-first)

Project Dragon uses vendored icon packs (no runtime icon scraping / external icon URLs):

- Crypto asset icons: `app/assets/crypto_icons/spothq/cryptocurrency-icons` (spothq/cryptocurrency-icons, CC0)
- UI icons: `app/assets/ui_icons/tabler/filled` (Tabler Icons Filled, MIT)

### Sync crypto icons into the DB

The app caches resolved icons into `symbols.icon_uri` (data URIs) for fast rendering.

- In Streamlit → Runs Explorer, use **Sync missing icons** (or rely on the page’s best-effort auto-sync).

### Add a new Tabler icon

Curated subset location:

- `app/assets/ui_icons/tabler/filled/<name>.svg`

One-liner (copies from the upstream submodule into the curated subset):

```bash
PYTHONPATH=src python -m project_dragon.ui.icons add <name> --variant filled
```

Then reference it from Python via `project_dragon.ui.icons`.
