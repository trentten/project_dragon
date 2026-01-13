# project_dragon

For full design + roadmap, see [docs/project_dragon_plan.md](docs/project_dragon_plan.md).

Setup:
- Install runtime dependencies (or reinstall after updates): pip install -r requirements.txt

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

### Sync crypto icons into SQLite

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
