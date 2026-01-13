# UI Icons (Tabler)

Project Dragon standardizes on **Tabler Icons** stored locally under:

- `app/assets/ui_icons/tabler/filled/` (curated subset used by the app)
- `app/assets/ui_icons/tabler/outline/` (optional)
- `app/assets/ui_icons/tabler/_upstream/` (vendored upstream icon set)

The runtime code never fetches icons from the web.

## Use an icon in Streamlit

Preferred API:

- `project_dragon.ui.icons.st_tabler_icon("player-play", size_px=18)`

For embedding inside HTML/AgGrid cell renderers:

- `project_dragon.ui.icons.get_tabler_svg("player-play")` (returns raw SVG)
- `project_dragon.ui.icons.render_tabler_icon("player-play", size_px=16)` (returns inline SVG HTML)

## Common icon mapping

The app uses a small mapping for common actions in:

- `src/project_dragon/ui/icons.py` → `ICONS`

Example:

```python
from project_dragon.ui.icons import ICONS, st_tabler_icon

st_tabler_icon(ICONS["run"], size_px=18)
```

## Add a new Tabler icon (one-liner)

If the icon exists in the upstream set, copy it into the curated subset:

```bash
PYTHONPATH=src python -m project_dragon.ui.icons add <icon-name> --variant filled
```

This writes:

- `app/assets/ui_icons/tabler/filled/<icon-name>.svg`

Then reference it via:

- `get_tabler_svg("<icon-name>")` or `st_tabler_icon("<icon-name>")`
