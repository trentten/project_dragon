from __future__ import annotations

import base64
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional


TablerVariant = Literal["filled", "outline"]


ICONS: dict[str, str] = {
    # Common actions
    "run": "player-play",
    "create": "circle-plus",
    "open": "folder",
    "export": "file-download",
    "settings": "settings",
    "filter": "filter",
    "compare": "exchange",
    "refresh": "replace",
    "shortlist": "star",
    # Status
    "ok": "circle-check",
    "warn": "alert-triangle",
    "error": "circle-x",
    # Navigation-ish
    "runs": "table",
    "results": "clipboard-list",
    "live": "dashboard",
    "backtest": "chart-candle",
    "sweeps": "chart-area-line",
    "accounts": "user",
    "jobs": "layout-list",
    "candle_cache": "candle",
}


def _repo_root() -> Path:
    env_root = os.environ.get("PROJECT_DRAGON_ROOT") or os.environ.get("DRAGON_ASSETS_ROOT")
    if env_root:
        try:
            p = Path(env_root).expanduser().resolve()
            if p.exists():
                return p
        except Exception:
            pass

    candidates = [Path.cwd(), Path(__file__).resolve()]
    for base in candidates:
        for parent in [base] + list(base.parents):
            if (parent / "app" / "assets" / "ui_icons" / "tabler").exists():
                return parent

    for fallback in (Path("/app"), Path("/workspaces/project_dragon")):
        try:
            if (fallback / "app" / "assets" / "ui_icons" / "tabler").exists():
                return fallback
        except Exception:
            pass

    return Path(__file__).resolve().parents[3]


def _tabler_root() -> Path:
    return _repo_root() / "app" / "assets" / "ui_icons" / "tabler"


def tabler_assets_available() -> bool:
    try:
        root = _tabler_root()
        return root.exists()
    except Exception:
        return False


def tabler_icon_inventory() -> dict[str, int]:
    counts: dict[str, int] = {"filled": 0, "outline": 0}
    try:
        root = _tabler_root()
        for variant in ("filled", "outline"):
            p = root / variant
            if p.exists():
                counts[variant] = len(list(p.glob("*.svg")))
    except Exception:
        pass
    return counts


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@lru_cache(maxsize=512)
def get_tabler_svg(name: str, variant: TablerVariant = "filled") -> str:
    """Return the raw SVG for a Tabler icon (local-first).

    Lookup order:
    1) Curated subset: app/assets/ui_icons/tabler/<variant>/<name>.svg
    2) Upstream submodule fallback: app/assets/ui_icons/tabler/_upstream/icons/<variant>/<name>.svg

    `name` should be passed without the .svg extension.
    """

    icon_name = (name or "").strip().removesuffix(".svg")
    if not icon_name:
        raise ValueError("Tabler icon name is required")

    variant_norm: TablerVariant = "outline" if str(variant) == "outline" else "filled"
    root = _tabler_root()

    curated = root / variant_norm / f"{icon_name}.svg"
    if curated.exists():
        return _read_text(curated)

    upstream = root / "_upstream" / "icons" / variant_norm / f"{icon_name}.svg"
    if upstream.exists():
        return _read_text(upstream)

    # Fallback: if filled requested but missing, try outline.
    if variant_norm == "filled":
        outline = root / "outline" / f"{icon_name}.svg"
        if outline.exists():
            return _read_text(outline)
        upstream_outline = root / "_upstream" / "icons" / "outline" / f"{icon_name}.svg"
        if upstream_outline.exists():
            return _read_text(upstream_outline)

    raise FileNotFoundError(f"Tabler icon not found: {variant_norm}/{icon_name}.svg")


def tabler_svg_to_data_uri(svg: str) -> str:
    raw = (svg or "").strip().encode("utf-8")
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/svg+xml;base64,{b64}"


_svg_open_tag_re = re.compile(r"<svg\b[^>]*>", flags=re.IGNORECASE | re.DOTALL)


def _set_svg_attr(open_tag: str, attr: str, value: str) -> str:
    # Replace existing attr if present, else inject before '>'
    a = attr.strip()
    if re.search(rf"\b{re.escape(a)}=", open_tag):
        return re.sub(rf"\b{re.escape(a)}=\"[^\"]*\"", f'{a}="{value}"', open_tag, count=1)
    return open_tag[:-1] + f' {a}="{value}">'  # safe: open_tag ends with '>'


def render_tabler_icon(
    name: str,
    *,
    size_px: int = 16,
    color: str = "currentColor",
    variant: TablerVariant = "filled",
    as_img: bool = False,
) -> str:
    """Return an HTML snippet for a Tabler icon.

    By default this returns inline SVG wrapped in a span. In some Streamlit builds,
    inline <svg> may be escaped/sanitized and show up as raw text. Setting
    `as_img=True` will return an <img> with a data-URI source instead.
    """

    svg = get_tabler_svg(name, variant=variant)
    m = _svg_open_tag_re.search(svg)
    if not m:
        return svg

    open_tag = m.group(0)
    open_tag2 = open_tag
    open_tag2 = _set_svg_attr(open_tag2, "width", str(int(size_px)))
    open_tag2 = _set_svg_attr(open_tag2, "height", str(int(size_px)))

    # Ensure icons respect desired color.
    if str(variant) == "outline":
        open_tag2 = _set_svg_attr(open_tag2, "fill", "none")
        open_tag2 = _set_svg_attr(open_tag2, "stroke", str(color))
    else:
        # Filled icons: set fill to the chosen color.
        open_tag2 = _set_svg_attr(open_tag2, "fill", str(color))
        open_tag2 = _set_svg_attr(open_tag2, "stroke", "none")

    svg2 = svg[: m.start()] + open_tag2 + svg[m.end() :]

    if as_img:
        uri = tabler_svg_to_data_uri(svg2)
        return (
            f'<img src="{uri}" width="{int(size_px)}" height="{int(size_px)}" '
            f'style="display:block;" alt="" />'
        )

    # Inline SVG path: wrap with a span to normalize layout.
    return (
        f'<span style="display:inline-flex; align-items:center; justify-content:center; '
        f'line-height:0;">{svg2}</span>'
    )


def st_tabler_icon(
    name: str,
    *,
    size_px: int = 16,
    color: str = "currentColor",
    variant: TablerVariant = "filled",
    as_img: bool = False,
) -> None:
    import streamlit as st  # type: ignore

    html = render_tabler_icon(name, size_px=size_px, color=color, variant=variant, as_img=as_img)
    st.markdown(html, unsafe_allow_html=True)


def add_tabler_icon(
    name: str,
    *,
    variant: TablerVariant = "filled",
    force: bool = False,
) -> Path:
    """Copy an icon from the upstream submodule into the curated subset.

    Example:
        PYTHONPATH=src python -m project_dragon.ui.icons add player-play

    Requires the Tabler upstream submodule at app/assets/ui_icons/tabler/_upstream.
    """

    icon_name = (name or "").strip().removesuffix(".svg")
    if not icon_name:
        raise ValueError("Icon name required")

    variant_norm: TablerVariant = "outline" if str(variant) == "outline" else "filled"
    root = _tabler_root()
    src = root / "_upstream" / "icons" / variant_norm / f"{icon_name}.svg"
    if not src.exists():
        raise FileNotFoundError(f"Upstream icon not found: {src}")

    dst_dir = root / variant_norm
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / f"{icon_name}.svg"

    if dst.exists() and not force:
        return dst

    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    # Bust cache for this icon name.
    try:
        get_tabler_svg.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    return dst


def _main(argv: Optional[list[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Project Dragon: Tabler icon helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="Copy an upstream icon into curated subset")
    add.add_argument("name")
    add.add_argument("--variant", choices=["filled", "outline"], default="filled")
    add.add_argument("--force", action="store_true")

    ns = p.parse_args(argv)

    if ns.cmd == "add":
        out = add_tabler_icon(ns.name, variant=ns.variant, force=bool(ns.force))
        print(str(out))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
