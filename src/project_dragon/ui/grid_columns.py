from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

try:
    from st_aggrid import JsCode  # type: ignore
except Exception:  # pragma: no cover
    JsCode = None  # type: ignore

from project_dragon.ui.components import aggrid_pill_style


_DIRECTION_STYLE_JS = """
function(params) {
    const v = (params.value || '').toString().toLowerCase();
    if (v.includes('long')) {
        return { color: '#FFFFFF', backgroundColor: 'rgba(46,160,67,0.12)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
    }
    if (v.includes('short')) {
        return { color: '#FFFFFF', backgroundColor: 'rgba(248,81,73,0.12)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
    }
    return { color: '#FFFFFF', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
}
"""


_DIRECTION_RENDERER_JS = """
class DirectionRenderer {
    init(params) {
        const raw = (params && params.value) ? params.value.toString() : '';
        const v = raw.toLowerCase();
        const wrap = document.createElement('div');
        wrap.style.display = 'flex';
        wrap.style.alignItems = 'center';
        wrap.style.justifyContent = 'center';
        wrap.style.gap = '6px';
        wrap.style.width = '100%';

        const arrow = document.createElement('span');
        arrow.style.fontWeight = '900';
        arrow.style.fontSize = '12px';
        if (v.includes('long')) {
            arrow.innerText = '▲';
            arrow.style.color = '#7ee787';
        } else if (v.includes('short')) {
            arrow.innerText = '▼';
            arrow.style.color = '#ff7b72';
        } else {
            arrow.innerText = '';
            arrow.style.color = '#cbd5e1';
        }

        const text = document.createElement('span');
        text.innerText = raw;
        text.style.fontWeight = '500';
        text.style.color = '#FFFFFF';

        wrap.appendChild(arrow);
        wrap.appendChild(text);
        this.eGui = wrap;
    }
    getGui() { return this.eGui; }
    refresh(params) { return false; }
}
"""


_DDMMYY_FORMATTER_JS = """
function(params) {
    try {
        const v = params && params.value !== undefined && params.value !== null ? params.value : '';
        const s = v.toString().trim();
        if (!s) return '';

        // Fast-path for YYYY-MM-DD.
        const m = s.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})/);
        if (m) {
            return `${m[3]}/${m[2]}/${m[1].slice(2)}`;
        }

        let d;
        if (/^[0-9]+$/.test(s)) {
            const n = parseInt(s, 10);
            d = new Date(n > 1000000000000 ? n : (n * 1000));
        } else {
            d = new Date(s);
        }
        if (isNaN(d.getTime())) return s;

        const dd = String(d.getUTCDate()).padStart(2, '0');
        const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
        const yy = String(d.getUTCFullYear()).slice(2);
        return `${dd}/${mm}/${yy}`;
    } catch (e) {
        const v = params && params.value !== undefined && params.value !== null ? params.value : '';
        return v;
    }
}
"""


def _js_code(js: str) -> Any:
    if JsCode is None:
        return None
    return JsCode(js)


def col_timeframe_pill(field: str = "timeframe", header: str = "Timeframe", **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "cellStyle": aggrid_pill_style("timeframe"),
        "cellClass": "ag-center-aligned-cell",
    }
    base.update(overrides)
    base["field"] = field
    return base


def col_direction_pill(field: str = "direction", header: str = "Direction", **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "cellStyle": _js_code(_DIRECTION_STYLE_JS),
        "cellRenderer": _js_code(_DIRECTION_RENDERER_JS),
        "cellClass": "ag-center-aligned-cell",
    }
    base.update(overrides)
    base["field"] = field
    return base


def col_avg_position_time(field: str = "avg_position_time", header: str = "Avg Position Time", **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "cellClass": "ag-center-aligned-cell",
    }
    base.update(overrides)
    base["field"] = field
    return base


def col_shortlist(field: str = "shortlist", header: str = "Shortlist", editable: bool = True, **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "editable": bool(editable),
        "cellRenderer": "agCheckboxCellRenderer",
        "cellEditor": "agCheckboxCellEditor",
        "filter": False,
        "cellClass": "ag-center-aligned-cell",
    }
    base.update(overrides)
    base["field"] = field
    return base


def col_shortlist_note(field: str = "shortlist_note", header: str = "Shortlist Note", editable: bool = True, **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "editable": bool(editable),
        "cellEditor": "agTextCellEditor",
    }
    base.update(overrides)
    base["field"] = field
    return base


def col_date_ddmmyy(field: str, header: str, **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "headerName": header,
        "valueFormatter": _js_code(_DDMMYY_FORMATTER_JS),
        "cellClass": "ag-center-aligned-cell",
    }
    base.update(overrides)
    base["field"] = field
    return base


def apply_columns(gb: Any, col_defs: Iterable[Dict[str, Any]], saved_widths: Optional[Dict[str, int]] = None) -> None:
    width_map: Dict[str, int] = {}
    if isinstance(saved_widths, dict):
        for k, v in saved_widths.items():
            try:
                width_map[str(k)] = int(v)
            except Exception:
                continue

    for col_def in col_defs:
        if not isinstance(col_def, dict):
            continue
        cfg = dict(col_def)
        field = cfg.pop("field", None) or cfg.pop("colId", None)
        if not field:
            continue
        field_str = str(field)
        if field_str in width_map:
            cfg["width"] = width_map[field_str]
        gb.configure_column(field_str, **cfg)
