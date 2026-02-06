from __future__ import annotations

from typing import Any

try:
    from st_aggrid import JsCode  # type: ignore
except Exception:  # pragma: no cover
    JsCode = None  # type: ignore


def _js_code(js: str) -> Any:
    if JsCode is None:
        return None
    return JsCode(js)


def asset_market_getter_js(
    *,
    symbol_field: str = "symbol",
    market_field: str = "market",
    base_asset_field: str = "base_asset",
    asset_field: str = "asset",
) -> Any:
    return _js_code(
        f"""
        function(params) {{
            try {{
                const data = (params && params.data) ? params.data : {{}};
                const rawSym = (data["{symbol_field}"] !== undefined && data["{symbol_field}"] !== null)
                    ? data["{symbol_field}"].toString()
                    : '';
                const rawMarket = (data["{market_field}"] !== undefined && data["{market_field}"] !== null)
                    ? data["{market_field}"].toString()
                    : '';

                const base = (data["{base_asset_field}"] !== undefined && data["{base_asset_field}"] !== null && data["{base_asset_field}"].toString().trim())
                    ? data["{base_asset_field}"].toString().trim().toUpperCase()
                    : ((data["{asset_field}"] !== undefined && data["{asset_field}"] !== null && data["{asset_field}"].toString().trim())
                        ? data["{asset_field}"].toString().trim().toUpperCase()
                        : (rawSym || '').toString().trim());
                const baseClean = base ? base.replace(/:USDT$/i, '') : base;
                const mkt = (rawMarket || '').toString().trim().toUpperCase();
                return (baseClean && mkt) ? `${{baseClean}} - ${{mkt}}` : baseClean;
            }} catch (e) {{
                const v = (params && params.value !== undefined && params.value !== null) ? params.value.toString() : '';
                return v;
            }}
        }}
        """
    )


def asset_renderer_js(*, icon_field: str = "icon_uri", size_px: int = 18, text_color: str = "#FFFFFF") -> Any:
    return _js_code(
        f"""
        class AssetRenderer {{
            init(params) {{
                const label = (params && params.value !== undefined && params.value !== null)
                    ? params.value.toString()
                    : '';
                const uri = (params && params.data && params.data["{icon_field}"]) ? params.data["{icon_field}"].toString() : '';

                const wrap = document.createElement('div');
                wrap.style.display = 'flex';
                wrap.style.alignItems = 'center';
                wrap.style.gap = '8px';
                wrap.style.width = '100%';

                if (uri) {{
                    const img = document.createElement('img');
                    img.src = uri;
                    img.style.width = '{size_px}px';
                    img.style.height = '{size_px}px';
                    img.style.borderRadius = '3px';
                    img.style.display = 'block';
                    wrap.appendChild(img);
                }} else {{
                    const spacer = document.createElement('div');
                    spacer.style.width = '{size_px}px';
                    spacer.style.height = '{size_px}px';
                    wrap.appendChild(spacer);
                }}

                const text = document.createElement('span');
                text.innerText = label;
                text.style.fontWeight = '500';
                text.style.color = '{text_color}';
                wrap.appendChild(text);

                this.eGui = wrap;
            }}
            getGui() {{ return this.eGui; }}
            refresh(params) {{ return false; }}
        }}
        """
    )
