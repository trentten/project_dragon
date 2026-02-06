from __future__ import annotations

from collections import defaultdict

from project_dragon.exchange_normalization import canonical_exchange_id, canonical_symbol
from project_dragon.storage import open_db_connection


def main() -> int:
    with open_db_connection(read_only=True) as conn:
        ex_rows = conn.execute(
            "SELECT exchange_id, COUNT(*) AS c FROM assets GROUP BY exchange_id ORDER BY c DESC, exchange_id ASC"
        ).fetchall()
        print("assets_by_exchange_id")
        for r in ex_rows or []:
            print(f"- {r[0]}: {r[1]}")

        rows = conn.execute(
            "SELECT exchange_id, symbol FROM assets WHERE exchange_id IN ('woo','woox','woo-x','woo_x') ORDER BY exchange_id, symbol"
        ).fetchall()

    grouped = defaultdict(list)
    for ex_id, sym in rows or []:
        ex_c = canonical_exchange_id(str(ex_id or ""))
        sym_c = canonical_symbol(ex_c, str(sym or ""))
        grouped[(ex_c, sym_c)].append((str(ex_id or ""), str(sym or "")))

    dups = [(k, v) for k, v in grouped.items() if len(v) > 1]
    dups.sort(key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1]))

    print("\ncanonical_symbol_duplicates_in_assets")
    for (ex_c, sym_c), items in dups[:100]:
        print(f"- {ex_c} {sym_c} ({len(items)} rows): {items[:5]}")

    if len(dups) > 100:
        print(f"... {len(dups) - 100} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
