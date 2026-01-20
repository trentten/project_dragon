from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from project_dragon.storage import list_assets_server_side, open_db_connection


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _seed_assets(conn, *, exchange_id: str, n: int) -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(n):
        # Make updated_at monotonic so default ORDER BY updated_at DESC is deterministic.
        updated_at = _iso(base + timedelta(seconds=i))
        created_at = updated_at
        symbol = f"SYM{i:03d}/USDT"
        base_asset = f"SYM{i:03d}"
        conn.execute(
            """
            INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'active', %s, %s)
            ON CONFLICT(exchange_id, symbol) DO UPDATE SET
                base_asset = excluded.base_asset,
                quote_asset = excluded.quote_asset,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (exchange_id, symbol, base_asset, "USDT", created_at, updated_at),
        )


def _create_category(conn, *, user_id: str, exchange_id: str, name: str) -> int:
    now = _iso(datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc))
    row = conn.execute(
        """
        INSERT INTO asset_categories(user_id, exchange_id, name, source, created_at, updated_at)
        VALUES (%s, %s, %s, 'user', %s, %s)
        ON CONFLICT(user_id, exchange_id, name) DO UPDATE SET updated_at = excluded.updated_at
        RETURNING id
        """,
        (user_id, exchange_id, name, now, now),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _add_membership(conn, *, user_id: str, exchange_id: str, category_id: int, symbol: str) -> None:
    now = _iso(datetime(2026, 1, 2, 0, 0, 1, tzinfo=timezone.utc))
    conn.execute(
        """
        INSERT INTO asset_category_membership(user_id, exchange_id, category_id, symbol, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(user_id, exchange_id, category_id, symbol) DO NOTHING
        """,
        (user_id, exchange_id, int(category_id), symbol, now),
    )
def test_assets_server_side_paging_and_count() -> None:
    user_id = "admin@local"
    exchange_id = "woo"

    with open_db_connection() as conn:
        _seed_assets(conn, exchange_id=exchange_id, n=50)
        conn.commit()

        rows, total = list_assets_server_side(
            conn=conn,
            user_id=user_id,
            exchange_id=exchange_id,
            page=1,
            page_size=10,
            filter_model=None,
            sort_model=None,
            status="all",
        )

    assert total == 50
    assert len(rows) == 10

    # Default ordering is updated_at DESC, so page 1 should include the highest indexes.
    symbols = [r.get("symbol") for r in rows]
    assert symbols[0] == "SYM049/USDT"
    assert symbols[-1] == "SYM040/USDT"


def test_assets_server_side_sort_model_symbol_asc() -> None:
    user_id = "admin@local"
    exchange_id = "woo"

    with open_db_connection() as conn:
        _seed_assets(conn, exchange_id=exchange_id, n=20)
        conn.commit()

        rows, total = list_assets_server_side(
            conn=conn,
            user_id=user_id,
            exchange_id=exchange_id,
            page=1,
            page_size=5,
            sort_model=[{"colId": "symbol", "sort": "asc"}],
            filter_model=None,
            status="all",
        )

    assert total == 20
    assert [r.get("symbol") for r in rows] == [
        "SYM000/USDT",
        "SYM001/USDT",
        "SYM002/USDT",
        "SYM003/USDT",
        "SYM004/USDT",
    ]


def test_assets_server_side_filter_contains_and_category_name() -> None:
    user_id = "admin@local"
    exchange_id = "woo"

    with open_db_connection() as conn:
        _seed_assets(conn, exchange_id=exchange_id, n=30)

        # Overwrite a few names for a contains filter on the synthetic "name" column.
        conn.execute(
            "UPDATE assets SET base_asset = %s WHERE exchange_id = %s AND symbol = %s",
            ("ALPHA", exchange_id, "SYM005/USDT"),
        )
        conn.execute(
            "UPDATE assets SET base_asset = %s WHERE exchange_id = %s AND symbol = %s",
            ("ALPHABETA", exchange_id, "SYM006/USDT"),
        )
        conn.execute(
            "UPDATE assets SET base_asset = %s WHERE exchange_id = %s AND symbol = %s",
            ("GAMMA", exchange_id, "SYM007/USDT"),
        )

        cat_id = _create_category(conn, user_id=user_id, exchange_id=exchange_id, name="Majors")
        _add_membership(conn, user_id=user_id, exchange_id=exchange_id, category_id=cat_id, symbol="SYM005/USDT")
        _add_membership(conn, user_id=user_id, exchange_id=exchange_id, category_id=cat_id, symbol="SYM007/USDT")
        conn.commit()

        # Filter model: name contains "ALPHA".
        rows, total = list_assets_server_side(
            conn=conn,
            user_id=user_id,
            exchange_id=exchange_id,
            page=1,
            page_size=50,
            filter_model={
                "name": {
                    "filterType": "text",
                    "type": "contains",
                    "filter": "ALPHA",
                }
            },
            sort_model=[{"colId": "symbol", "sort": "asc"}],
            status="all",
        )
        assert total == 2
        assert [r.get("symbol") for r in rows] == ["SYM005/USDT", "SYM006/USDT"]

        # Category name filter (UI selectbox): should return only category members.
        rows2, total2 = list_assets_server_side(
            conn=conn,
            user_id=user_id,
            exchange_id=exchange_id,
            page=1,
            page_size=50,
            filter_model=None,
            sort_model=[{"colId": "symbol", "sort": "asc"}],
            category_name="Majors",
            status="all",
        )
        assert total2 == 2
        assert [r.get("symbol") for r in rows2] == ["SYM005/USDT", "SYM007/USDT"]
