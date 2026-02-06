from __future__ import annotations

from project_dragon.storage import create_credential, create_trading_account, list_trading_accounts, open_db_connection


def test_create_trading_account() -> None:
    user_id = "admin@local"
    with open_db_connection() as conn:
        cred_id = create_credential(
            conn,
            user_id=user_id,
            exchange_id="woox",
            label="test-cred",
            api_key="k",
            api_secret_plain="s",
        )
        account_id = create_trading_account(
            conn,
            user_id=user_id,
            exchange_id="woox",
            label="test-account",
            credential_id=int(cred_id),
        )

        rows = list_trading_accounts(conn, user_id=user_id, exchange_id="woox")
        ids = [int(r.get("id")) for r in rows if r.get("id") is not None]
        assert account_id in ids
