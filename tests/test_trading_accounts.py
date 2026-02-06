from __future__ import annotations

from project_dragon.storage import (
    create_credential,
    create_trading_account,
    get_trading_account,
    list_trading_accounts,
    open_db_connection,
)


def test_create_trading_account_roundtrip() -> None:
    user_id = "admin@local"
    exchange_id = "woox"
    label = "Primary"

    with open_db_connection() as conn:
        cred_id = create_credential(
            conn,
            user_id=user_id,
            exchange_id=exchange_id,
            label="Primary Keys",
            api_key="test_key",
            api_secret_plain="test_secret",
        )
        acct_id = create_trading_account(
            conn,
            user_id=user_id,
            exchange_id=exchange_id,
            label=label,
            credential_id=int(cred_id),
        )

        account = get_trading_account(conn, user_id, int(acct_id))
        assert account is not None
        assert int(account.get("id")) == int(acct_id)
        assert str(account.get("exchange_id")) == exchange_id
        assert str(account.get("label")) == label
        assert int(account.get("credential_id")) == int(cred_id)
        assert str(account.get("status")) == "active"

        accounts = list_trading_accounts(conn, user_id, exchange_id=exchange_id)
        ids = [int(a.get("id")) for a in accounts if a.get("id") is not None]
        assert int(acct_id) in ids
