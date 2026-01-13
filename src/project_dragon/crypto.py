from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


class CryptoConfigError(RuntimeError):
    pass


def _get_fernet() -> Fernet:
    key = (os.environ.get("DRAGON_MASTER_KEY") or "").strip()
    if not key:
        raise CryptoConfigError(
            "DRAGON_MASTER_KEY is not set. Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:
        raise CryptoConfigError(
            "DRAGON_MASTER_KEY is invalid. It must be a urlsafe base64-encoded 32-byte key from Fernet.generate_key()."
        ) from exc


def encrypt_str(plain: str) -> str:
    f = _get_fernet()
    token = f.encrypt((plain or "").encode("utf-8"))
    return token.decode("utf-8")


def decrypt_str(token: str) -> str:
    f = _get_fernet()
    try:
        plain = f.decrypt((token or "").encode("utf-8"))
    except InvalidToken as exc:
        raise CryptoConfigError("Failed to decrypt secret (invalid token or wrong DRAGON_MASTER_KEY).") from exc
    return plain.decode("utf-8")


def mask_api_key(api_key: Optional[str]) -> str:
    s = (api_key or "").strip()
    if not s:
        return ""
    if len(s) <= 6:
        return "***"
    return f"{s[:3]}…{s[-3:]}"
