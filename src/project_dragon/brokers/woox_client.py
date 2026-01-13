from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)


class WooXAPIError(Exception):
    def __init__(self, message: str, payload: Optional[dict] = None, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.payload = payload or {}
        self.status_code = status_code


@dataclass
class WooXClient:
    api_key: str
    api_secret: str
    base_url: str = "https://api.woox.io"
    timeout: float = 10.0
    max_retries: int = 3
    backoff_seconds: float = 0.5
    session: Optional[requests.Session] = None

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()

    def _sign(self, method: str, request_path: str, body_str: str, timestamp_ms: str) -> str:
        payload = f"{timestamp_ms}{method}{request_path}{body_str}"
        return hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        method_upper = method.upper()
        path_clean = path if path.startswith("/") else f"/{path}"
        query_str = urlencode(params or {}, doseq=True)
        request_path_sig = f"{path_clean}?{query_str}" if query_str else path_clean
        body_str = json.dumps(json_body, separators=(",", ":")) if json_body else ""

        timestamp_ms = str(int(time.time() * 1000))
        signature = self._sign(method_upper, request_path_sig, body_str, timestamp_ms)

        headers = {
            "x-api-key": self.api_key,
            "x-api-timestamp": timestamp_ms,
            "x-api-signature": signature,
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}{path_clean}"

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(
                    method=method_upper,
                    url=url,
                    params=params,
                    data=body_str if body_str else None,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise WooXAPIError(f"Request failed: {exc}") from exc
                time.sleep(self.backoff_seconds * attempt)
                continue

            if resp.status_code in {429} or resp.status_code >= 500:
                if attempt >= self.max_retries:
                    raise WooXAPIError(f"HTTP {resp.status_code} after retries", status_code=resp.status_code)
                time.sleep(self.backoff_seconds * attempt)
                continue

            try:
                payload_json = resp.json()
            except ValueError as exc:  # pragma: no cover - defensive
                raise WooXAPIError("Invalid JSON response", status_code=resp.status_code) from exc

            if not payload_json.get("success", False):
                raise WooXAPIError(
                    payload_json.get("message", "WooX error"),
                    payload=payload_json,
                    status_code=resp.status_code,
                )
            return payload_json.get("data", payload_json)

        raise WooXAPIError("Unexpected retry exhaustion")

    def set_position_mode(self, mode: str) -> Dict[str, Any]:
        return self.request("PUT", "/v3/futures/positionMode", json_body={"positionMode": mode})

    def get_positions(self, symbol: str) -> Dict[str, Any]:
        return self.request("GET", "/v3/futures/positions", params={"symbol": symbol})

    def place_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", "/v3/trade/order", json_body=body)

    def cancel_order(self, order_id: Optional[str], symbol: str, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["clientOrderId"] = client_order_id
        return self.request("DELETE", "/v3/trade/order", params=params)

    def get_order(self, order_id: Optional[str], symbol: str, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["clientOrderId"] = client_order_id
        return self.request("GET", "/v3/trade/order", params=params)

    def get_orders(self, symbol: str, status: str = "INCOMPLETE") -> Dict[str, Any]:
        return self.request("GET", "/v3/trade/orders", params={"symbol": symbol, "status": status})

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        return self.request("GET", "/v1/public/market/tickers", params={"symbol": symbol})

    def get_orderbook(self, symbol: str, depth: int = 1) -> Dict[str, Any]:
        return self.request("GET", "/v3/public/orderbook", params={"symbol": symbol, "depth": depth})

    # --- Perps leverage (best-effort) --------------------------------
    def get_futures_leverage(
        self,
        symbol: str,
        *,
        margin_mode: Optional[str] = None,
        position_mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch futures leverage details.

        WooX returns account-level futures settings here, including positionMode
        (e.g. "HEDGE_MODE").
        """
        params: Dict[str, Any] = {"symbol": symbol}
        if margin_mode is not None:
            params["marginMode"] = str(margin_mode)
        if position_mode is not None:
            params["positionMode"] = str(position_mode)
        return self.request("GET", "/v3/futures/leverage", params=params)

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Best-effort wrapper for futures leverage.

        Endpoint naming varies across WooX versions; callers should catch WooXAPIError.
        """
        body = {"symbol": symbol, "leverage": int(leverage)}
        return self.request("PUT", "/v3/futures/leverage", json_body=body)

    # --- Perps income / funding (best-effort) -------------------------
    def get_income_history(
        self,
        symbol: str,
        *,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Best-effort wrapper for WooX income history.

        Endpoint names vary across WooX versions; this keeps the worker code thin.
        """
        params: Dict[str, Any] = {"symbol": symbol}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        if limit is not None:
            params["limit"] = int(limit)
        # Common guess for V3 perps income endpoint.
        return self.request("GET", "/v3/futures/income", params=params)

    def get_funding_history(
        self,
        symbol: str,
        *,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Best-effort wrapper for perps funding history.

        If WooX doesn't expose this endpoint for your account/region, caller should catch WooXAPIError.
        """
        params: Dict[str, Any] = {"symbol": symbol}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        if limit is not None:
            params["limit"] = int(limit)
        # Another common guess.
        return self.request("GET", "/v3/futures/funding", params=params)
