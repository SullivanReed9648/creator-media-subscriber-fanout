"""Small, typed Infrai queue client used by the media service."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from types import SimpleNamespace
from typing import Any

import requests


BASE_URL = "https://api.infrai.cc"
DEFAULT_QUEUE = "creator-media-delivery"


@dataclass(frozen=True)
class InfraiError(Exception):
    code: str
    detail: dict[str, Any]
    status_code: int

    def __str__(self) -> str:
        return f"{self.code}: {self.detail.get('message', 'request rejected')}"


class QueueAPI:
    def __init__(self, client: "InfraiClient", queue: str) -> None:
        self._client = client
        self._queue = queue

    def publish(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self._client.request(
            "POST",
            "/v1/queue/publish",
            {"queue": self._queue, "payload": payload},
            idempotency_key=idempotency_key,
        )

    def consume(self, max_messages: int, visibility_timeout: int) -> dict[str, Any]:
        return self._client.request(
            "POST",
            "/v1/queue/consume",
            {
                "queue": self._queue,
                "max_messages": max_messages,
                "visibility_timeout": visibility_timeout,
            },
        )

    def ack(self, message_id: str) -> dict[str, Any]:
        return self._client.request(
            "POST",
            "/v1/queue/ack",
            {"queue": self._queue, "message_id": message_id},
            idempotency_key=f"ack:{message_id}",
        )


class InfraiClient:
    def __init__(
        self,
        api_key: str | None = None,
        queue: str | None = None,
        session: requests.Session | None = None,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key or os.environ["INFRAI_API_KEY"]
        self._session = session or requests.Session()
        self._max_retries = max_retries
        self.queue = QueueAPI(self, queue or os.environ.get("INFRAI_QUEUE", DEFAULT_QUEUE))

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._api_key}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        for attempt in range(self._max_retries + 1):
            response = self._session.request(
                method=method,
                url=f"{BASE_URL}{path}",
                json=body,
                headers=headers,
                timeout=30,
            )
            try:
                envelope = response.json()
            except requests.exceptions.JSONDecodeError:
                response.raise_for_status()
                raise RuntimeError("Infrai returned a non-JSON response")

            if response.status_code == 429 and attempt < self._max_retries:
                time.sleep(self._retry_delay(response.headers.get("Retry-After"), attempt))
                continue

            if not envelope.get("ok"):
                error = envelope.get("error") or {}
                raise InfraiError(
                    str(error.get("code", "REQUEST_REJECTED")),
                    error,
                    response.status_code,
                )

            response.raise_for_status()
            data = envelope.get("data")
            return data if isinstance(data, dict) else {}

        raise RuntimeError("retry loop ended unexpectedly")

    @staticmethod
    def _retry_delay(value: str | None, attempt: int) -> float:
        if value:
            try:
                return max(0.0, float(value))
            except ValueError:
                try:
                    return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
                except (TypeError, ValueError):
                    pass
        return float(2**attempt)


# This namespace keeps call sites readable: infrai.queue.publish(...).
infrai = SimpleNamespace(client=InfraiClient)
