"""HTTP entry point and worker for processed-media subscriber delivery."""

from __future__ import annotations

import argparse
from typing import Any, Literal, Protocol

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from infrai_client import InfraiClient, InfraiError


class AssetEvent(BaseModel):
    asset_id: str = Field(min_length=1)
    creator_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    processing_status: Literal["ingested", "processing", "ready"]
    playback_url: HttpUrl | None = None
    subscriber_ids: list[str] = Field(min_length=1)


class FanoutResult(BaseModel):
    asset_id: str
    queued: int
    state: Literal["waiting_for_processing", "delivery_queued"]
    message_ids: list[str]


class QueuePort(Protocol):
    def publish(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        raise AssertionError("QueuePort methods are supplied by the queue client")

    def consume(self, max_messages: int, visibility_timeout: int) -> dict[str, Any]:
        raise AssertionError("QueuePort methods are supplied by the queue client")

    def ack(self, message_id: str) -> dict[str, Any]:
        raise AssertionError("QueuePort methods are supplied by the queue client")


def fan_out_ready_asset(event: AssetEvent, queue: QueuePort) -> FanoutResult:
    """Publish one delivery job per unique subscriber once processing is complete."""
    if event.processing_status != "ready" or event.playback_url is None:
        return FanoutResult(
            asset_id=event.asset_id,
            queued=0,
            state="waiting_for_processing",
            message_ids=[],
        )

    message_ids: list[str] = []
    for subscriber_id in dict.fromkeys(event.subscriber_ids):
        data = queue.publish(
            payload={
                "kind": "creator_media_ready",
                "asset_id": event.asset_id,
                "creator_id": event.creator_id,
                "subscriber_id": subscriber_id,
                "title": event.title,
                "playback_url": str(event.playback_url),
            },
            idempotency_key=f"media:{event.asset_id}:subscriber:{subscriber_id}",
        )
        message_ids.append(str(data["message_id"]))

    return FanoutResult(
        asset_id=event.asset_id,
        queued=len(message_ids),
        state="delivery_queued",
        message_ids=message_ids,
    )


def drain_delivery_batch(queue: QueuePort) -> int:
    """Render a small delivery batch and acknowledge each completed item."""
    batch = queue.consume(max_messages=10, visibility_timeout=60)
    delivered = 0
    for message in batch.get("items", []):
        payload = message["payload"]
        print(
            f"deliver subscriber={payload['subscriber_id']} "
            f"creator={payload['creator_id']} asset={payload['asset_id']} "
            f"url={payload['playback_url']}"
        )
        queue.ack(message_id=message["message_id"])
        delivered += 1
    return delivered


app = FastAPI(title="Creator media fanout")


@app.post("/media-events", response_model=FanoutResult)
def accept_media_event(event: AssetEvent) -> FanoutResult:
    try:
        client = InfraiClient()
        return fan_out_ready_asset(event, client.queue)
    except InfraiError as exc:
        caller_status = exc.status_code if 400 <= exc.status_code < 500 else 502
        raise HTTPException(
            status_code=caller_status,
            detail={"code": exc.code, "message": exc.detail.get("message")},
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Drain creator media delivery jobs")
    parser.add_argument("worker", nargs="?", default="worker")
    args = parser.parse_args()
    if args.worker == "worker":
        print(f"delivered={drain_delivery_batch(InfraiClient().queue)}")


if __name__ == "__main__":
    main()
