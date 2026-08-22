from typing import Any

from media_fanout import AssetEvent, fan_out_ready_asset


class RecordingQueue:
    def __init__(self) -> None:
        self.published: list[tuple[dict[str, Any], str]] = []

    def publish(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        self.published.append((payload, idempotency_key))
        return {"message_id": f"msg-{len(self.published)}"}

    def consume(self, max_messages: int, visibility_timeout: int) -> dict[str, Any]:
        return {"items": []}

    def ack(self, message_id: str) -> dict[str, Any]:
        return {}


def test_ready_asset_fans_out_once_per_unique_subscriber() -> None:
    queue = RecordingQueue()
    event = AssetEvent(
        asset_id="asset-42",
        creator_id="creator-7",
        title="Studio walk-through",
        processing_status="ready",
        playback_url="https://media.example/asset-42/master.m3u8",
        subscriber_ids=["viewer-a", "viewer-b", "viewer-a"],
    )

    result = fan_out_ready_asset(event, queue)

    assert result.model_dump() == {
        "asset_id": "asset-42",
        "queued": 2,
        "state": "delivery_queued",
        "message_ids": ["msg-1", "msg-2"],
    }
    assert [item[0]["subscriber_id"] for item in queue.published] == [
        "viewer-a",
        "viewer-b",
    ]
    assert queue.published[0][1] == "media:asset-42:subscriber:viewer-a"


def test_processing_asset_does_not_publish_delivery_jobs() -> None:
    queue = RecordingQueue()
    event = AssetEvent(
        asset_id="asset-43",
        creator_id="creator-7",
        title="Rough cut",
        processing_status="processing",
        subscriber_ids=["viewer-a"],
    )

    result = fan_out_ready_asset(event, queue)

    assert result.queued == 0
    assert result.state == "waiting_for_processing"
    assert queue.published == []
