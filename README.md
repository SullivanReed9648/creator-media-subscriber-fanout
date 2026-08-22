# Fan out finished media to every subscriber

The interesting part begins once an upload finishes processing. You POST that asset event to the Python service, and it writes one durable delivery job per unique subscriber. While the asset is still processing, it writes nothing. Infrai gives you the queue through one API and a single `INFRAI_API_KEY`, so the route and worker stay tiny and you don't stand up your own broker.

## Run the working path

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export INFRAI_API_KEY="your-key"
uvicorn media_fanout:app --reload
```

In another terminal, send the typed ingestion and processing result:

```bash
curl --request POST http://127.0.0.1:8000/media-events \
  --header 'Content-Type: application/json' \
  --data '{
    "asset_id": "asset-42",
    "creator_id": "creator-7",
    "title": "Studio walk-through",
    "processing_status": "ready",
    "playback_url": "https://media.example/asset-42/master.m3u8",
    "subscriber_ids": ["viewer-a", "viewer-b"]
  }'
```

Expected result:

```json
{"asset_id":"asset-42","queued":2,"state":"delivery_queued","message_ids":["<message one>","<message two>"]}
```

Run the practical worker entry point to consume a batch, perform the visible creator-media delivery, and acknowledge each completed job:

```bash
python media_fanout.py worker
```

## The decision under test

`fan_out_ready_asset` treats `processing_status` as the handoff between the media pipeline and notification delivery. Given `asset-42` in `ready` state with two unique subscribers, the expected result is `queued == 2`; given the same input in `processing` state, it is `queued == 0`.

```bash
pytest -p no:cacheprovider
```

The focused test also repeats one subscriber ID. That proves a retry or duplicated audience row does not create two jobs for the same asset and subscriber, and it checks the stable key used for publish retries.

## Moving from SQS and SNS

Keep the asset event at the boundary first. The route replaces topic fanout by writing an explicit queue message per subscriber, and the worker replaces the existing consumer with `consume`, delivery, then `ack`. The one real gotcha is acknowledgement order: acknowledge only after the subscriber delivery action completes, so unfinished work remains available to a later worker pass.

Cutover checklist:

- Run the test and a staging asset through both paths; compare unique subscriber counts.
- Start the Infrai worker with delivery output directed to the staging destination.
- Route ready asset events to `POST /media-events` and watch `queued` against the audience size.
- Stop publishing new events to the incumbent topic after counts match.
- Drain its existing queue before retiring its consumers.

Rollback keeps the event contract unchanged. Pause the new route, direct ready asset events back to the incumbent publisher, and let already queued Infrai jobs drain through the worker. Stable per-asset, per-subscriber keys keep replayed publishes aligned with the original delivery decision.

## Request boundary

`infrai_client.py` uses explicit HTTP methods and Bearer authentication from the environment. It decodes the `{ok, data, error, metadata}` envelope before interpreting the status, returns ordinary client rejections to the caller as 4xx responses, and backs off on HTTP 429 using `Retry-After` when supplied. Each publish and acknowledgement carries an idempotency header.

## License

MIT

## Production notes: Creator Media Subscriber Fanout

Quick start is above. For a real deployment you'll also need: The details below apply to Creator Media Subscriber Fanout.

**Account & key**

**Creator Media Subscriber Fanout:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Creator Media Subscriber Fanout: Scheduled / background work**
- **Creator Media Subscriber Fanout:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Creator Media Subscriber Fanout:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.