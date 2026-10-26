"""Round 5's control transport on Azure, the App's side: SQS FIFO's SendMessage, on Service Bus.

The AWS App sends each control event to its lane's SQS FIFO queue (`app.py`, `publish_control`),
with the lane and job as its message group and the event's ID as its deduplication ID. The Azure
App sends the same body to its lane's Service Bus queue, as its own identity (the App Access
Connector's, through its Unity Catalog service credential), with the same two keys:

- **the session ID `<lane>-<job>`**, so a job's events arrive in order, as a message group's do;
- **the message ID, the event's ID**, so the queue's duplicate detection drops a resend.

It sends over HTTPS, which a session queue accepts, so the App takes no new dependency. Receiving
from a session needs AMQP, so the runners read with the Service Bus SDK (`runner/azure_io.py`).
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

Opener = Callable[..., Any]


class ServiceBusPublishError(RuntimeError):
    """Service Bus didn't take a control event; the dispatcher retries it, as an SQS failure."""


def message_properties(event: Any) -> dict[str, str]:
    """The two keys SQS FIFO reads off a message, in Service Bus's names."""

    return {"SessionId": f"{event.lane_id}-{event.job_id}", "MessageId": str(event.event_id)}


def service_bus_publisher(
    namespace: str,
    queues: Mapping[str, str],
    token: Callable[[], str],
    *,
    opener: Opener = urllib.request.urlopen,
) -> Callable[[Any], Awaitable[None]]:
    """`publish_control` for `Round5ControlDispatcher`: one HTTPS send per control event."""

    if not namespace.strip() or set(queues) != {"lakebase", "competitor"}:
        raise ValueError("Round 5's transport needs its namespace and both lanes' queues")

    async def publish_control(event: Any) -> None:
        def send() -> None:
            queue = urllib.parse.quote(queues[event.lane_id], safe="")
            request = urllib.request.Request(
                f"https://{namespace}.servicebus.windows.net/{queue}/messages",
                data=event.encoded_body().encode("utf-8"),
                method="POST",
                headers={
                    "Authorization": f"Bearer {token()}",
                    "BrokerProperties": json.dumps(message_properties(event)),
                    "Content-Type": "application/octet-stream",
                },
            )
            try:
                with opener(request, timeout=30) as response:
                    status = getattr(response, "status", 201)
            except urllib.error.HTTPError as error:
                raise ServiceBusPublishError(
                    f"Service Bus refused {event.lane_id}'s control event: HTTP {error.code}"
                ) from error
            if status != 201:
                raise ServiceBusPublishError(
                    f"Service Bus answered {event.lane_id}'s control event with HTTP {status}")

        await asyncio.to_thread(send)

    return publish_control
