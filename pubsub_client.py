import json
import logging
import os
import time
from typing import List, Optional, Tuple, Dict, Any
import config

logger = logging.getLogger("cloudqueue.pubsub")

# Global singleton client instances (lazily initialized)
_publisher = None
_subscriber = None


class MockPubSubMessage:
    """Mock message container mirroring google.cloud.pubsub_v1 types."""
    def __init__(self, data: bytes, ack_id: str, attributes: Optional[Dict[str, str]] = None):
        self.data = data
        self.attributes = attributes or {}
        self.message = self
        self.ack_id = ack_id


class MockPubSubBroker:
    """
    In-memory Pub/Sub broker for offline testing and verification
    when GCP credentials / emulator are not present.
    Mimics exact Pub/Sub behavior: FIFO queue, ACKs, NACKs, and message IDs.
    """
    def __init__(self):
        import threading
        self.queue = []
        self.unacked = {}
        self._msg_counter = 0
        self._lock = threading.Lock()

    def publish(self, topic: str, data: bytes, **attrs):
        with self._lock:
            self._msg_counter += 1
            msg_id = f"mock-msg-{self._msg_counter}"
            ack_id = f"ack-{msg_id}"
            msg = MockPubSubMessage(data=data, ack_id=ack_id, attributes=attrs)
            self.queue.append(msg)
            
            class _Future:
                def result(self, timeout=None):
                    return msg_id
            return _Future()

    def pull(self, subscription: str, max_messages: int = 1, timeout: float = 5.0):
        with self._lock:
            received = []
            count = 0
            while self.queue and count < max_messages:
                msg = self.queue.pop(0)
                self.unacked[msg.ack_id] = msg
                received.append(msg)
                count += 1
            
            class _PullResponse:
                def __init__(self, msgs):
                    self.received_messages = msgs
            return _PullResponse(received)

    def acknowledge(self, subscription: str, ack_ids: List[str]):
        with self._lock:
            for aid in ack_ids:
                self.unacked.pop(aid, None)

    def modify_ack_deadline(self, subscription: str, ack_ids: List[str], ack_deadline_seconds: int = 0):
        with self._lock:
            if ack_deadline_seconds == 0:
                for aid in ack_ids:
                    msg = self.unacked.pop(aid, None)
                    if msg:
                        self.queue.append(msg)


_mock_broker = None


def is_mock_enabled() -> bool:
    """Returns True if the offline mock broker is enabled via environment."""
    return os.environ.get("CLOUDQUEUE_MOCK_PUBSUB", "false").strip().lower() in ("true", "1", "yes")


def get_mock_broker() -> MockPubSubBroker:
    global _mock_broker
    if _mock_broker is None:
        _mock_broker = MockPubSubBroker()
    return _mock_broker


def set_clients(publisher=None, subscriber=None):
    """Overrides the publisher and subscriber clients (e.g., for test fixtures)."""
    global _publisher, _subscriber
    _publisher = publisher
    _subscriber = subscriber


def get_publisher():
    """Lazily initializes and returns the Google Cloud Pub/Sub PublisherClient."""
    global _publisher
    if _publisher is not None:
        return _publisher

    if is_mock_enabled():
        _publisher = get_mock_broker()
        return _publisher

    from google.cloud import pubsub_v1
    _publisher = pubsub_v1.PublisherClient()
    return _publisher


def get_subscriber():
    """Lazily initializes and returns the Google Cloud Pub/Sub SubscriberClient."""
    global _subscriber
    if _subscriber is not None:
        return _subscriber

    if is_mock_enabled():
        _subscriber = get_mock_broker()
        return _subscriber

    from google.cloud import pubsub_v1
    _subscriber = pubsub_v1.SubscriberClient()
    return _subscriber


def get_topic_path(project_id: Optional[str] = None, topic_id: Optional[str] = None) -> str:
    """Formats the fully-qualified Pub/Sub topic path."""
    proj = project_id or config.GOOGLE_CLOUD_PROJECT
    top = topic_id or config.PUBSUB_TOPIC
    if not proj:
        if is_mock_enabled():
            return f"projects/mock-project/topics/{top}"
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT environment variable must be set to use GCP Pub/Sub mode."
        )
    pub = get_publisher()
    if hasattr(pub, "topic_path"):
        return pub.topic_path(proj, top)
    return f"projects/{proj}/topics/{top}"


def get_subscription_path(project_id: Optional[str] = None, subscription_id: Optional[str] = None) -> str:
    """Formats the fully-qualified Pub/Sub subscription path."""
    proj = project_id or config.GOOGLE_CLOUD_PROJECT
    sub = subscription_id or config.PUBSUB_SUBSCRIPTION
    if not proj:
        if is_mock_enabled():
            return f"projects/mock-project/subscriptions/{sub}"
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT environment variable must be set to use GCP Pub/Sub mode."
        )
    subscriber = get_subscriber()
    if hasattr(subscriber, "subscription_path"):
        return subscriber.subscription_path(proj, sub)
    return f"projects/{proj}/subscriptions/{sub}"


def publish_job(job_id: int, experiment_id: Optional[str] = None, timeout: float = 10.0) -> str:
    """
    Publishes a single job ID to the Pub/Sub topic.
    Payload contains ONLY the existing SQLite job_id (and experiment_id if applicable).
    
    Structure:
      Normal job: {"job_id": 416}
      Experiment job: {"job_id": 417, "experiment_id": "EXP-008"}
    """
    publisher = get_publisher()
    topic_path = get_topic_path()

    payload = {"job_id": int(job_id)}
    if experiment_id:
        payload["experiment_id"] = str(experiment_id)

    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    message_id = future.result(timeout=timeout)
    return str(message_id)


def publish_jobs_batch(
    job_ids: List[int],
    experiment_id: Optional[str] = None,
    timeout: float = 30.0
) -> List[str]:
    """
    Publishes multiple job IDs in batch to the Pub/Sub topic.
    Returns the list of generated Pub/Sub message IDs.
    """
    publisher = get_publisher()
    topic_path = get_topic_path()

    futures = []
    for jid in job_ids:
        payload = {"job_id": int(jid)}
        if experiment_id:
            payload["experiment_id"] = str(experiment_id)
        data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(topic_path, data=data)
        futures.append(future)

    message_ids = []
    for f in futures:
        message_ids.append(str(f.result(timeout=timeout)))

    return message_ids


def pull_messages(
    max_messages: int = 1,
    timeout: float = 5.0,
    subscription_path: Optional[str] = None
) -> List[Any]:
    """
    Pulls up to `max_messages` from the configured Pub/Sub subscription.
    Returns a list of received message objects.
    Catches DeadlineExceeded or timeout exceptions cleanly and returns [].
    """
    subscriber = get_subscriber()
    sub_path = subscription_path or get_subscription_path()

    if is_mock_enabled() or isinstance(subscriber, MockPubSubBroker):
        response = subscriber.pull(subscription=sub_path, max_messages=max_messages, timeout=timeout)
        return list(response.received_messages)

    try:
        from google.api_core.exceptions import DeadlineExceeded, RetryError
        response = subscriber.pull(
            request={"subscription": sub_path, "max_messages": max_messages},
            timeout=timeout
        )
        return list(response.received_messages)
    except (DeadlineExceeded, RetryError):
        return []
    except Exception as e:
        # If timeout occurred inside gRPC
        if "deadline" in str(e).lower() or "timeout" in str(e).lower():
            return []
        raise e


def acknowledge_message(ack_id: str, subscription_path: Optional[str] = None):
    """Acknowledges a received message by its ack_id."""
    subscriber = get_subscriber()
    sub_path = subscription_path or get_subscription_path()

    if is_mock_enabled() or isinstance(subscriber, MockPubSubBroker):
        subscriber.acknowledge(subscription=sub_path, ack_ids=[ack_id])
        return

    subscriber.acknowledge(
        request={"subscription": sub_path, "ack_ids": [ack_id]}
    )


def nack_message(ack_id: str, subscription_path: Optional[str] = None):
    """
    NACKs a message by resetting its ack deadline to 0 seconds,
    re-queuing it immediately for other available workers.
    """
    subscriber = get_subscriber()
    sub_path = subscription_path or get_subscription_path()

    if is_mock_enabled() or isinstance(subscriber, MockPubSubBroker):
        subscriber.modify_ack_deadline(subscription=sub_path, ack_ids=[ack_id], ack_deadline_seconds=0)
        return

    subscriber.modify_ack_deadline(
        request={"subscription": sub_path, "ack_ids": [ack_id], "ack_deadline_seconds": 0}
    )
