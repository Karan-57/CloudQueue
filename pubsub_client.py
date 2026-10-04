import json
import os
import time
from typing import List, Optional, Dict, Any
import config

_publisher = None
_subscriber = None


class MockPubSubMessage:
    def __init__(self, data: bytes, ack_id: str, attributes: Optional[Dict[str, str]] = None):
        self.data = data
        self.attributes = attributes or {}
        self.message = self
        self.ack_id = ack_id


class MockPubSubBroker:
    def __init__(self, clear_db=False):
        import threading
        self.queue = []
        self.unacked = {}
        self._msg_counter = 0
        self._lock = threading.Lock()
        self._ipc_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".gcp", "mock_pubsub.db")
        self._init_ipc_db()
        if clear_db:
            self.reset()

    def reset(self):
        with self._lock:
            self.queue.clear()
            self.unacked.clear()
            self._msg_counter = 0
            try:
                import sqlite3
                conn = sqlite3.connect(self._ipc_db, timeout=10.0)
                conn.execute("DELETE FROM mock_pubsub_queue")
                conn.commit()
                conn.close()
            except Exception:
                pass

    def _init_ipc_db(self):
        try:
            os.makedirs(os.path.dirname(self._ipc_db), exist_ok=True)
            import sqlite3
            conn = sqlite3.connect(self._ipc_db, timeout=10.0)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mock_pubsub_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT,
                    data BLOB,
                    attrs TEXT,
                    msg_id TEXT,
                    ack_id TEXT UNIQUE,
                    status TEXT,
                    created_at REAL
                );
            """)
            conn.commit()
            conn.close()
        except Exception:
            pass

    def publish(self, topic: str, data: bytes, **attrs):
        with self._lock:
            self._msg_counter += 1
            now = time.time()
            msg_id = f"mock-msg-{os.getpid()}-{self._msg_counter}-{int(now * 1000)}"
            ack_id = f"ack-{msg_id}"
            msg = MockPubSubMessage(data=data, ack_id=ack_id, attributes=attrs)
            self.queue.append(msg)

            try:
                import sqlite3
                conn = sqlite3.connect(self._ipc_db, timeout=10.0)
                conn.execute("""
                    INSERT INTO mock_pubsub_queue (topic, data, attrs, msg_id, ack_id, status, created_at)
                    VALUES (?, ?, ?, ?, ?, 'QUEUED', ?)
                """, (topic, data, json.dumps(attrs), msg_id, ack_id, now))
                conn.commit()
                conn.close()
            except Exception:
                pass

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

            if count < max_messages:
                try:
                    import sqlite3
                    conn = sqlite3.connect(self._ipc_db, timeout=10.0)
                    cursor = conn.cursor()
                    needed = max_messages - count
                    cursor.execute("""
                        SELECT id, data, attrs, ack_id FROM mock_pubsub_queue
                        WHERE status = 'QUEUED'
                        ORDER BY id ASC
                        LIMIT ?
                    """, (needed,))
                    rows = cursor.fetchall()
                    for r in rows:
                        row_id, r_data, r_attrs_json, r_ack = r
                        cursor.execute("UPDATE mock_pubsub_queue SET status = 'UNACKED' WHERE id = ?", (row_id,))
                        conn.commit()
                        attrs = json.loads(r_attrs_json) if r_attrs_json else {}
                        msg = MockPubSubMessage(data=r_data, ack_id=r_ack, attributes=attrs)
                        self.unacked[r_ack] = msg
                        received.append(msg)
                    conn.close()
                except Exception:
                    pass

            class _PullResponse:
                def __init__(self, msgs):
                    self.received_messages = msgs
            return _PullResponse(received)

    def acknowledge(self, subscription: str, ack_ids: List[str]):
        with self._lock:
            for aid in ack_ids:
                self.unacked.pop(aid, None)
            try:
                import sqlite3
                conn = sqlite3.connect(self._ipc_db, timeout=10.0)
                cursor = conn.cursor()
                cursor.executemany("DELETE FROM mock_pubsub_queue WHERE ack_id = ?", [(aid,) for aid in ack_ids])
                conn.commit()
                conn.close()
            except Exception:
                pass

    def modify_ack_deadline(self, subscription: str, ack_ids: List[str], ack_deadline_seconds: int = 0):
        with self._lock:
            if ack_deadline_seconds == 0:
                for aid in ack_ids:
                    msg = self.unacked.pop(aid, None)
                    if msg:
                        self.queue.append(msg)
                try:
                    import sqlite3
                    conn = sqlite3.connect(self._ipc_db, timeout=10.0)
                    cursor = conn.cursor()
                    cursor.executemany("UPDATE mock_pubsub_queue SET status = 'QUEUED' WHERE ack_id = ?", [(aid,) for aid in ack_ids])
                    conn.commit()
                    conn.close()
                except Exception:
                    pass


_mock_broker = None


def is_mock_enabled() -> bool:
    return os.environ.get("CLOUDQUEUE_MOCK_PUBSUB", "false").strip().lower() in ("true", "1", "yes")


def get_mock_broker() -> MockPubSubBroker:
    global _mock_broker
    if _mock_broker is None:
        _mock_broker = MockPubSubBroker()
    return _mock_broker


def set_clients(publisher=None, subscriber=None):
    global _publisher, _subscriber
    _publisher = publisher
    _subscriber = subscriber


def get_publisher():
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
    proj = project_id or config.GOOGLE_CLOUD_PROJECT
    top = topic_id or config.PUBSUB_TOPIC
    if not proj:
        if is_mock_enabled():
            return f"projects/mock-project/topics/{top}"
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set to use GCP Pub/Sub mode.")
    pub = get_publisher()
    if hasattr(pub, "topic_path"):
        return pub.topic_path(proj, top)
    return f"projects/{proj}/topics/{top}"


def get_subscription_path(project_id: Optional[str] = None, subscription_id: Optional[str] = None) -> str:
    proj = project_id or config.GOOGLE_CLOUD_PROJECT
    sub = subscription_id or config.PUBSUB_SUBSCRIPTION
    if not proj:
        if is_mock_enabled():
            return f"projects/mock-project/subscriptions/{sub}"
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set to use GCP Pub/Sub mode.")
    subscriber = get_subscriber()
    if hasattr(subscriber, "subscription_path"):
        return subscriber.subscription_path(proj, sub)
    return f"projects/{proj}/subscriptions/{sub}"


def publish_job(job_id: int, experiment_id: Optional[str] = None, timeout: float = 10.0) -> str:
    publisher = get_publisher()
    topic_path = get_topic_path()

    payload = {"job_id": int(job_id)}
    if experiment_id:
        payload["experiment_id"] = str(experiment_id)

    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    return str(future.result(timeout=timeout))


def publish_jobs_batch(
    job_ids: List[int],
    experiment_id: Optional[str] = None,
    timeout: float = 30.0
) -> List[str]:
    publisher = get_publisher()
    topic_path = get_topic_path()

    futures = []
    for jid in job_ids:
        payload = {"job_id": int(jid)}
        if experiment_id:
            payload["experiment_id"] = str(experiment_id)
        futures.append(publisher.publish(topic_path, data=json.dumps(payload).encode("utf-8")))

    return [str(f.result(timeout=timeout)) for f in futures]


def pull_messages(
    max_messages: int = 1,
    timeout: float = 5.0,
    subscription_path: Optional[str] = None
) -> List[Any]:
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
        if "deadline" in str(e).lower() or "timeout" in str(e).lower():
            return []
        raise e


def acknowledge_message(ack_id: str, subscription_path: Optional[str] = None):
    subscriber = get_subscriber()
    sub_path = subscription_path or get_subscription_path()

    if is_mock_enabled() or isinstance(subscriber, MockPubSubBroker):
        subscriber.acknowledge(subscription=sub_path, ack_ids=[ack_id])
        return

    subscriber.acknowledge(
        request={"subscription": sub_path, "ack_ids": [ack_id]}
    )


def nack_message(ack_id: str, subscription_path: Optional[str] = None):
    subscriber = get_subscriber()
    sub_path = subscription_path or get_subscription_path()

    if is_mock_enabled() or isinstance(subscriber, MockPubSubBroker):
        subscriber.modify_ack_deadline(subscription=sub_path, ack_ids=[ack_id], ack_deadline_seconds=0)
        return

    subscriber.modify_ack_deadline(
        request={"subscription": sub_path, "ack_ids": [ack_id], "ack_deadline_seconds": 0}
    )
