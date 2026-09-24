import logging
import os
import socket
import threading
import time
from uuid import UUID

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from app.config import redis_url
from app.db import SessionLocal
from app.services.scheduler import (
    claim_task,
    finish_task,
    heartbeat_task,
    process_task,
    publish_pending_outbox,
    reap_expired_leases,
)
from app.services.task_queue import TaskQueue

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("workflow.worker")
LEASE_SECONDS = float(os.environ.get("TASK_LEASE_SECONDS", "30"))
HEARTBEAT_INTERVAL = max(0.5, LEASE_SECONDS / 3)


def heartbeat_loop(task_id: UUID, worker_id: str, stop: threading.Event) -> None:
    while not stop.wait(HEARTBEAT_INTERVAL):
        try:
            with SessionLocal() as session:
                alive = heartbeat_task(session, task_id, worker_id, LEASE_SECONDS)
            if not alive:
                logger.warning("task lease no longer owned task_run_id=%s", task_id)
                return
        except SQLAlchemyError:
            logger.exception("heartbeat update failed task_run_id=%s", task_id)


def handle_message(
    queue: TaskQueue,
    worker_id: str,
    message_id: str,
    task_id: UUID,
    target_attempt: int,
) -> None:
    with SessionLocal() as session:
        claimed = claim_task(
            session,
            task_id,
            target_attempt,
            worker_id,
            LEASE_SECONDS,
        )
    if not claimed:
        queue.acknowledge(message_id)
        logger.info("stale or duplicate message acknowledged task_run_id=%s", task_id)
        return

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=heartbeat_loop,
        args=(task_id, worker_id, stop_heartbeat),
        name=f"heartbeat-{task_id}",
        daemon=True,
    )
    heartbeat.start()
    try:
        with SessionLocal() as session:
            output, error = process_task(task_id, session)
    finally:
        stop_heartbeat.set()
        heartbeat.join(timeout=HEARTBEAT_INTERVAL + 1)

    delay = 0.5
    while True:
        try:
            with SessionLocal() as session:
                finish_task(
                    session,
                    task_id,
                    worker_id,
                    output=output,
                    error=error,
                )
            queue.acknowledge(message_id)
            logger.info(
                "task result recorded task_run_id=%s attempt=%s message_id=%s",
                task_id,
                target_attempt,
                message_id,
            )
            return
        except (SQLAlchemyError, RedisError):
            logger.exception("result/ack failed; retrying task message task_run_id=%s", task_id)
            time.sleep(delay)
            delay = min(delay * 2, 15)


def run_worker() -> None:
    worker_id = os.environ.get("WORKER_ID", socket.gethostname())
    queue = TaskQueue(redis_url())
    logger.info(
        "worker starting worker_id=%s lease_seconds=%s",
        worker_id,
        LEASE_SECONDS,
    )

    try:
        while True:
            try:
                queue.ensure_consumer_group()
                with SessionLocal() as session:
                    reap_expired_leases(session)
                    publish_pending_outbox(session, queue)

                pending = queue.read(worker_id, pending=True)
                if pending:
                    for message_id, task_id, attempt in pending:
                        handle_message(queue, worker_id, message_id, task_id, attempt)
                    continue

                messages = queue.read(worker_id, pending=False)
                for message_id, task_id, attempt in messages:
                    handle_message(queue, worker_id, message_id, task_id, attempt)
            except (SQLAlchemyError, RedisError):
                logger.exception("worker loop failed; reconnecting")
                time.sleep(1)
            except Exception:
                logger.exception("unexpected worker loop failure")
                time.sleep(1)
    finally:
        queue.close()


if __name__ == "__main__":
    run_worker()
