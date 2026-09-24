import logging
import os
import socket
import time
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError

from app.config import redis_url
from app.db import SessionLocal
from app.services.scheduler import claim_task, finish_task, process_task, publish_pending_outbox
from app.services.task_queue import TaskQueue

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("workflow.worker")


def handle_message(queue: TaskQueue, consumer: str, message_id: str, task_id: UUID, *, redelivered: bool) -> None:
    retry_delay = 0.5
    while True:
        try:
            with SessionLocal() as session:
                claimed = claim_task(session, task_id, redelivered=redelivered)

            if claimed:
                with SessionLocal() as session:
                    output, error = process_task(task_id, session)
                with SessionLocal() as session:
                    finish_task(session, task_id, output=output, error=error)

            queue.acknowledge(message_id)
            logger.info("task handled task_run_id=%s message_id=%s", task_id, message_id)
            return
        except (SQLAlchemyError, OSError) as exc:
            logger.exception("task handling failed; retaining stream message: %s", exc)
            redelivered = True
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 15)
        except Exception:
            logger.exception("unexpected worker error; retaining stream message")
            redelivered = True
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 15)


def run_worker() -> None:
    consumer = os.environ.get("WORKER_ID", socket.gethostname())
    queue = TaskQueue(redis_url())
    pending_on_startup = True
    logger.info("worker starting worker_id=%s", consumer)

    try:
        while True:
            try:
                queue.ensure_consumer_group()
                with SessionLocal() as session:
                    publish_pending_outbox(session, queue)

                messages = queue.read(consumer, pending=pending_on_startup)
                if pending_on_startup:
                    if not messages:
                        pending_on_startup = False
                    for message_id, task_id in messages:
                        handle_message(queue, consumer, message_id, task_id, redelivered=True)
                    continue

                for message_id, task_id in messages:
                    handle_message(queue, consumer, message_id, task_id, redelivered=False)
            except Exception:
                logger.exception("worker loop failed; will reconnect")
                time.sleep(1)
    finally:
        queue.close()


if __name__ == "__main__":
    run_worker()
