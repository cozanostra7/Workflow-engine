from uuid import UUID

from redis import Redis
from redis.exceptions import ResponseError


STREAM_NAME = "workflow:ready-tasks"
CONSUMER_GROUP = "workflow-workers"


class TaskQueue:
    def __init__(self, redis_url: str) -> None:
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    def ensure_consumer_group(self) -> None:
        try:
            self.redis.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, task_run_id: UUID) -> str:
        return self.redis.xadd(STREAM_NAME, {"task_run_id": str(task_run_id)})

    def read(self, consumer: str, *, pending: bool = False) -> list[tuple[str, UUID]]:
        entry_id = "0" if pending else ">"
        messages = self.redis.xreadgroup(
            CONSUMER_GROUP,
            consumer,
            {STREAM_NAME: entry_id},
            count=1,
            block=1000,
        )
        parsed: list[tuple[str, UUID]] = []
        for _, entries in messages:
            for message_id, fields in entries:
                try:
                    parsed.append((message_id, UUID(fields["task_run_id"])))
                except (KeyError, ValueError):
                    parsed.append((message_id, UUID(int=0)))
        return parsed

    def acknowledge(self, message_id: str) -> None:
        self.redis.xack(STREAM_NAME, CONSUMER_GROUP, message_id)

    def close(self) -> None:
        self.redis.close()
