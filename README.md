# Distributed Workflow Engine

A small, production-style workflow orchestration platform built incrementally to explore durable state, task delivery, retries, and worker failure recovery.

## Architecture

```text
Client ──HTTP──> FastAPI control plane ──> PostgreSQL (durable state)
                         │
                         └──────────────> Redis (future task delivery)
                                               │
                                         Go workers (future)
```

PostgreSQL will be the source of truth for workflow definitions and execution state. Redis will distribute ready work once execution is introduced. This separation means queue delivery can be repaired from persisted state; Redis is not the durable record of what a workflow has done.

The current foundation includes the API process, PostgreSQL, Redis, and service health checks. Phase 2 adds versioned workflow definitions and durable workflow/task run records. Execution and the Redis queue are still future phases.

## Requirements

- Docker with Docker Compose

## Environment configuration

Compose reads the local `.env` file. Copy `.env.example` to `.env` before starting the stack. The checked-in example uses development-only credentials; change them for your environment and keep `.env` private. If you change the PostgreSQL credentials after the database volume has already been initialized, update the existing database role too; the official PostgreSQL image only applies initialization variables when the data directory is empty.

## Run

```bash
docker compose up --build
```

The API is available at <http://localhost:8000>; interactive API docs are at <http://localhost:8000/docs>. Check API process health with:

```bash
curl http://localhost:8000/health
```

Stop the services with `docker compose down`. PostgreSQL data is stored in the `postgres_data` named volume and is retained when containers stop. To remove it, run `docker compose down -v`.

## Development

The API source is in `api/app`. Its `/health` endpoint confirms the process is responding; it does not yet check PostgreSQL or Redis connectivity. Compose waits for those dependencies to become healthy before starting the API.

Database schema changes use Alembic. The API container applies pending migrations before starting. To apply or inspect migrations from a local API environment, run `alembic upgrade head` or `alembic history` from `api/`, with `DATABASE_URL` set.

## Persistence model

- **Workflow:** stable identity and name.
- **Workflow version:** an immutable numbered definition revision.
- **Workflow step:** normalized step identity, task type, JSONB input, and dependency names. Dependencies remain a JSON array for this first iteration because they are definition data read as a whole; later DAG validation will verify referenced names and cycles.
- **Workflow run:** a particular execution pinned to a workflow version, with a version column reserved for optimistic concurrency control.
- **Task run:** one step in a run, with status, attempt number, input/result/error JSONB, and uniqueness per step in that run.

State values and database check constraints reject unknown statuses. Those constraints protect persisted data, but the transition rules (for example, pending → queued → running) will be implemented with execution logic in a later phase. Outputs and errors use JSONB because their shape depends on task type; names, identifiers, relationships, timestamps, and statuses remain relational and constrained.

To verify the database connection after startup, request `GET /health/database`. This executes `SELECT 1`; the regular `/health` checks only that the API process responds.

We will design and migrate these with the persistence phase, when state transitions and transaction boundaries are introduced.
