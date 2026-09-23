from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import engine
from app.routes.workflows import router as workflows_router

app = FastAPI(
    title="Workflow Engine API",
    description="Control plane for a distributed workflow engine.",
    version="0.1.0",
)
app.include_router(workflows_router)


@app.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    """Report that the API process is responding."""
    return {"status": "ok"}


@app.get("/health/database", tags=["operations"])
def database_health() -> dict[str, str]:
    """Check the API can execute a query against PostgreSQL."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ok", "database": "ok"}
