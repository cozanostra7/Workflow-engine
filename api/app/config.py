import os


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://workflow:workflow@localhost:5432/workflow",
    )
