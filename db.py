"""PostgreSQL connection helpers (Neon, Supabase, etc.)."""
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_database_url() -> str:
    """Build a SQLAlchemy/psycopg2 URL from DATABASE_URL or POSTGRES_* env vars."""
    url = os.getenv("DATABASE_URL")
    if url:
        # Some hosts (e.g. Heroku-style) use postgres:// — SQLAlchemy expects postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        return url
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    if not all([host, user, password]):
        raise ValueError(
            "Set DATABASE_URL or POSTGRES_HOST, POSTGRES_USER, and POSTGRES_PASSWORD"
        )
    ssl = os.getenv("POSTGRES_SSLMODE", "require")
    user_q = quote_plus(user)
    pass_q = quote_plus(password)
    return f"postgresql+psycopg2://{user_q}:{pass_q}@{host}:{port}/{dbname}?sslmode={ssl}"


def get_engine() -> Engine:
    """Shared SQLAlchemy engine for pandas and SQL execution."""
    return create_engine(get_database_url(), pool_pre_ping=True)
