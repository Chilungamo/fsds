"""Configuration read from the environment (or a local .env file).

Nothing secret is hard-coded: database credentials and the SEC User-Agent
come from environment variables. See .env.example.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def sec_headers() -> dict:
    """HTTP headers for SEC EDGAR.

    The SEC requires a descriptive User-Agent with contact details, e.g.
    ``"Jane Doe research jane@example.com"``. Set SEC_USER_AGENT in .env.
    """
    ua = os.getenv("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError(
            "SEC_USER_AGENT is not set. Add e.g. "
            "SEC_USER_AGENT='Your Name your.email@example.com' to .env"
        )
    return {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}


def db_url():
    """SQLAlchemy URL for the Postgres database, built from DB_* variables."""
    from sqlalchemy.engine.url import URL

    password = os.getenv("DB_PASS")
    if password is None:
        raise RuntimeError("DB_PASS is not set. Add it to .env (see .env.example).")
    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=password,
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "fsds"),
    )


def get_engine():
    from sqlalchemy import create_engine

    return create_engine(db_url())
