import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.settings import db

logger = logging.getLogger(__name__)


def get_engine():
    """
    Build and return a SQLAlchemy engine from environment config.
    Raises a clear error if the connection string is incomplete —
    better to fail early here than get a cryptic error mid-pipeline.
    """
    if not all([db.user, db.password, db.host, db.name]):
        raise EnvironmentError(
            "Database credentials are missing. "
            "Check DB_USER, DB_PASSWORD, and DB_NAME in your .env file."
        )

    engine = create_engine(db.url, pool_pre_ping=True)

    # quick connectivity check before we hand the engine back
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
    except SQLAlchemyError as e:
        logger.error("Could not connect to the database: %s", e)
        raise

    return engine
