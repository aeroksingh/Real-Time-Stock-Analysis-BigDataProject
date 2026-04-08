"""
database/init_db.py
-------------------
Reads schema.sql and applies it to PostgreSQL.
Run once before starting the consumer.

Usage:
    python database/init_db.py
"""

import os
import sys
import logging
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ------------------------------------------------------------------
# Bootstrap: add project root to path and load .env
# ------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("init_db")

# ------------------------------------------------------------------
# Build connection URL from environment variables
# ------------------------------------------------------------------
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB",   "stock_analysis")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "stockpass123")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def run_schema() -> None:
    """Connect to PostgreSQL and execute schema.sql."""
    schema_path = Path(__file__).parent / "schema.sql"

    if not schema_path.exists():
        logger.error("schema.sql not found at %s", schema_path)
        sys.exit(1)

    sql_text = schema_path.read_text(encoding="utf-8")

    try:
        engine = create_engine(DATABASE_URL, echo=False)
        with engine.connect() as conn:
            # Execute statement by statement (split on ';')
            for statement in sql_text.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
            conn.commit()
        logger.info("Schema applied successfully to %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    except Exception as exc:
        logger.error("Failed to apply schema: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    run_schema()