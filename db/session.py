# session.py
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import OperationalError
from contextlib import contextmanager
import time
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------
# Database configuration
# ---------------------------
DB_USER = os.getenv("POSTGRES_USER", "your_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "edl_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------------------------
# Engine creation with pooling
# ---------------------------
# Use pool_size and max_overflow to manage connection pool
engine = create_engine(
    DATABASE_URL,
    echo=False,              # Set True for SQL debug logging
    pool_size=10,            # Number of persistent connections
    max_overflow=20,         # Extra connections beyond pool_size
    pool_pre_ping=True,      # Checks if connection is alive
    pool_recycle=1800,       # Recycle connections every 30 min
    connect_args={"connect_timeout": 10},  # Timeout for initial connection
)

# Scoped session ensures thread-safe sessions in multi-threaded apps
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

# ---------------------------
# Context manager for sessions
# ---------------------------
@contextmanager
def get_session(retries: int = 3, retry_delay: int = 5):
    """
    Context manager for database sessions with retry logic.
    Usage:
        with get_session() as session:
            session.add(obj)
            session.commit()
    """
    attempt = 0
    while attempt < retries:
        session = Session()
        try:
            yield session
            session.commit()
            break
        except OperationalError as e:
            session.rollback()
            attempt += 1
            logger.warning(f"Database operation failed (attempt {attempt}/{retries}): {e}")
            time.sleep(retry_delay)
            if attempt >= retries:
                logger.error("Max retries reached. Raising exception.")
                raise

        except Exception as e:
            session.rollback()
            logger.error(f"Session failed: {e}")
            raise
        finally:
            session.close()
