"""PostgreSQL database operations with Psycopg3 for fast bulk loading."""

import io
import logging
from typing import List, Set

import polars as pl
import psycopg

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database handler with temp table upsert."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._pk_cache: dict = {}
        self.conn = None

    def connect(self):
        """Establish database connection with retry."""
        if self.conn is not None:
            return

        for attempt in range(4):
            try:
                self.conn = psycopg.connect(self.database_url, autocommit=False)
                return
            except psycopg.OperationalError as e:
                if attempt == 3:
                    logger.error(f"Failed to connect to database: {e}")
                    raise
                import time
                time.sleep(2 ** attempt)

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_postgres_stats(self) -> dict:
        """Fetch real-time stats from Postgres about temp files and memory."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                # Get temp file usage for current DB
                cur.execute("SELECT temp_files, temp_bytes FROM pg_stat_database WHERE datname = current_database()")
                res = cur.fetchone()
                temp_files = res[0] if res else 0
                temp_bytes = res[1] if res else 0

                # Get active backends count
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
                active_backends = cur.fetchone()[0]

                return {
                    "temp_files": temp_files,
                    "temp_bytes_mb": temp_bytes / (1024 * 1024),
                    "active_backends": active_backends
                }
        except Exception:
            return {}

    def log_active_queries(self):
        """Log what Postgres is currently doing (for Phase 3 debugging)."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT query, wait_event_type, wait_event 
                    FROM pg_stat_activity 
                    WHERE state = 'active' AND query NOT LIKE '%pg_stat_activity%'
                    LIMIT 3
                """)
                for row in cur.fetchall():
                    query_snippet = row[0][:60].replace("\n", " ")
                    logger.info(f"  [PG ACTIVE] {query_snippet}... | Wait: {row[1]}:{row[2]}")
        except Exception:
            pass

    def bulk_load(self, df: pl.DataFrame, table_name: str, columns: List[str]):
        """Direct COPY to table (fast, no temp table, no upsert)."""
        if df.is_empty():
            return

        self.connect()

        try:
            # Write CSV to BytesIO
            csv_buffer = io.BytesIO()
            df.write_csv(csv_buffer, include_header=False)
            csv_bytes = csv_buffer.getvalue()
            
            # Remove null bytes
            if b"\x00" in csv_bytes:
                csv_bytes = csv_bytes.replace(b"\x00", b"")

            # Direct COPY to table
            columns_str = ", ".join([f'"{col}"' for col in columns])
            with self.conn.cursor() as cur:
                with cur.copy(f'COPY {table_name} ({columns_str}) FROM STDIN WITH CSV ENCODING \'UTF8\'') as copy:
                    copy.write(csv_bytes)
            
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in bulk_load for {table_name}: {e}")
            raise

