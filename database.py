"""PostgreSQL database operations with Psycopg3 for fast bulk loading."""

import logging
from typing import List, Set

import polars as pl
import psycopg
from psycopg import sql

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database handler using direct COPY (no temp tables)."""

    def __init__(self, database_url: str):
        self.database_url = database_url
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

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get all processed filenames for a directory."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT filename FROM processed_files WHERE directory = %s",
                    (directory,),
                )
                return {row[0] for row in cur.fetchall()}
        except Exception:
            return set()

    def mark_processed(self, directory: str, filename: str):
        """Mark a file as processed."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO processed_files (directory, filename)
                       VALUES (%s, %s)
                       ON CONFLICT (directory, filename) DO NOTHING""",
                    (directory, filename),
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error marking file as processed: {e}")

    def clear_processed_files(self, directory: str):
        """Clear all processed file records for a directory."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM processed_files WHERE directory = %s",
                    (directory,),
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error clearing processed files: {e}")

    def bulk_load(self, df: pl.DataFrame, table_name: str, columns: List[str]):
        """
        Direct COPY to table (no temp table, no upsert).
        Data must be deduplicated in Polars before calling this.
        """
        if df.is_empty():
            return

        self.connect()

        try:
            import io
            
            # Write CSV directly to BytesIO
            csv_buffer = io.BytesIO()
            df.write_csv(csv_buffer, include_header=False)
            csv_bytes = csv_buffer.getvalue()
            
            # Remove null bytes if present
            if b"\x00" in csv_bytes:
                csv_bytes = csv_bytes.replace(b"\x00", b"")

            # Direct COPY to final table
            columns_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
            copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ENCODING 'UTF8')").format(
                sql.Identifier(table_name), columns_sql
            )

            with self.conn.cursor() as cur:
                with cur.copy(copy_query) as copy:
                    copy.write(csv_bytes)
            
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in bulk_load for {table_name}: {e}")
            raise
