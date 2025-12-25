#!/usr/bin/env python3
"""
CNPJ Data Pipeline - High Performance Parallel Execution.
"""

import argparse
import logging
import sys
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import threading

from pathlib import Path
import zipfile
import time

from config import config
from database import Database
from processor import process_csv_file, get_file_type
from utils import ResourceMonitor, log_resource_summary

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Reference tables (small, can all run in parallel)
REFERENCE_TYPES = ["CNAECSV", "MOTICSV", "MUNICCSV", "NATJUCSV", "PAISCSV", "QUALSCSV"]

def get_optimal_batch_size(file_type: str) -> int:
    """Return optimal batch size based on file type."""
    if file_type in REFERENCE_TYPES:
        return 10000  # Small tables, small batches
    else:
        return 500000  # All data files use same batch size


def extract_and_process(zip_path: Path, database_url: str) -> bool:
    """Extract ZIP, process CSV, ingest to DB, and cleanup CSV."""
    monitor = ResourceMonitor(interval=1.0)
    monitor.start()
    
    db = Database(database_url)
    csv_path = None
    
    try:
        file_type = get_file_type(zip_path.name)
        if not file_type:
            logger.error(f"Cannot determine file type for {zip_path.name}")
            return False
            
        batch_size = get_optimal_batch_size(file_type)
        
        # Extract CSV from ZIP with CRC validation
        try:
            with zipfile.ZipFile(zip_path) as z:
                # Test ZIP integrity first
                bad_file = z.testzip()
                if bad_file:
                    logger.error(f"Corrupted file in ZIP: {bad_file}. Please re-download {zip_path.name}")
                    return False
                    
                largest_member = max(z.infolist(), key=lambda x: x.file_size)
                csv_path = zip_path.parent / f"{zip_path.stem}_{largest_member.filename}"
                
                if not csv_path.exists():
                    with z.open(largest_member) as source, open(csv_path, "wb") as target:
                        import shutil
                        shutil.copyfileobj(source, target)
        except zipfile.BadZipFile as e:
            logger.error(f"Corrupted ZIP: {zip_path.name}. Error: {e}. Please re-download.")
            return False
        
        # Process and ingest
        rows_total = 0
        for df, table_name, columns in process_csv_file(csv_path, batch_size):
            db.bulk_load(df, table_name, columns)
            rows_total += len(df)
        
        logger.info(f"✓ {zip_path.name}: {rows_total:,} rows")
        return True
        
    except Exception as e:
        logger.error(f"✗ {zip_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        summary = monitor.stop()
        log_resource_summary(zip_path.name, monitor.get_summary())
        db.disconnect()
        
        # Cleanup extracted CSV
        if csv_path and csv_path.exists():
            try:
                csv_path.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete {csv_path.name}: {e}")

# No longer needed - indexes are not created in initial.sql

def create_indexes(database_url: str, work_mem_session: str = "1GB", parallel_workers: int = 2):
    """Create indexes and reconstruct tables for global deduplication."""
    logger.info("Starting Phase 3: Reconstruction and Indexing...")
    db = Database(database_url)
    try:
        db.connect()
        
        with open("create_indexes.sql") as f:
            content = f.read()
        
        # Split by blocks
        blocks = content.split("-- BLOCK:")
        with db.conn.cursor() as cur:
            # Disable JIT for complex parallel reconstruction to avoid stability issues
            cur.execute("SET jit = off")
            # Optimize for parallel sorting (crucial for DISTINCT ON)
            cur.execute(f"SET max_parallel_workers_per_gather = {parallel_workers}")
            cur.execute("SET max_parallel_workers = 8")
            # Adaptive work_mem to avoid disk sorts (aggressive for 32GB)
            cur.execute(f"SET work_mem = '{work_mem_session}'")
            cur.execute("SET maintenance_work_mem = '8GB'")
            
            for block in blocks:
                if not block.strip():
                    continue
                    
                lines = block.strip().split("\n")
                block_name = lines[0].strip()
                sql = "\n".join(lines[1:]).strip()
                
                if sql:
                    logger.info(f"Executing reconstruction block: {block_name}...")
                    monitor = ResourceMonitor(interval=2.0)
                    monitor.start()
                    
                    stop_event = threading.Event()
                    
                    # Log PG stats periodically during long SQL blocks
                    def pg_logger():
                        db_stats = Database(database_url)
                        try:
                            while not stop_event.is_set():
                                db_stats.log_active_queries()
                                stats = db_stats.get_postgres_stats()
                                if stats.get("temp_bytes_mb", 0) > 0:
                                    logger.info(f"  [PG DISK] Temp Files: {stats['temp_files']} | Vol: {stats['temp_bytes_mb']:.1f}MB")
                                # Wait with timeout to respond to stop_event
                                stop_event.wait(timeout=15)
                        finally:
                            db_stats.disconnect()

                    pg_thread = threading.Thread(target=pg_logger, daemon=True)
                    pg_thread.start()

                    cur.execute(sql)
                    db.conn.commit()
                    
                    stop_event.set()
                    pg_thread.join(timeout=1.0)
                    
                    summary = monitor.stop()
                    from utils import log_resource_summary
                    log_resource_summary(f"PHASE3-{block_name}", monitor.get_summary())
        
        # VACUUM requires autocommit mode
        logger.info("Running VACUUM ANALYZE to finalize...")
        old_autocommit = db.conn.autocommit
        db.conn.autocommit = True
        with db.conn.cursor() as cur:
            cur.execute("VACUUM ANALYZE")
        db.conn.autocommit = old_autocommit
        
        logger.info("✓ Phase 3 completed successfully")
    except Exception as e:
        logger.error(f"Error during reconstruction: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.disconnect()

def main():
    import psutil
    ram_gb = psutil.virtual_memory().total / (1024**3)
    logger.info(f"System Check: {ram_gb:.1f} GB RAM detected")
    
    # Adaptive sizing (Adjusted for 32GB machine)
    if ram_gb < 12:
        ingest_workers = 2
        parallel_reconstruction_workers = 2
        work_mem_session = "1GB"
        logger.info("Mode: SAFE (Optimized for <12GB RAM)")
    elif ram_gb < 24:
        ingest_workers = 4
        parallel_reconstruction_workers = 4
        work_mem_session = "2GB"
        logger.info("Mode: TURBO (Optimized for 16GB RAM)")
    else:
        # User has 32GB+ - Balanced Turbo Mode
        ingest_workers = 4
        parallel_reconstruction_workers = 2
        work_mem_session = "1GB"
        logger.info("Mode: BALANCED-TURBO (Safe Performance for 32GB RAM)")

    parser = argparse.ArgumentParser(description="High Performance CNPJ Pipeline")
    parser.add_argument("--skip-index-mgmt", action="store_true", help="Skip index drop/create")
    args = parser.parse_args()

    temp_dir = Path(config.temp_dir)
    if not temp_dir.exists():
        logger.error(f"Temp directory {temp_dir} does not exist")
        sys.exit(1)

    zip_files = list(temp_dir.glob("*.zip"))
    if not zip_files:
        logger.error(f"No ZIP files found in {temp_dir}")
        sys.exit(1)

    # Separate reference and data files
    reference_files = [f for f in zip_files if get_file_type(f.name) in REFERENCE_TYPES]
    data_files = [f for f in zip_files if get_file_type(f.name) not in REFERENCE_TYPES]
    
    # Sort data files by size (largest first to keep workers busy)
    data_files.sort(key=lambda f: f.stat().st_size, reverse=True)
    
    logger.info(f"Starting pipeline: {len(reference_files)} reference, {len(data_files)} data files")
    logger.info("Note: Indexes will be created AFTER all data is loaded for optimal performance")
    
    # Phase 1: Process ALL reference tables in parallel (they're tiny)
    if reference_files:
        logger.info(f"Phase 1: Processing {len(reference_files)} reference tables in parallel...")
        with ProcessPoolExecutor(max_workers=len(reference_files)) as executor:
            futures = {
                executor.submit(extract_and_process, f, config.database_url): f.name
                for f in reference_files
            }
            for future in as_completed(futures):
                future.result()
    
    # Phase 2: Process data files with adaptive workers
    logger.info(f"Phase 2: Processing {len(data_files)} data files ({ingest_workers} workers)...")
    
    with ProcessPoolExecutor(max_workers=ingest_workers) as executor:
        futures = {
            executor.submit(extract_and_process, f, config.database_url): f.name
            for f in data_files
        }
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            logger.info(f"Progress: {completed}/{len(data_files)}")
    
    # Recreate indexes after load with adaptive memory
    if not args.skip_index_mgmt:
        create_indexes(config.database_url, work_mem_session, parallel_reconstruction_workers)
    
    logger.info("✓✓✓ Pipeline completed!")

if __name__ == "__main__":
    main()
