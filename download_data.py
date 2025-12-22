#!/usr/bin/env python3
"""
Standalone Download Script for CNPJ Data.
Downloads all ZIP files for a given month into the temp directory without extracting them.
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from config import config
from downloader import Downloader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def download_file(url: str, dest_path: Path, retry_attempts: int = 3):
    """Download a single file with progress bar and retries."""
    if dest_path.exists():
        # Check if it's a valid file (could add size check here)
        logger.info(f"File already exists: {dest_path.name}")
        return True

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(dest_path, "wb") as f, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc=dest_path.name,
                leave=False
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {dest_path.name}: {e}")
            if attempt == retry_attempts - 1:
                return False

def main():
    parser = argparse.ArgumentParser(description="Download CNPJ ZIP files")
    parser.add_argument("--month", type=str, help="Month to download (YYYY-MM)")
    parser.add_argument("--workers", type=int, default=config.download_workers, help="Number of parallel downloads")
    args = parser.parse_args()

    downloader = Downloader(config)
    
    try:
        if args.month:
            directory = args.month
        else:
            directory = downloader.get_latest_directory()
            logger.info(f"Latest directory identified: {directory}")

        files = downloader.get_directory_files(directory)
        logger.info(f"Found {len(files)} files to download in {directory}")

        temp_path = Path(config.temp_dir)
        temp_path.mkdir(exist_ok=True)

        base_url = f"{config.base_url}/{directory}"
        
        results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_file = {
                executor.submit(download_file, f"{base_url}/{f}", temp_path / f): f 
                for f in files
            }
            
            with tqdm(total=len(files), desc="Total Progress") as pbar:
                for future in as_completed(future_to_file):
                    filename = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            results.append(filename)
                        else:
                            logger.error(f"Failed to download {filename}")
                    except Exception as exc:
                        logger.error(f"{filename} generated an exception: {exc}")
                    pbar.update(1)

        logger.info(f"Downloaded {len(results)}/{len(files)} files.")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
