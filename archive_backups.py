#!/usr/bin/env python3

import os
import argparse
import logging
from datetime import datetime, timedelta
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from pathlib import Path

REQUIRED_CONFIG_KEYS = [
    ('paths', 'log_dir'),
    ('service_account', 'path'),
    ('storage', 'source_bucket'),
    ('storage', 'target_bucket'),
    ('storage', 'retention_days'),
    ('databases',)
]

def load_config(config_path: str) -> dict:
    """Load configuration from yaml file and validate required keys."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    for keys in REQUIRED_CONFIG_KEYS:
        d = config
        for key in keys:
            if key not in d:
                raise KeyError(f"Missing config key: {'.'.join(keys)}")
            d = d[key] if isinstance(d, dict) else d
    return config

def setup_logging(log_dir: str, log_name: str) -> None:
    """Configure logging with both file and console output."""
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = Path(log_dir) / f"{today}_{log_name}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(str(log_file)),
            logging.StreamHandler()
        ]
    )

def authenticate(service_account_path: str) -> storage.Client:
    """Authenticate with Google Cloud using service account."""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
    return storage.Client()

def extract_backup_date(blob_name: str) -> datetime | None:
    """Extract date from backup filename."""
    try:
        # Get just the filename without the path
        filename = os.path.basename(blob_name)
        # Extract the date part (should be at the start of the filename)
        date_str = filename.split('_')[0]
        return datetime.strptime(date_str, '%Y-%m-%d')
    except Exception as e:
        logging.debug(f"Failed to extract date from {blob_name}: {str(e)}")
        return None

def should_archive(backup_date: datetime | None, threshold_days: int) -> bool:
    """Check if backup should be archived based on age."""
    if backup_date is None:
        return False
    age_days = (datetime.utcnow() - backup_date).days
    return age_days >= threshold_days

def move_blob(blob: storage.Blob, target_bucket: storage.Bucket, target_path: str, dry_run: bool) -> bool:
    """Move blob to target bucket with verification."""
    if dry_run:
        logging.info(f"[DRY-RUN] Would move: {blob.name} → {target_path}")
        return True

    try:
        new_blob = blob.bucket.copy_blob(
            blob, target_bucket, target_path
        )
        if new_blob:
            blob.delete()
            logging.info(f"MOVED: {blob.name} → {target_path}")
            return True
        else:
            logging.error(f"Failed to copy blob: {blob.name}")
            return False
    except Exception as e:
        logging.error(f"Error moving blob {blob.name}: {str(e)}")
        return False

def process_server_backups(server: str, db_config: dict, source_bucket: storage.Bucket, 
                         target_bucket: storage.Bucket, threshold_days: int, 
                         dry_run: bool) -> tuple[int, int]:
    """Process backups for a specific server."""
    server_source_path = f"{db_config['source_path']}/{server}"
    moved_count = 0
    processed_count = 0

    blobs = list(source_bucket.list_blobs(prefix=server_source_path))
    for blob in blobs:
        processed_count += 1
        if not blob.name.endswith(db_config['file_extension']):
            logging.debug(f"Skipping non-backup file: {blob.name}")
            continue

        backup_date = extract_backup_date(blob.name)
        if backup_date is None:
            continue

        if should_archive(backup_date, threshold_days):
            # Maintain the same directory structure in target
            target_path = blob.name.replace(
                db_config['source_path'],
                db_config['target_path']
            )
            if move_blob(blob, target_bucket, target_path, dry_run):
                moved_count += 1

    return moved_count, processed_count

def process_backups_parallel(db_type: str, db_config: dict, 
                           source_bucket: storage.Bucket, target_bucket: storage.Bucket, 
                           threshold_days: int, dry_run: bool, max_workers: int) -> int:
    """Process backups in parallel using thread pool."""
    total_moved = 0
    total_processed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_server = {
            executor.submit(
                process_server_backups,
                server,
                db_config,
                source_bucket,
                target_bucket,
                threshold_days,
                dry_run
            ): server
            for server in db_config['servers']
        }

        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                moved, processed = future.result()
                total_moved += moved
                total_processed += processed
                logging.info(f"Server {server}: Moved {moved}/{processed} files")
            except Exception as e:
                logging.error(f"Error processing server {server}: {str(e)}")

    return total_moved

def main():
    """Main function to run the backup archival process."""
    parser = argparse.ArgumentParser(description="Archive GCS backups older than threshold.")
    parser.add_argument('--dry-run', action='store_true', help="Preview actions without executing them.")
    parser.add_argument('--days', type=int, help="Override retention age in days.")
    parser.add_argument('--config', type=str, default='config.yaml', help="Path to config file.")
    parser.add_argument('--max-workers', type=int, default=4, help="Number of parallel workers.")
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return

    setup_logging(config['paths']['log_dir'], 'archive_backups')
    logging.info("Starting backup archival script")

    client = authenticate(config['service_account']['path'])
    retention_days = args.days if args.days is not None else config['storage']['retention_days']
    logging.info(f"Using retention period of {retention_days} days")

    source_bucket = client.bucket(config['storage']['source_bucket'])
    target_bucket = client.bucket(config['storage']['target_bucket'])
    total_archived = 0

    for db_type, db_config in config['databases'].items():
        logging.info(f"Processing {db_type} backups")
        count = process_backups_parallel(
            db_type, db_config, source_bucket, target_bucket,
            retention_days, args.dry_run, args.max_workers
        )
        action = "Would move" if args.dry_run else "Moved"
        logging.info(f"{action} {count} {db_type} backups")
        total_archived += count

    logging.info(f"Archive process complete. Total files {('to move' if args.dry_run else 'moved')}: {total_archived}")

if __name__ == "__main__":
    main()
