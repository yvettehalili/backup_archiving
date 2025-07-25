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
    # Basic validation
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

def extract_backup_date(filename: str) -> datetime | None:
    """Extract date from backup filename."""
    try:
        date_part = filename.split('_')[0]
        return datetime.strptime(date_part, '%Y-%m-%d')
    except Exception:
        return None

def should_archive(backup_date: datetime | None, threshold_days: int) -> bool:
    """Check if backup should be archived based on age."""
    return backup_date is not None and (datetime.utcnow() - backup_date).days >= threshold_days

def move_blob(blob: storage.Blob, target_bucket: storage.Bucket, target_path: str, dry_run: bool) -> bool:
    """Move blob to target bucket with verification."""
    source_md5 = blob.md5_hash
    if dry_run:
        logging.info(f"[DRY-RUN] Would move: {blob.name} → {target_path}")
        return True

    # Check if target exists
    if target_bucket.blob(target_path).exists():
        logging.warning(f"Target blob {target_path} already exists and will be overwritten.")

    # Copy blob to target bucket
    new_blob = blob.copy_to_bucket(target_bucket, new_name=target_path)
    new_blob.reload()

    if new_blob.md5_hash != source_md5:
        logging.warning(f"MD5 mismatch: {blob.name}")
        return False

    blob.delete()
    logging.info(f"MOVED: {blob.name} → {target_path}")
    return True

def process_backups_parallel(db_type: str, db_config: dict, 
                             source_bucket: storage.Bucket, target_bucket: storage.Bucket, 
                             threshold_days: int, dry_run: bool, max_workers: int) -> int:
    """Process backups in parallel using thread pool."""
    blobs = list(source_bucket.list_blobs(prefix=db_config['source_path']))
    moved_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for blob in blobs:
            if not blob.name.endswith(db_config['file_extension']):
                logging.debug(f"Skipping (extension): {blob.name}")
                continue

            backup_date = extract_backup_date(blob.name)
            if backup_date is None:
                logging.warning(f"Could not extract date from filename: {blob.name}")
                continue
            if not should_archive(backup_date, threshold_days):
                logging.debug(f"Skipping (not old enough): {blob.name}")
                continue

            # Safe path replacement for compatibility
            if blob.name.startswith(db_config['source_path']):
                target_path = db_config['target_path'] + blob.name[len(db_config['source_path']):]
            else:
                target_path = db_config['target_path'] + blob.name

            futures.append(
                executor.submit(move_blob, blob, target_bucket, target_path, dry_run)
            )

        for future in as_completed(futures):
            try:
                if future.result():
                    moved_count += 1
            except Exception as e:
                logging.error(f"Error: {e}")

    return moved_count

def main():
    """Main function to run the backup archival process."""
    parser = argparse.ArgumentParser(description="Archive GCS backups older than threshold.")
    parser.add_argument('--dry-run', action='store_true', help="Preview actions without executing them.")
    parser.add_argument('--days', type=int, help="Override retention age in days.")
    parser.add_argument('--config', type=str, default='config.yaml', help="Path to config YAML file.")
    parser.add_argument('--max-workers', type=int, default=4, help="Number of parallel workers.")
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return

    # Setup logging
    setup_logging(config['paths']['log_dir'], 'archive_backups')

    logging.info("Starting backup archival script")
    client = authenticate(config['service_account']['path'])

    # Get retention days from args or config
    retention_days = args.days if args.days is not None else config['storage']['retention_days']

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

    logging.info(f"Archive process complete. Total files {'to move' if args.dry_run else 'moved'}: {total_archived}")

if __name__ == "__main__":
    main()