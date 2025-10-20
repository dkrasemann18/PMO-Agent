"""
# Simple directory watcher connector that posts transcript files to the PMO Agent webhook
# This connector is intentionally dependency-light and uses polling so it can run in constrained environments.

import os
import time
import requests
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pmotool.connector")

DEFAULT_POLL_INTERVAL = 5

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def watch_and_post(directory: str, webhook_url: str, stage: bool = True, poll_interval: int = DEFAULT_POLL_INTERVAL, processed_dir_name: str = "processed"):
    """
    Watch `directory` for new .txt files. When a new file appears, read it as a meeting transcript
    and POST JSON to the `webhook_url` in the same shape as /webhook/transcript expects.

    The file name (without extension) is used as meeting_id. Files are moved into a `processed` subdirectory
    after successful POST to avoid reprocessing.

    Note: This is a very simple connector intended as an example. For production use consider using
    a file-watching library (watchdog) or integrate directly with your teleconferencing/ASR provider.
    """
    directory = os.path.abspath(directory)
    processed_dir = os.path.join(directory, processed_dir_name)
    ensure_dir(directory)
    ensure_dir(processed_dir)

    seen = set()
    logger.info("Starting watcher: dir=%s webhook=%s stage=%s interval=%s", directory, webhook_url, stage, poll_interval)

    try:
        while True:
            try:
                files = [f for f in os.listdir(directory) if f.lower().endswith('.txt')]
            except FileNotFoundError:
                logger.warning("Directory not found, creating: %s", directory)
                ensure_dir(directory)
                files = []

            for fn in sorted(files):
                full = os.path.join(directory, fn)
                # Skip files already in processed dir (safety)
                try:
                    if os.path.commonpath([full, processed_dir]) == processed_dir:
                        continue
                except Exception:
                    # ignore commonpath issues on some platforms
                    pass
                if full in seen:
                    continue
                # Basic sanity: ensure file is stable (size unchanged for a short period)
                try:
                    size1 = os.path.getsize(full)
                    time.sleep(0.2)
                    size2 = os.path.getsize(full)
                    if size1 != size2:
                        # file still being written
                        continue
                except Exception:
                    continue

                logger.info("Found transcript file: %s", full)
                try:
                    with open(full, 'r', encoding='utf-8') as fh:
                        transcript = fh.read()
                except Exception as e:
                    logger.exception("Failed to read file %s: %s", full, e)
                    seen.add(full)
                    continue

                meeting_id = os.path.splitext(fn)[0]
                payload = {
                    "meeting_id": meeting_id,
                    "title": meeting_id,
                    "transcript": transcript,
                    "attendees": [],
                    "stage": bool(stage)
                }
                headers = {"Content-Type": "application/json"}
                try:
                    resp = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
                    if 200 <= resp.status_code < 300:
                        logger.info("Posted transcript for %s -> %s (status=%s)", meeting_id, webhook_url, resp.status_code)
                        # move file to processed dir with timestamp to avoid clobbering
                        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
                        dest = os.path.join(processed_dir, f"{meeting_id}.{ts}.txt")
                        try:
                            os.rename(full, dest)
                            logger.info("Moved processed file to %s", dest)
                        except Exception:
                            logger.exception("Failed to move processed file %s", full)
                    else:
                        logger.warning("Webhook returned non-2xx for %s: %s %s", meeting_id, resp.status_code, resp.text)
                        # don't mark as seen so we retry later
                        continue
                except Exception as e:
                    logger.exception("Failed to POST transcript for %s: %s", meeting_id, e)
                    # don't mark as seen so retry later
                    continue

                seen.add(full)

            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='pmotool-connector')
    parser.add_argument('--dir', '-d', default='transcripts', help='Directory to watch for transcript .txt files')
    parser.add_argument('--webhook', '-w', default='http://localhost:8080/webhook/transcript', help='PMO Agent webhook URL')
    parser.add_argument('--no-stage', action='store_true', help='Disable staging; will persist actions directly')
    parser.add_argument('--interval', type=int, default=5, help='Poll interval in seconds')
    args = parser.parse_args()
    watch_and_post(args.dir, args.webhook, stage=not args.no_stage, poll_interval=args.interval)
"""