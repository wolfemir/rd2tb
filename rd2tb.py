#!/usr/bin/env python3

import os
import json
import time
import sqlite3
import requests
from datetime import datetime
from pathlib import Path
import threading
import glob

# ====================
# Config + Constants
# ====================
TORBOX_API_BASE = "https://api.torbox.app"
TORBOX_API_VERSION = "v1"
TORBOX_MAX_CONCURRENT = os.getenv("TORBOX_MAX_CONCURRENT", "3")
TORBOX_API_TOKEN = os.getenv("TORBOX_API_TOKEN")
ZURGDATA_FOLDER = os.getenv("ZURGDATA_FOLDER", "/zurgdata")
APP_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_NAME = os.path.join(APP_DATA_DIR, "torrents.db")
TABLE_NAME = "torrents"
STATUS_INTERVAL = 30  # Status update interval in seconds

# ANSI Color Codes
class Colors:
    HEADER = '\033[95m'      # Pink
    INFO = '\033[94m'        # Blue
    SUCCESS = '\033[92m'     # Green
    WARNING = '\033[93m'     # Yellow
    ERROR = '\033[91m'       # Red
    DEBUG = '\033[96m'       # Cyan
    API = '\033[95m'         # Pink
    RATE = '\033[93m'        # Yellow
    PROGRESS = '\033[94m'    # Blue
    ENDC = '\033[0m'         # Reset
    BOLD = '\033[1m'         # Bold
    UNDERLINE = '\033[4m'    # Underline

def log(message, level="INFO"):
    """Helper function to print logs with timestamp and colors"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Color mapping
    color = Colors.INFO  # Default color
    
    if "API Request" in message:
        color = Colors.API
        level = "API"
    elif "API Response" in message:
        color = Colors.API
        level = "API"
    elif "Rate limit" in message.lower():
        color = Colors.RATE
        level = "RATE"
    elif "Progress" in message or "Status" in message:
        color = Colors.PROGRESS
        level = "STATUS"
    elif "✓" in message:
        color = Colors.SUCCESS
        level = "SUCCESS"
    elif "✗" in message:
        color = Colors.ERROR
        level = "ERROR"
    else:
        color = {
            "INFO": Colors.INFO,
            "SUCCESS": Colors.SUCCESS,
            "WARNING": Colors.WARNING,
            "ERROR": Colors.ERROR,
            "DEBUG": Colors.DEBUG,
            "API": Colors.API,
            "RATE": Colors.RATE,
            "STATUS": Colors.PROGRESS
        }.get(level, Colors.INFO)
    
    # Format the log message
    log_prefix = f"[{timestamp}] [{level}]"
    
    # Special formatting for headers and separators
    if "===" in message:
        print(f"{Colors.HEADER}{Colors.BOLD}{log_prefix} {message}{Colors.ENDC}")
    else:
        # Highlight important numbers and percentages
        import re
        message = re.sub(r'(\d+/\d+)', f'{Colors.BOLD}\\1{color}', message)
        message = re.sub(r'(\d+%)', f'{Colors.BOLD}\\1{color}', message)
        
        print(f"{color}{log_prefix} {message}{Colors.ENDC}")

def format_time(seconds):
    """Format seconds into a human readable string"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        seconds = int(seconds % 60)
        return f"{minutes}m {seconds}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"

# Stats for periodic reporting
class Stats:
    def __init__(self):
        self.last_report_time = time.time()
        self.files_found = 0
        self.files_processed = 0
        self.cached_count = 0
        self.added_count = 0
        self.error_count = 0
        self.current_batch = 0
        self.total_batches = 0
        self.duplicates_skipped = 0
        self.new_files = 0
        self.existing_skipped = 0
        
    def report_if_needed(self):
        current_time = time.time()
        if current_time - self.last_report_time >= STATUS_INTERVAL:
            self.print_status()
            self.last_report_time = current_time
            
    def print_status(self):
        log("\n=== Status Update ===")
        log(f"Files Found: {self.files_found}")
        log(f"Files Processed: {self.files_processed}")
        log(f"Cached Files: {self.cached_count}")
        log(f"Added to TorBox: {self.added_count}")
        log(f"Errors: {self.error_count}")
        if self.total_batches > 0:
            log(f"Progress: Batch {self.current_batch}/{self.total_batches}")
        log("==================\n")

# Global stats object
stats = Stats()

# Rate Limits
GENERAL_RATE_LIMIT = 5  # per second
CREATE_TORRENT_HOUR_LIMIT = 60  # per hour
CREATE_TORRENT_MIN_LIMIT = 10   # per minute

# Ensure app data directory exists
os.makedirs(APP_DATA_DIR, exist_ok=True)

# Rate limiting state
last_general_request_time = 0
create_torrent_requests = []  # List of timestamps

def apply_rate_limit(request_type="general"):
    """
    Apply rate limiting based on request type.
    Returns True if request should proceed, False if it should wait.
    Also updates the rate limit state.
    """
    global last_general_request_time
    current_time = time.time()
    
    # General rate limit (5/sec)
    if current_time - last_general_request_time < 1/GENERAL_RATE_LIMIT:
        sleep_time = 1/GENERAL_RATE_LIMIT - (current_time - last_general_request_time)
        time.sleep(sleep_time)
    last_general_request_time = time.time()
    
    # Additional limits for create_torrent
    if request_type == "create_torrent":
        # Clean old timestamps
        current_time = time.time()
        create_torrent_requests[:] = [t for t in create_torrent_requests if current_time - t < 3600]  # Keep last hour
        
        # Check hour limit (60/hour)
        if len(create_torrent_requests) >= CREATE_TORRENT_HOUR_LIMIT:
            oldest = create_torrent_requests[0]
            if current_time - oldest < 3600:  # Less than an hour since oldest
                log(f"[Rate Limit] Hour limit reached. Waiting {format_time(3600 - (current_time - oldest))}s", "WARNING")
                return False
        
        # Check minute limit (10/min)
        last_minute_requests = len([t for t in create_torrent_requests if current_time - t < 60])
        if last_minute_requests >= CREATE_TORRENT_MIN_LIMIT:
            log(f"[Rate Limit] Minute limit reached. Waiting for next minute window", "WARNING")
            return False
        
        # Add current request
        create_torrent_requests.append(current_time)
    
    return True

def load_settings(filename):
    """Load settings from file or create with defaults if not exists"""
    # Get scan interval in minutes from env or use default 120 minutes
    scan_minutes = int(os.getenv("SCAN_INTERVAL_MINUTES", "120"))
    
    settings = {
        "folder": os.getenv("ZURGDATA_FOLDER", "/zurgdata"),
        "api_token": os.getenv("TORBOX_API_TOKEN", ""),
        "max_concurrent": int(os.getenv("TORBOX_MAX_CONCURRENT", "2")),
        "scan_interval_minutes": scan_minutes  # Store in minutes for readability
    }

    # Create settings file if it doesn't exist
    if not os.path.exists(filename):
        try:
            with open(filename, 'w') as f:
                json.dump(settings, f, indent=4)
            log(f"Created new settings file: {filename}")
        except Exception as e:
            log(f"Warning: Could not create settings file: {e}", "WARNING")
    
    # Load settings from file
    else:
        try:
            with open(filename, 'r') as f:
                file_settings = json.load(f)
                
                # Update with file settings if environment variables not set
                if not os.getenv("ZURGDATA_FOLDER"):
                    settings["folder"] = file_settings.get("folder", settings["folder"])
                if not os.getenv("TORBOX_API_TOKEN"):
                    settings["api_token"] = file_settings.get("api_token", settings["api_token"])
                if not os.getenv("TORBOX_MAX_CONCURRENT"):
                    settings["max_concurrent"] = file_settings.get("max_concurrent", settings["max_concurrent"])
                if not os.getenv("SCAN_INTERVAL_MINUTES"):
                    settings["scan_interval_minutes"] = file_settings.get("scan_interval_minutes", settings["scan_interval_minutes"])
                    
            # Update file with any missing settings
            if not all(k in file_settings for k in settings):
                with open(filename, 'w') as f:
                    json.dump(settings, f, indent=4)
                log(f"Updated settings file with new defaults")
        except Exception as e:
            log(f"Warning: Could not read settings file: {e}", "WARNING")

    return settings

# ==========================
# 2. Initialize SQLite DB
# ==========================
def init_db():
    """Initialize the SQLite database with proper schema"""
    os.makedirs(APP_DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
    table_exists = cur.fetchone() is not None
    
    if not table_exists:
        # Create new table with all columns
        log("Creating new database table...")
        cur.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                hash TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                magnet TEXT,
                is_cached INTEGER DEFAULT 0,
                is_added INTEGER DEFAULT 0,
                if_slow INTEGER DEFAULT 0,
                torbox_id TEXT,
                progress INTEGER DEFAULT 0,
                seeders INTEGER DEFAULT 0,
                state TEXT,
                last_checked TIMESTAMP,
                created_at TIMESTAMP,
                last_attempt TIMESTAMP,
                no_seeders_since TIMESTAMP,
                UNIQUE(hash)
            )
        """)
        # Set default timestamps for new table
        cur.execute(f"UPDATE {TABLE_NAME} SET last_checked = datetime('now'), created_at = datetime('now')")
    else:
        # Check and add missing columns
        log("Checking database schema...")
        cur.execute(f"PRAGMA table_info({TABLE_NAME})")
        columns = {col[1] for col in cur.fetchall()}
        
        # Add missing columns if needed
        if 'magnet' not in columns:
            log("Adding magnet column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN magnet TEXT")
            
        if 'progress' not in columns:
            log("Adding progress column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN progress INTEGER DEFAULT 0")
            
        if 'seeders' not in columns:
            log("Adding seeders column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN seeders INTEGER DEFAULT 0")
            
        if 'state' not in columns:
            log("Adding state column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN state TEXT")
            
        if 'last_checked' not in columns:
            log("Adding last_checked column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN last_checked TIMESTAMP")
            cur.execute(f"UPDATE {TABLE_NAME} SET last_checked = datetime('now')")
        
        if 'created_at' not in columns:
            log("Adding created_at column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN created_at TIMESTAMP")
            cur.execute(f"UPDATE {TABLE_NAME} SET created_at = datetime('now')")
        
        if 'torbox_id' not in columns:
            log("Adding torbox_id column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN torbox_id TEXT")
        
        if 'last_attempt' not in columns:
            log("Adding last_attempt column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN last_attempt TIMESTAMP")
        
        if 'no_seeders_since' not in columns:
            log("Adding no_seeders_since column...")
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN no_seeders_since TIMESTAMP")
    
    # Create index on hash for faster lookups
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_hash ON {TABLE_NAME}(hash)")
    
    return conn

def insert_or_update_torrent(conn, torrent_hash, name, magnet=None):
    """Insert or update a torrent in the database"""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (hash, name, magnet, created_at, last_checked)
            VALUES (?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(hash) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, name),
                magnet = COALESCE(EXCLUDED.magnet, magnet),
                last_checked = datetime('now')
        """, (torrent_hash, name, magnet))
        conn.commit()
    except sqlite3.Error as e:
        log(f"Database error: {e}", "ERROR")

def scan_zurg_files(root_folder, conn):
    """Scan for .zurginfo files recursively, skipping existing hashes"""
    log(f"\nScanning directory: {root_folder}")
    
    stats.files_found = 0
    stats.new_files = 0
    stats.duplicates_skipped = 0
    stats.existing_skipped = 0
    
    # Get existing hashes from database
    cur = conn.cursor()
    cur.execute(f"SELECT hash FROM {TABLE_NAME}")
    existing_hashes = {row[0] for row in cur.fetchall()}
    log(f"Found {len(existing_hashes)} existing hashes in database")
    
    found_hashes = set()  # Track unique hashes in this scan
    
    pattern = os.path.join(root_folder, "**/*.zurginfo")
    for filepath in glob.glob(pattern, recursive=True):
        try:
            stats.files_found += 1
            
            with open(filepath, 'r') as f:
                data = json.load(f)
                
            # Extract hash and filename
            torrent_hash = data.get('hash')
            if not torrent_hash:
                log(f"No hash found in {filepath}", "ERROR")
                continue
            
            # Skip if already in database
            if torrent_hash in existing_hashes:
                stats.existing_skipped += 1
                continue
                
            # Skip if we've seen this hash in current scan
            if torrent_hash in found_hashes:
                stats.duplicates_skipped += 1
                continue
                
            found_hashes.add(torrent_hash)
            stats.new_files += 1
            
            # Get name from filename field
            name = data.get('filename')
            if not name:
                name = os.path.splitext(os.path.basename(filepath))[0]
                
            magnet = f"magnet:?xt=urn:btih:{torrent_hash}"
            
            yield {
                'hash': torrent_hash,
                'name': name,
                'magnet': magnet,
                'filepath': filepath
            }
            
            if stats.new_files % 100 == 0:
                log(f"Found {stats.new_files} new files (scanned {stats.files_found} total)...")
                
        except json.JSONDecodeError:
            log(f"Invalid JSON in {filepath}", "ERROR")
        except Exception as e:
            log(f"Error processing {filepath}: {e}", "ERROR")
    
    # Update last scan time
    cur.execute("CREATE TABLE IF NOT EXISTS scan_info (last_scan TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cur.execute("DELETE FROM scan_info")
    cur.execute("INSERT INTO scan_info (last_scan) VALUES (CURRENT_TIMESTAMP)")
    conn.commit()

def process_uncached_torrents(api_token, max_concurrent):
    """Process torrents that aren't cached yet"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Initialize rate limit tracking
    minute_start = time.time()
    hour_start = time.time()
    minute_count = 0
    hour_count = 0
    
    try:
        while True:
            current_time = time.time()
            
            # Reset minute counter if a minute has passed
            if current_time - minute_start >= 60:
                minute_start = current_time
                minute_count = 0
            
            # Reset hour counter if an hour has passed
            if current_time - hour_start >= 3600:
                hour_start = current_time
                hour_count = 0
            
            # Check hour limit (60/hour)
            if hour_count >= 59:  # Leave room for error
                wait_time = 3600 - (current_time - hour_start)
                if wait_time > 0:
                    log(f"Hour rate limit reached. Waiting {format_time(wait_time)}...")
                    time.sleep(wait_time)
                    hour_start = time.time()
                    hour_count = 0
                    minute_start = time.time()
                    minute_count = 0
                continue
            
            # Check minute limit (10/min)
            if minute_count >= 9:  # Leave room for error
                wait_time = 60 - (current_time - minute_start)
                if wait_time > 0:
                    log(f"Minute rate limit reached. Waiting {format_time(wait_time)}...")
                    time.sleep(wait_time)
                    minute_start = time.time()
                    minute_count = 0
                continue
            
            # Get count of active downloads
            cur.execute(f"""
                SELECT COUNT(*) FROM {TABLE_NAME}
                WHERE is_added = 1 
                AND state IN ('downloading', 'queued')
            """)
            active_count = cur.fetchone()[0]
            
            # If we're at max concurrent, wait and check progress
            if active_count >= max_concurrent:
                log(f"Active download limit reached ({active_count}/{max_concurrent}). Waiting...")
                time.sleep(30)  # Wait longer before checking again
                continue
            
            # Get next uncached torrent
            cur.execute(f"""
                SELECT hash, name, magnet 
                FROM {TABLE_NAME}
                WHERE is_cached = 0 
                AND is_added = 0
                AND (
                    last_attempt IS NULL 
                    OR datetime('now', '-10 minutes') > last_attempt
                )
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                log("No more uncached torrents to process")
                break
                
            torrent_hash, name, magnet = row
            
            # Update last attempt time
            cur.execute(f"""
                UPDATE {TABLE_NAME}
                SET last_attempt = datetime('now')
                WHERE hash = ?
            """, (torrent_hash,))
            conn.commit()
            
            # Try to add to TorBox
            torbox_id = create_torrent_on_torbox(api_token, magnet_link=magnet, torrent_name=name)
            
            if torbox_id:
                # Successfully added
                cur.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET is_added = 1,
                        torbox_id = ?,
                        state = 'queued',
                        last_checked = datetime('now')
                    WHERE hash = ?
                """, (int(torbox_id), torrent_hash))
                conn.commit()
                log(f"{Colors.SUCCESS} Added torrent to TorBox: {name}")
                
                # Increment rate limit counters on success
                minute_count += 1
                hour_count += 1
                
                time.sleep(1)  # Small delay between successful adds
            else:
                # Check if we hit the active limit
                cur.execute(f"""
                    SELECT COUNT(*) FROM {TABLE_NAME}
                    WHERE is_added = 1 
                    AND state IN ('downloading', 'queued')
                """)
                current_active = cur.fetchone()[0]
                
                if current_active >= max_concurrent:
                    log(f"Active download limit reached ({current_active}/{max_concurrent}). Waiting...")
                    time.sleep(30)  # Wait longer when we hit the limit
                else:
                    # Some other error occurred
                    log(f"{Colors.ERROR} Failed to add torrent to TorBox: {name}", "ERROR")
                    time.sleep(5)  # Short delay on error
            
            # Print rate limit status every 10 adds
            if (minute_count + hour_count) % 10 == 0:
                log("\n=== Rate Limit Status ===")
                log(f"Hour: {hour_count}/60 (resets in {format_time(3600 - (time.time() - hour_start))})")
                log(f"Minute: {minute_count}/10 (resets in {format_time(60 - (time.time() - minute_start))})")
                log("========================\n")
            
    except Exception as e:
        log(f"Error processing uncached torrents: {e}", "ERROR")
    finally:
        conn.close()

def check_download_progress():
    """Monitor download progress and handle stuck/dead torrents"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    try:
        # Get all active downloads and seeding torrents
        cur.execute(f"""
            SELECT hash, torbox_id, name, progress, state,
                   strftime('%s', last_checked) as last_check_ts,
                   seeders
            FROM {TABLE_NAME}
            WHERE is_added = 1 
            AND torbox_id IS NOT NULL
            AND state IN ('downloading', 'queued')
        """)
        active_torrents = cur.fetchall()
        
        current_time = time.time()
        for torrent in active_torrents:
            hash, torbox_id, name, progress, state, last_check_ts, seeders = torrent
            
            # Skip if torrent was checked recently
            if last_check_ts and current_time - float(last_check_ts) < 60:
                continue
                
            # Check if torrent has no seeders for too long
            if seeders == 0 and state == 'downloading':
                # Get the time when seeders first dropped to 0
                cur.execute(f"""
                    SELECT strftime('%s', no_seeders_since)
                    FROM {TABLE_NAME}
                    WHERE hash = ?
                """, (hash,))
                no_seeders_since = cur.fetchone()[0]
                
                if no_seeders_since:
                    time_with_no_seeders = current_time - float(no_seeders_since)
                    if time_with_no_seeders > 120:  # 2 minutes
                        log(f"Removing torrent {name} (no seeders for 2+ minutes)")
                        
                        # Try to delete from TorBox
                        if control_torrent(TORBOX_API_TOKEN, torbox_id, "delete"):
                            # Update database
                            cur.execute(f"""
                                UPDATE {TABLE_NAME}
                                SET is_added = 0,
                                    torbox_id = NULL,
                                    state = NULL,
                                    progress = 0,
                                    no_seeders_since = NULL,
                                    last_checked = datetime('now')
                                WHERE hash = ?
                            """, (hash,))
                            conn.commit()
                            continue
                        else:
                            # If delete failed but it was a 422, consider it deleted
                            cur.execute(f"""
                                UPDATE {TABLE_NAME}
                                SET is_added = 0,
                                    torbox_id = NULL,
                                    state = NULL,
                                    progress = 0,
                                    no_seeders_since = NULL,
                                    last_checked = datetime('now')
                                WHERE hash = ?
                            """, (hash,))
                            conn.commit()
                            continue
                else:
                    # First time seeing no seeders, record the time
                    cur.execute(f"""
                        UPDATE {TABLE_NAME}
                        SET no_seeders_since = datetime('now')
                        WHERE hash = ?
                    """, (hash,))
                    conn.commit()
            elif seeders > 0:
                # Reset no_seeders_since if we have seeders
                cur.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET no_seeders_since = NULL
                    WHERE hash = ?
                """, (hash,))
                conn.commit()
            
            # Update last checked time
            cur.execute(f"""
                UPDATE {TABLE_NAME}
                SET last_checked = datetime('now')
                WHERE hash = ?
            """, (hash,))
            conn.commit()
            
    except Exception as e:
        log(f"Error checking download progress: {e}", "ERROR")
    finally:
        conn.close()

def add_cached_torrents(api_token):
    """Add all cached torrents to TorBox first"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    try:
        # Get all cached but not added torrents
        cur.execute(f"""
            SELECT hash, name, magnet 
            FROM {TABLE_NAME}
            WHERE is_cached = 1 AND is_added = 0
            ORDER BY created_at ASC
        """)
        cached_torrents = cur.fetchall()
        
        if not cached_torrents:
            log("No cached torrents to add")
            return
            
        total = len(cached_torrents)
        added = 0
        minute_start = time.time()
        hour_start = time.time()
        minute_count = 0
        hour_count = 0
        
        log(f"Found {total} cached torrents to add...")
        
        # Process one at a time
        for torrent_hash, name, magnet in cached_torrents:
            try:
                current_time = time.time()
                
                # Reset minute counter if a minute has passed
                if current_time - minute_start >= 60:
                    requests_this_minute = 0
                    minute_start = current_time
                
                # Reset hour counter if an hour has passed
                if current_time - hour_start >= 3600:
                    hour_start = current_time
                    hour_count = 0
                
                # Check hour limit (60/hour)
                if hour_count >= 59:  # Leave room for error
                    wait_time = 3600 - (current_time - hour_start)
                    if wait_time > 0:
                        log(f"Hour limit reached. Waiting {int(wait_time)}s...")
                        time.sleep(wait_time)
                        hour_start = time.time()
                        hour_count = 0
                        minute_start = time.time()
                        minute_count = 0
                
                # Check minute limit (10/min)
                if minute_count >= 9:  # Leave room for error
                    wait_time = 60 - (current_time - minute_start)
                    if wait_time > 0:
                        log(f"Minute limit reached. Waiting {int(wait_time)}s...")
                        time.sleep(wait_time)
                        minute_start = time.time()
                        minute_count = 0
                
                log(f"Adding cached torrent ({added + 1}/{total}): {name}")
                
                # If no magnet link, create one from hash
                if not magnet:
                    magnet = f"magnet:?xt=urn:btih:{torrent_hash}"
                
                # Create torrent on TorBox
                torbox_id = create_torrent_on_torbox(
                    api_token,
                    magnet_link=magnet,
                    torrent_name=name
                )
                
                if torbox_id:
                    # Update database
                    cur.execute(f"""
                        UPDATE {TABLE_NAME}
                        SET is_added = 1,
                            torbox_id = ?
                        WHERE hash = ?
                    """, (torbox_id, torrent_hash))
                    conn.commit()
                    added += 1
                    log(f"{Colors.SUCCESS} Successfully added to TorBox: {name}")
                    
                    # Increment counters
                    minute_count += 1
                    hour_count += 1
                else:
                    log(f"{Colors.ERROR} Failed to add to TorBox: {name}", "ERROR")
                
                # Small delay between adds
                time.sleep(0.5)
                
            except Exception as e:
                log(f"Error adding cached torrent {name}: {e}", "ERROR")
                continue
            
            # Save progress every 10 torrents
            if added % 10 == 0:
                log(f"\nProgress update:")
                log(f"- Added {added}/{total} torrents")
                log(f"- Current hour count: {hour_count}/60")
                log(f"- Current minute count: {minute_count}/10")
        
        log(f"\nFinished adding cached torrents:")
        log(f"Successfully added {added}/{total} torrents")
        
    except Exception as e:
        log(f"Error in add_cached_torrents: {e}", "ERROR")
    finally:
        conn.close()

def create_torrent_on_torbox(api_token, magnet_link=None, file_path=None, seed=0, allow_zip=False, torrent_name=None, as_queued=False):
    """Create a new torrent on TorBox by either magnet link or file upload (or both)."""
    
    if not magnet_link and not file_path:
        log("Error: Must provide either magnet link or file path", "ERROR")
        return None
        
    try:
        # Apply rate limiting
        if not apply_rate_limit("create_torrent"):
            return None
            
        # Prepare request
        url = f"{TORBOX_API_BASE}/{TORBOX_API_VERSION}/api/torrents/createtorrent"  
        headers = {"Authorization": f"Bearer {api_token}"}
        
        # Build form data
        data = {
            "seed": str(seed),  
            "allow_zip": str(allow_zip).lower(),  
            "as_queued": str(as_queued).lower()  
        }
        
        if magnet_link:
            data["magnet"] = magnet_link
        if torrent_name:
            data["name"] = torrent_name
            
        # Debug: Show request data
        log(f"API Request - URL: {url}")
        log(f"API Request - Data: {data}")
        
        # Make request
        response = requests.post(url, headers=headers, data=data, timeout=30)
        
        # Debug: Show response
        log(f"API Response - Status: {response.status_code}")
        log(f"API Response - Headers: {dict(response.headers)}")
        try:
            log(f"API Response - Body: {response.json()}")
        except:
            log(f"API Response - Body: {response.text}")
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', '60'))
            log(f"Rate limited, waiting {retry_after}s before retry...", "WARNING")
            time.sleep(retry_after)
            return None
            
        # Handle auth errors
        if response.status_code == 403:
            log("API token invalid or expired", "ERROR")
            return None
            
        # Handle server errors
        if response.status_code >= 500:
            log(f"Server error {response.status_code}", "ERROR")
            return None
            
        if response.status_code == 200:
            data = response.json()
            
            if data.get("success"):
                # Handle cached torrent response format
                if "Found Cached Torrent" in data.get("detail", ""):
                    torrent_id = data.get("data", {}).get("torrent_id")
                    if torrent_id:
                        return str(torrent_id)
                    else:
                        log("Cached torrent found but no ID in response", "ERROR")
                        return None
                        
                # Handle normal torrent creation response format
                elif data.get("data", {}).get("id"):
                    torrent_id = data["data"]["id"]
                    return str(torrent_id)
                    
                else:
                    log(f"Error: {data.get('detail', 'Unknown error')}", "ERROR")
                    return None
            else:
                error_msg = data.get('error')
                error_detail = data.get('detail')
                if error_detail and "already exists" in error_detail.lower():
                    # Try to extract torrent ID from error message
                    import re
                    match = re.search(r'ID: (\d+)', error_detail)
                    if match:
                        return str(match.group(1))
                log(f"Error: {error_detail or error_msg or 'Unknown error'}", "ERROR")
                return None
                
    except requests.exceptions.Timeout:
        log("Request timed out", "ERROR")
        return None
        
    except requests.exceptions.RequestException as e:
        log(f"Network error: {e}", "ERROR")
        return None
        
    except Exception as e:
        log(f"Unexpected error: {e}", "ERROR")
        return None
        
    return None

def get_torrent_info(api_token, torbox_id=None):
    """
    Get detailed information about torrents including seeders and progress.
    If torbox_id is provided, returns info for that specific torrent.
    Otherwise returns all torrents.
    """
    endpoint = f"{TORBOX_API_BASE}/{TORBOX_API_VERSION}/api/torrents/mylist"
    if torbox_id:
        endpoint = f"{endpoint}?id={torbox_id}"
    
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    
    try:
        resp = requests.get(endpoint, headers=headers, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        
        if not result.get("success", False):
            err = result.get("error") or "UNKNOWN_ERROR"
            detail = result.get("detail", "")
            log(f"Error getting torrent info: {err}, detail={detail}", "ERROR")
            return None
            
        data = result.get("data", [])
        if torbox_id:
            # If specific ID requested, return just that torrent's info
            for torrent in data:
                if torrent.get("id") == torbox_id:
                    return torrent
            return None
        
        return data
        
    except requests.exceptions.RequestException as e:
        log(f"HTTP error getting torrent info: {e}", "ERROR")
        return None
    except Exception as e:
        log(f"Unknown error getting torrent info: {e}", "ERROR")
        return None

def control_torrent(api_token, torbox_id, action):
    """
    Control a torrent's state. Valid actions:
    - pause: Pause the torrent
    - resume: Resume the torrent
    - reannounce: Force reannounce to trackers
    - delete: Delete the torrent
    """
    try:
        url = f"{TORBOX_API_BASE}/{TORBOX_API_VERSION}/api/torrents/controltorrent"
        headers = {"Authorization": f"Bearer {api_token}"}
        data = {
            "id": str(torbox_id),  # Ensure ID is string
            "action": action
        }
        
        # Debug logging
        log(f"Control Request - URL: {url}")
        log(f"Control Request - Data: {data}")
        
        response = requests.post(url, headers=headers, data=data, timeout=30)
        
        # Debug logging
        log(f"Control Response - Status: {response.status_code}")
        log(f"Control Response - Headers: {dict(response.headers)}")
        try:
            log(f"Control Response - Body: {response.json()}")
        except:
            log(f"Control Response - Body: {response.text}")
        
        if response.status_code == 422:
            # Torrent might already be deleted or not exist
            log(f"Torrent {torbox_id} not found or already deleted")
            return True  # Consider it a success since the torrent is gone
            
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', '60'))
            log(f"Rate limited, waiting {retry_after}s before retry...", "WARNING")
            time.sleep(retry_after)
            return False
            
        response.raise_for_status()
        return True
        
    except requests.exceptions.HTTPError as e:
        log(f"HTTP error controlling torrent: {e}", "ERROR")
        return False
    except requests.exceptions.RequestException as e:
        log(f"Network error controlling torrent: {e}", "ERROR")
        return False
    except Exception as e:
        log(f"Unexpected error controlling torrent: {e}", "ERROR")
        return False

def update_torrent_status():
    """Update status of all torrents in our database"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    try:
        # Get all torrents that have been added to TorBox
        cur.execute(f"""
            SELECT hash, torbox_id, name, state 
            FROM {TABLE_NAME}
            WHERE is_added = 1 
            AND torbox_id IS NOT NULL
        """)
        torrents = cur.fetchall()
        
        # Get status from TorBox API
        torrent_info = get_torrent_info(TORBOX_API_TOKEN)
        if not torrent_info:
            return
            
        # Create lookup by torbox_id
        torrent_status = {t["id"]: t for t in torrent_info}
        
        # Update each torrent's status
        for torrent_hash, torbox_id, name, old_state in torrents:
            status = torrent_status.get(torbox_id)
            if not status:
                continue
                
            progress = status.get("progress", 0)
            seeders = status.get("seeders", 0)
            state = status.get("state", "unknown")
            
            # Log state changes and completion
            if state != old_state:
                if state == "completed":
                    log(f"{Colors.SUCCESS} Download complete: {name}")
                elif state == "downloading":
                    log(f"{Colors.PROGRESS} Downloading {name}: {progress}%")
                elif state == "error":
                    log(f"{Colors.ERROR} Download error: {name}")
            elif state == "downloading" and progress % 10 == 0:  # Log every 10% progress
                log(f"{Colors.PROGRESS} Downloading {name}: {progress}%")
            
            cur.execute(f"""
                UPDATE {TABLE_NAME}
                SET progress = ?,
                    seeders = ?,
                    state = ?,
                    last_checked = datetime('now')
                WHERE hash = ?
            """, (progress, seeders, state, torrent_hash))
            
        conn.commit()
        
    except sqlite3.Error as e:
        log(f"Database error updating torrent status: {e}", "ERROR")
    finally:
        conn.close()

def check_all_cache_status(conn, api_token):
    """Check cache status for all non-added files"""
    log("\nChecking cache status for all non-added files...")
    
    cur = conn.cursor()
    cur.execute(f"""
        SELECT hash, name 
        FROM {TABLE_NAME}
        WHERE is_added = 0
    """)
    non_added = cur.fetchall()
    
    if not non_added:
        log("No non-added files to check")
        return
        
    total = len(non_added)
    log(f"Found {total} non-added files to check")
    
    cached_count = 0
    processed = 0
    consecutive_errors = 0
    last_request_time = 0
    requests_this_minute = 0
    minute_start_time = time.time()
    
    # Process in batches of 50 hashes
    batch_size = 50
    for i in range(0, len(non_added), batch_size):
        batch = non_added[i:i + batch_size]
        hash_list = ','.join(h[0] for h in batch)
        
        try:
            current_time = time.time()
            
            # Reset minute counter if a minute has passed
            if current_time - minute_start_time >= 60:
                requests_this_minute = 0
                minute_start_time = current_time
            
            # Check if we're within rate limits (5/sec and general limit)
            if current_time - last_request_time < 0.2:  # 5 requests per second = 1 request per 0.2 seconds
                sleep_time = 0.2 - (current_time - last_request_time)
                time.sleep(sleep_time)
            
            # Check if we need a longer delay due to rate limiting
            if consecutive_errors > 3:
                wait_time = min(30, 2 ** consecutive_errors)  # Exponential backoff up to 30s
                log(f"Too many errors, waiting {wait_time}s before retry...", "WARNING")
                time.sleep(wait_time)
            
            # Debug: Show what we're sending
            log(f"Checking batch of {len(batch)} hashes...")
            
            # Check batch of hashes
            response = requests.get(
                f"{TORBOX_API_BASE}/{TORBOX_API_VERSION}/api/torrents/checkcached",
                params={
                    "hash": hash_list,
                    "format": "object",
                    "list_files": "false"
                },
                headers={"Authorization": f"Bearer {api_token}"},
                timeout=30
            )
            
            last_request_time = time.time()
            requests_this_minute += 1
            
            # Debug: Show response headers
            log(f"Response status: {response.status_code}")
            log(f"Rate limits - Remaining: {response.headers.get('x-ratelimit-remaining', 'N/A')}, Reset: {response.headers.get('x-ratelimit-reset', 'N/A')}")
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', '60'))
                log(f"Rate limited, waiting {retry_after}s before retry...", "WARNING")
                time.sleep(retry_after)
                consecutive_errors += 1
                continue
                
            # Handle auth errors
            if response.status_code == 403:
                log("API token invalid or expired", "ERROR")
                return
                
            # Handle server errors
            if response.status_code >= 500:
                log(f"Server error {response.status_code}, retrying...", "ERROR")
                consecutive_errors += 1
                time.sleep(5 * consecutive_errors)  # Increasing backoff
                continue
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("success"):
                    consecutive_errors = 0  # Reset error counter on success
                    cached_results = data.get("data", {})
                    
                    # Update each hash in the batch
                    for torrent_hash, name in batch:
                        # If the hash exists in the response, it's cached
                        is_cached = torrent_hash in cached_results
                        
                        if is_cached:
                            cached_count += 1
                            log(f"{Colors.SUCCESS} Found cached: {name}")
                            cur.execute(f"""
                                UPDATE {TABLE_NAME}
                                SET is_cached = 1,
                                    last_checked = datetime('now')
                                WHERE hash = ?
                            """, (torrent_hash,))
                        else:
                            log(f"{Colors.ERROR} Not cached: {name}")
                            cur.execute(f"""
                                UPDATE {TABLE_NAME}
                                SET is_cached = 0,
                                    last_checked = datetime('now')
                                WHERE hash = ?
                            """, (torrent_hash,))
                    
                    # Commit every batch
                    conn.commit()
                    
                    processed += len(batch)
                    if processed % 100 == 0:
                        log(f"Processed {processed}/{total} files...")
                        log(f"Found {cached_count} cached torrents so far...")
                else:
                    error_msg = data.get('error')
                    error_detail = data.get('detail')
                    log(f"Error checking cache for batch: {error_msg} - {error_detail}", "ERROR")
                    consecutive_errors += 1
                    
        except requests.exceptions.Timeout:
            log("Request timed out, retrying...", "WARNING")
            consecutive_errors += 1
            time.sleep(5)
            continue
            
        except requests.exceptions.RequestException as e:
            log(f"Network error: {e}", "ERROR")
            consecutive_errors += 1
            time.sleep(5)
            continue
            
        except Exception as e:
            log(f"Unexpected error: {e}", "ERROR")
            consecutive_errors += 1
            time.sleep(5)
            continue
    
    # Final commit for any remaining changes
    try:
        conn.commit()
    except sqlite3.Error as e:
        log(f"Error committing final changes: {e}", "ERROR")
        
    log(f"\nFinished checking cache status:")
    log(f"Processed {processed}/{total} files")
    log(f"Found {cached_count} cached torrents")

def main():
    while True:  # Outer loop to prevent complete exit
        try:
            log("\n=== Starting TorBox Client ===")
            
            # Load settings
            settings = load_settings("settings.json")
            folder_path = settings.get("folder") or ZURGDATA_FOLDER
            api_token = settings.get("api_token") or TORBOX_API_TOKEN
            max_concurrent = int(settings.get("max_concurrent") or TORBOX_MAX_CONCURRENT)
            scan_minutes = int(settings.get("scan_interval_minutes") or 120)
            scan_interval = scan_minutes * 60  # Convert to seconds for sleep
            
            if not api_token:
                log("No API token provided. Please add it to settings.json", "ERROR")
                time.sleep(60)  # Wait before retry
                continue
            
            log("\nConfiguration:")
            log(f"- Zurg Data Folder: {folder_path}")
            log(f"- Max Concurrent: {max_concurrent}")
            log(f"- Scan Interval: {scan_minutes} minutes")
            log(f"- Database: {DB_NAME}")
            log(f"- API Base URL: {TORBOX_API_BASE}/{TORBOX_API_VERSION}")
            
            # Initialize database
            log("\nInitializing database...")
            conn = init_db()
            
            try:
                # Check cache status for all non-added files at startup
                check_all_cache_status(conn, api_token)
                
                # Start background threads for monitoring progress
                def progress_check_loop():
                    while True:
                        try:
                            check_download_progress()
                            time.sleep(STATUS_INTERVAL)
                        except Exception as e:
                            log(f"Error in progress check: {e}", "ERROR")
                            time.sleep(STATUS_INTERVAL)
                
                progress_thread = threading.Thread(target=progress_check_loop, daemon=True)
                progress_thread.start()
                
                # Main scan loop
                while True:
                    try:
                        # 1. Scan and parse files
                        log("\nScanning for new torrent files...")
                        new_items = False
                        for item in scan_zurg_files(folder_path, conn):
                            new_items = True
                            insert_or_update_torrent(conn, item["hash"], item["name"], item["magnet"])
                            
                        log(f"\nScan complete:")
                        log(f"- Total files scanned: {stats.files_found}")
                        log(f"- Already in database: {stats.existing_skipped}")
                        log(f"- New files added: {stats.new_files}")
                        log(f"- Duplicates skipped: {stats.duplicates_skipped}")
                        
                        if new_items:
                            # Check cache status for new items
                            check_all_cache_status(conn, api_token)
                        
                        # First, add all cached torrents to TorBox
                        log("\nAdding cached torrents to TorBox...")
                        cur = conn.cursor()
                        cur.execute(f"""
                            SELECT COUNT(*) FROM {TABLE_NAME}
                            WHERE is_cached = 1 AND is_added = 0
                        """)
                        cached_count = cur.fetchone()[0]
                        
                        if cached_count > 0:
                            log(f"Found {cached_count} cached torrents to add...")
                            add_cached_torrents(api_token)
                        
                        # Process uncached torrents while waiting for next scan
                        log("\nProcessing uncached torrents...")
                        start_time = time.time()
                        scan_seconds = scan_minutes * 60
                        
                        while True:
                            current_time = time.time()
                            elapsed_time = current_time - start_time
                            
                            # If scan interval has passed
                            if elapsed_time >= scan_seconds:
                                # Check for new cached torrents
                                cur.execute(f"""
                                    SELECT COUNT(*) FROM {TABLE_NAME}
                                    WHERE is_cached = 1 AND is_added = 0
                                """)
                                new_cached_count = cur.fetchone()[0]
                                
                                if new_cached_count > 0:
                                    log(f"\nFound {new_cached_count} new cached torrents")
                                    log("Finishing current uncached batch before processing them...")
                                
                                # Reset timer
                                start_time = current_time
                            
                            # Process a batch of uncached torrents
                            process_uncached_torrents(api_token, max_concurrent)
                            
                            # Check if we should process new cached torrents
                            cur.execute(f"""
                                SELECT COUNT(*) FROM {TABLE_NAME}
                                WHERE is_cached = 1 AND is_added = 0
                            """)
                            remaining_cached = cur.fetchone()[0]
                            
                            if remaining_cached > 0:
                                log(f"\nProcessing {remaining_cached} cached torrents...")
                                break  # Exit uncached processing to handle cached
                            
                            # Small delay to prevent hammering the database
                            time.sleep(1)
                        
                        log(f"\nWaiting {scan_minutes} minutes before next scan...")
                        time.sleep(scan_interval)
                        
                    except Exception as e:
                        log(f"Error in scan loop: {e}", "ERROR")
                        time.sleep(scan_interval)  # Still wait before retry on error
                        
            except Exception as e:
                log(f"Error in main process: {e}", "ERROR")
                time.sleep(60)  # Wait before complete restart
            finally:
                conn.close()
                
        except Exception as e:
            log(f"Critical error: {e}", "ERROR")
            time.sleep(60)  # Wait before complete restart
        
if __name__ == "__main__":
    main()
