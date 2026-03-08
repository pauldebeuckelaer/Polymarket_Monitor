"""
Polymarket Monitor — Global Configuration
==========================================
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────
DB_PATH = os.path.join("data", "polymarket.db")
THEMES_DIR = "themes"
LOG_DIR = "logs"

# ── Snapshot Loop ──────────────────────────────────────
SNAPSHOT_INTERVAL_MIN = 5          # default, themes can override
DISCOVERY_INTERVAL_MIN = 30        # search for new markets every 30 min

# ── Alert Thresholds (defaults, themes can override) ──
ALERT_THRESHOLD_PCT = 3.0          # probability move to trigger alert
ALERT_VOLUME_FLOOR = 10000         # minimum 24h volume to alert on
ALERT_TOP_N = 5                    # max movers per summary message

# —— Telegram ——————————————————————————————————————
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "polymarket_monitor")
TELEGRAM_ADMIN_USER_ID = int(os.getenv("TELEGRAM_ADMIN_USER_ID", "0"))
TELEGRAM_ENABLED = False          # Set True to re-enable Telegram alerts

# ── API ────────────────────────────────────────────────
API_TIMEOUT = 15
GAMMA_API = "https://gamma-api.polymarket.com"

# ── Cleanup ────────────────────────────────────────────
CLEANUP_INTERVAL_MIN = 1440        # run cleanup once per day (24h)
CLEANUP_KEEP_FULL_HOURS = 48       # keep 5-min resolution for last 48h