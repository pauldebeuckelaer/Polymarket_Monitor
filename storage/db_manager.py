"""
Database Manager
================
SQLite storage for Polymarket snapshots and alerts.
"""

import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DBManager:

    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_time TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                question TEXT,
                condition_id TEXT,
                yes_prob REAL,
                best_bid REAL,
                best_ask REAL,
                spread REAL,
                volume_24h REAL,
                total_volume REAL,
                liquidity REAL,
                last_trade REAL,
                change_1d REAL,
                change_1w REAL,
                UNIQUE(snapshot_time, condition_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_time TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                event_slug TEXT,
                question TEXT,
                condition_id TEXT,
                prob_now REAL,
                prob_before REAL,
                volume_now REAL,
                details TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_time ON snapshots(snapshot_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_cond ON snapshots(condition_id, snapshot_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_event ON snapshots(event_slug, snapshot_time)")
        conn.commit()
        conn.close()
        logger.info(f"Database ready: {self.db_path}")

    def store_snapshot(self, markets):
        """Store a list of market dicts from the client."""
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(self.db_path)
        stored = 0

        for m in markets:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO snapshots
                    (snapshot_time, event_slug, question, condition_id,
                     yes_prob, best_bid, best_ask, spread,
                     volume_24h, total_volume, liquidity,
                     last_trade, change_1d, change_1w)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    now, m['event_slug'], m['question'], m['condition_id'],
                    m['yes_prob'], m['best_bid'], m['best_ask'], m['spread'],
                    m['volume_24h'], m['total_volume'], m['liquidity'],
                    m['last_trade'], m['change_1d'], m['change_1w']
                ))
                stored += 1
            except Exception as e:
                logger.error(f"Store error: {e}")

        conn.commit()
        conn.close()
        logger.info(f"Stored {stored} markets at {now}")
        return stored

    def get_latest_snapshot(self, condition_id):
        """Get the most recent snapshot for a market."""
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT * FROM snapshots WHERE condition_id = ? ORDER BY snapshot_time DESC LIMIT 1",
            (condition_id,)
        ).fetchone()
        conn.close()
        return row

    def get_prob_at_time(self, condition_id, snapshot_time, minutes_ago):
        """Get probability from X minutes ago relative to a snapshot time."""
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT yes_prob FROM snapshots "
            "WHERE condition_id = ? AND snapshot_time <= datetime(?, ? || ' minutes') "
            "ORDER BY snapshot_time DESC LIMIT 1",
            (condition_id, snapshot_time, str(-minutes_ago))
        ).fetchone()
        conn.close()
        return row[0] if row else None

    def get_snapshot_count(self):
        """Get total number of snapshots stored."""
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        conn.close()
        return count

    def cleanup_old_data(self, keep_full_hours=48):
        """
        Cleanup routine:
        1. Delete all snapshots from resolved markets (yes_prob > 99 or < 1)
        2. Downsample data older than keep_full_hours to ~1 per hour per market
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # ── Step 1: Delete resolved market snapshots ──
        cursor.execute("""
                    DELETE FROM snapshots
                    WHERE yes_prob > 99 OR yes_prob < 1
                """)

        resolved_deleted = cursor.rowcount
        logger.info(f"🗑  Deleted {resolved_deleted} snapshots from resolved markets")

        # ── Step 2: Downsample old data to hourly ──
        cutoff = f"-{keep_full_hours} hours"

        # Keep the snapshot closest to the top of each hour
        cursor.execute("""
            DELETE FROM snapshots
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           condition_id,
                           snapshot_time,
                           ROW_NUMBER() OVER (
                               PARTITION BY condition_id, strftime('%Y-%m-%d %H', snapshot_time)
                               ORDER BY snapshot_time ASC
                           ) as rn
                    FROM snapshots
                    WHERE snapshot_time < datetime('now', ?)
                )
                WHERE rn = 1
            )
            AND snapshot_time < datetime('now', ?)
        """, (cutoff, cutoff))
        downsampled_deleted = cursor.rowcount
        logger.info(f"📉 Downsampled: removed {downsampled_deleted} old high-frequency snapshots")

        conn.commit()

        # Reclaim disk space
        cursor.execute("VACUUM")
        conn.close()

        total = resolved_deleted + downsampled_deleted
        logger.info(f"✅ Cleanup done: {total} total rows removed")
        return resolved_deleted, downsampled_deleted