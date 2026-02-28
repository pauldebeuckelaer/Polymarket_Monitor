"""
Polymarket Monitor — Main Loop
===============================
Loads active themes, fetches markets, stores snapshots,
detects probability moves, and sends alerts.

Usage:
    python main.py
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import config
from client.polymarket_client import PolymarketClient
from storage.db_manager import DBManager
from alerts.telegram import send_message, send_alerts

# ── Logging ────────────────────────────────────────────
os.makedirs(config.LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(config.LOG_DIR, "monitor.log"),
            encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger(__name__)


# ── Theme Loading ──────────────────────────────────────

def load_themes():
    """Load all active theme configs from themes/ directory."""
    themes = []
    themes_dir = Path(config.THEMES_DIR)

    if not themes_dir.exists():
        logger.warning(f"Themes directory not found: {themes_dir}")
        return themes

    for f in sorted(themes_dir.glob("*.json")):
        try:
            with open(f) as fh:
                theme = json.load(fh)

            if not theme.get("active", False):
                logger.info(f"  ⏸  {theme.get('name', f.stem)} — inactive, skipping")
                continue

            theme.setdefault("alert_threshold_pct", config.ALERT_THRESHOLD_PCT)
            theme.setdefault("snapshot_interval_min", config.SNAPSHOT_INTERVAL_MIN)
            theme.setdefault("slugs", [])
            theme.setdefault("tag_slugs", [])
            theme["_file"] = str(f)
            themes.append(theme)

            logger.info(f"  ✅ {theme['name']} — {len(theme['slugs'])} slugs, {len(theme['tag_slugs'])} tag filters")

        except Exception as e:
            logger.error(f"  ❌ Failed to load {f.name}: {e}")

    return themes


# ── Market Fetching (Optimized) ────────────────────────

def fetch_theme_events(client, theme):
    """
    Fetch all events for a theme in minimal API calls.
    Returns dict: {slug: event_dict, ...}
    """
    events_by_slug = {}
    api_calls = 0

    # Bulk fetch via tag_slugs
    for tag_slug in theme.get("tag_slugs", []):
        tag_events = client.fetch_events_by_tag_slug(tag_slug, limit=100)
        api_calls += 1
        for event in tag_events:
            slug = event.get("slug", "")
            if slug and not event.get("closed", False):
                events_by_slug[slug] = event

    # Fetch only uncovered manual slugs
    covered_slugs = set(events_by_slug.keys())
    manual_slugs = [s for s in theme.get("slugs", []) if s not in covered_slugs]

    for slug in manual_slugs:
        event = client.fetch_event_by_slug(slug)
        api_calls += 1
        if event:
            events_by_slug[slug] = event

    logger.info(f"    {len(events_by_slug)} events fetched in {api_calls} API calls")
    return events_by_slug


# ── Market Discovery ───────────────────────────────────

def discover_new_slugs(events_by_slug, theme):
    """Check fetched events for slugs not yet in the theme config."""
    known_slugs = set(theme.get("slugs", []))
    return [
        {"slug": slug, "title": event.get("title", "")}
        for slug, event in events_by_slug.items()
        if slug not in known_slugs
    ]


def update_theme_file(theme, new_slugs):
    """Add discovered slugs to the theme JSON file."""
    filepath = theme.get("_file")
    if not filepath or not new_slugs:
        return

    try:
        with open(filepath) as fh:
            data = json.load(fh)

        data.setdefault("slugs", [])
        added = 0
        for slug in new_slugs:
            if slug not in data["slugs"]:
                data["slugs"].append(slug)
                added += 1

        if added > 0:
            with open(filepath, "w") as fh:
                json.dump(data, fh, indent=2)
            logger.info(f"    📝 Added {added} new slugs to {Path(filepath).name}")

    except Exception as e:
        logger.error(f"    Failed to update theme file: {e}")


# ── Delta Detection ────────────────────────────────────

def detect_moves(db, markets, theme):
    """
    Compare current snapshot with previous for each market.
    Returns list of alerts for markets that moved beyond threshold.
    """
    threshold = theme.get("alert_threshold_pct", config.ALERT_THRESHOLD_PCT)
    alerts = []

    for m in markets:
        cid = m["condition_id"]
        prev = db.get_latest_snapshot(cid)

        if prev is None:
            continue

        prev_prob = prev[5]
        curr_prob = m["yes_prob"]
        delta = curr_prob - prev_prob

        if abs(delta) >= threshold:
            direction = "🟢" if delta > 0 else "🔴"
            alerts.append({
                "question": m["question"],
                "event_slug": m["event_slug"],
                "condition_id": cid,
                "prob_before": prev_prob,
                "prob_now": curr_prob,
                "delta": delta,
                "abs_delta": abs(delta),
                "direction": direction,
                "volume_24h": m["volume_24h"],
            })

    return alerts


# ── Log Formatting ─────────────────────────────────────

def log_alert(alert, theme_name):
    """Log individual alert to file (verbose)."""
    logger.info(
        f"\n{'='*50}\n"
        f"⚡ POLYMARKET MOVE — {theme_name}\n"
        f"{'='*50}\n"
        f"Market:  {alert['question']}\n"
        f"Move:    {alert['direction']}  {alert['prob_before']:.1f}% → {alert['prob_now']:.1f}%  (Δ {alert['delta']:+.1f}%)\n"
        f"Volume:  ${alert['volume_24h']:,.0f} (24h)\n"
        f"Slug:    {alert['event_slug']}\n"
        f"{'='*50}"
    )


# ── Main Cycle ─────────────────────────────────────────

def run_cycle(client, db, themes, do_discovery):
    """
    One full cycle: fetch events, extract markets, detect moves, store, alert.
    """
    total_markets = 0
    total_alerts = 0

    for theme in themes:
        theme_name = theme["name"]
        logger.info(f"  📡 {theme_name}")

        # Single fetch for both discovery + snapshot
        events_by_slug = fetch_theme_events(client, theme)

        if not events_by_slug:
            logger.info(f"    No events found")
            continue

        # Discovery
        if do_discovery:
            discovered = discover_new_slugs(events_by_slug, theme)
            if discovered:
                logger.info(f"    🆕 Discovered {len(discovered)} new markets:")
                new_slugs = []
                for d in discovered:
                    logger.info(f"       → {d['slug']}  ({d['title']})")
                    new_slugs.append(d["slug"])
                update_theme_file(theme, new_slugs)
                theme["slugs"] = list(set(theme.get("slugs", []) + new_slugs))
            else:
                logger.info(f"    No new markets discovered")

        # Extract markets
        all_markets = []
        for slug, event in events_by_slug.items():
            markets = client.extract_markets(event)
            all_markets.extend(markets)

        if not all_markets:
            logger.info(f"    No open markets found")
            continue

        # Detect moves BEFORE storing
        alerts = detect_moves(db, all_markets, theme)

        # Store snapshot
        stored = db.store_snapshot(all_markets)
        total_markets += stored

        # Log ALL alerts to file
        for alert in alerts:
            log_alert(alert, theme_name)

        # Telegram: filtered summary only
        sent = send_alerts(alerts, theme_name, stored)

        total_alerts += len(alerts)
        logger.info(f"    Stored {stored} markets | {len(alerts)} alerts | {sent} sent to Telegram")

    return total_markets, total_alerts


# ── Main Loop ──────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  POLYMARKET MONITOR — Starting")
    logger.info("=" * 60)

    # Init
    os.makedirs("data", exist_ok=True)
    os.makedirs(config.THEMES_DIR, exist_ok=True)

    client = PolymarketClient(timeout=config.API_TIMEOUT)
    db = DBManager(config.DB_PATH)

    # Verify Telegram
    if config.TELEGRAM_API_ID and config.TELEGRAM_ADMIN_USER_ID:
        logger.info("📱 Telegram configured (Telethon)")
        send_message("🟢 [POLYMARKET] Monitor started")
    else:
        logger.warning("⚠ Telegram not configured — alerts will be log-only")

    # Load themes
    logger.info("Loading themes...")
    themes = load_themes()

    if not themes:
        logger.error("No active themes found! Create a JSON file in themes/")
        sys.exit(1)

    logger.info(f"Loaded {len(themes)} active theme(s)")
    logger.info(f"Snapshot interval: {config.SNAPSHOT_INTERVAL_MIN} min")
    logger.info(f"Discovery interval: {config.DISCOVERY_INTERVAL_MIN} min")
    logger.info(f"Alert filter: vol >= ${config.ALERT_VOLUME_FLOOR:,} | top {config.ALERT_TOP_N} movers")
    logger.info("")

    cycle_count = 0
    last_discovery = 0

    try:
        while True:
            cycle_count += 1
            now = datetime.now(timezone.utc)
            logger.info(f"{'─'*50}")
            logger.info(f"📊 Cycle #{cycle_count} — {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

            minutes_since_discovery = (time.time() - last_discovery) / 60
            do_discovery = minutes_since_discovery >= config.DISCOVERY_INTERVAL_MIN

            if do_discovery:
                logger.info("  🔍 Discovery cycle (checking for new markets)")
                last_discovery = time.time()

            total_markets, total_alerts = run_cycle(client, db, themes, do_discovery)

            logger.info(f"  ✅ Cycle done: {total_markets} markets, {total_alerts} alerts")
            logger.info(f"  💾 DB total: {db.get_snapshot_count()} snapshots")
            logger.info(f"  💤 Next cycle in {config.SNAPSHOT_INTERVAL_MIN} min...")
            time.sleep(config.SNAPSHOT_INTERVAL_MIN * 60)

    except KeyboardInterrupt:
        logger.info("\n🛑 Monitor stopped by user")
        send_message("🔴 [POLYMARKET] Monitor stopped")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        send_message(f"💥 [POLYMARKET] Monitor crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()