"""
Telegram Alerts (Telethon)
==========================
Sends alerts via Telethon client to Saved Messages / admin user.
"""

import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone

from telethon import TelegramClient
import config

logger = logging.getLogger(__name__)

SESSION_PATH = str(Path(__file__).parent.parent / "sessions" / config.TELEGRAM_SESSION_NAME)


def _send_sync(text):
    """Synchronous wrapper around async Telethon send."""
    async def _send():
        if not config.TELEGRAM_API_ID or not config.TELEGRAM_ADMIN_USER_ID:
            logger.debug("Telegram not configured, skipping")
            return False

        try:
            client = TelegramClient(
                SESSION_PATH,
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH
            )
            await client.start(phone=config.TELEGRAM_PHONE)
            await client.send_message(config.TELEGRAM_ADMIN_USER_ID, text)
            await client.disconnect()
            logger.info("    📱 Telegram message sent")
            return True
        except Exception as e:
            logger.error(f"    Telegram failed: {e}")
            return False

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If already in an async context
            future = asyncio.ensure_future(_send())
            return future
        else:
            return asyncio.run(_send())
    except RuntimeError:
        return asyncio.run(_send())


def send_message(text):
    """Send a message to Telegram. Drop-in replacement."""
    return _send_sync(text)


def filter_alerts(alerts):
    """Apply volume floor and return top N movers."""
    filtered = [a for a in alerts if a["volume_24h"] >= config.ALERT_VOLUME_FLOOR]
    filtered.sort(key=lambda a: a["abs_delta"], reverse=True)
    return filtered[:config.ALERT_TOP_N]


def format_summary(top_alerts, theme_name, total_alerts, total_markets):
    """Format a single summary message for Telegram."""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")

    lines = [f"📊 [POLYMARKET] {theme_name} — {now}", ""]

    for a in top_alerts:
        q = a["question"]

        vol = a["volume_24h"]
        if vol >= 1_000_000:
            vol_str = f"${vol/1_000_000:.1f}M"
        elif vol >= 1_000:
            vol_str = f"${vol/1_000:.0f}K"
        else:
            vol_str = f"${vol:.0f}"

        lines.append(
            f"{a['direction']} {q}\n"
            f"   {a['prob_before']:.0f}% → {a['prob_now']:.0f}%  "
            f"(Δ {a['delta']:+.1f}%)  {vol_str} vol"
        )
        lines.append("")

    lines.append(f"📈 {total_alerts} moves > {config.ALERT_THRESHOLD_PCT}% | {total_markets} markets tracked")

    return "\n".join(lines)


def send_alerts(alerts, theme_name, total_markets):
    """Filter alerts and send a summary to Telegram if any qualify."""
    if not alerts:
        return 0

    top_alerts = filter_alerts(alerts)
    if not top_alerts:
        logger.info(f"    No alerts passed volume filter (${config.ALERT_VOLUME_FLOOR:,})")
        return 0

    summary = format_summary(top_alerts, theme_name, len(alerts), total_markets)
    send_message(summary)
    return len(top_alerts)