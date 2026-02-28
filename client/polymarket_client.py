"""
Polymarket API Client
=====================
Handles all communication with the Polymarket Gamma API.
"""

import requests
import logging

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"


class PolymarketClient:

    def __init__(self, timeout=15):
        self.base_url = GAMMA_API
        self.timeout = timeout

    def fetch_events_by_tag(self, tag_id, limit=50):
        """Fetch active events for a tag, sorted by 24h volume."""
        try:
            r = requests.get(
                f"{self.base_url}/events",
                params={
                    'tag_id': tag_id,
                    'closed': 'false',
                    'limit': limit,
                    'order': 'volume24hr',
                    'ascending': 'false',
                },
                timeout=self.timeout
            )
            r.raise_for_status()
            events = r.json()
            logger.info(f"Tag {tag_id}: fetched {len(events)} events")
            return events
        except Exception as e:
            logger.error(f"Failed to fetch tag {tag_id}: {e}")
            return []

    def fetch_event_by_slug(self, slug):
        """Fetch a specific event by slug."""
        try:
            r = requests.get(
                f"{self.base_url}/events",
                params={'slug': slug},
                timeout=self.timeout
            )
            r.raise_for_status()
            events = r.json()
            if events:
                logger.info(f"Fetched event: {slug}")
                return events[0]
            else:
                logger.warning(f"Event not found: {slug}")
                return None
        except Exception as e:
            logger.error(f"Failed to fetch {slug}: {e}")
            return None

    def fetch_events_by_tag_slug(self, tag_slug, limit=50):
        """Fetch active events filtered by Polymarket tag slug."""
        try:
            r = requests.get(
                f"{self.base_url}/events",
                params={
                    'tag_slug': tag_slug,
                    'closed': 'false',
                    'limit': limit,
                },
                timeout=self.timeout
            )
            r.raise_for_status()
            events = r.json()
            logger.info(f"Tag '{tag_slug}': fetched {len(events)} events")
            return events
        except Exception as e:
            logger.error(f"Failed to fetch tag '{tag_slug}': {e}")
            return []

    def extract_markets(self, event):
        """Extract open, tradeable markets from an event."""
        results = []
        event_slug = event.get('slug', '')
        event_title = event.get('title', '')

        for m in event.get('markets', []):
            if m.get('closed'):
                continue

            # Use bestBid as YES probability (most reliable)
            yes_prob = float(m.get('bestBid', 0) or 0) * 100
            best_bid = float(m.get('bestBid', 0) or 0)
            best_ask = float(m.get('bestAsk', 0) or 0)

            results.append({
                'event_slug': event_slug,
                'event_title': event_title,
                'question': m.get('question', ''),
                'condition_id': m.get('conditionId', ''),
                'yes_prob': yes_prob,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0,
                'volume_24h': float(m.get('volume24hr', 0) or 0),
                'total_volume': float(m.get('volume', 0) or 0),
                'liquidity': float(m.get('liquidity', 0) or 0),
                'last_trade': float(m.get('lastTradePrice', 0) or 0),
                'change_1d': float(m.get('oneDayPriceChange', 0) or 0),
                'change_1w': float(m.get('oneWeekPriceChange', 0) or 0),
            })

        return results