"""Explore Polymarket tags before adding a theme — run locally in PyCharm.

Read-only, public Gamma API, no key needed. Touches nothing on the box.

    python polymarket_explore.py                 # search tags for the default keywords
    python polymarket_explore.py fed oil         # search tags for your own keywords
    python polymarket_explore.py --tag midterms  # list the open events in one tag

For each matching tag it counts OPEN events (with paging, so no 100 cap)
and shows the top events by 24h volume. A tag that stays well under 100
open events is safe for the monitor's current one-call tag fetch.
"""
import sys
import requests

GAMMA = "https://gamma-api.polymarket.com"
DEFAULT_KEYWORDS = ["midterm", "election", "senate", "house", "fed", "fomc",
                    "inflation", "cpi", "economy", "recession", "oil", "crude",
                    "houthi", "yemen", "red sea", "saudi"]
PAGE = 100


def get(path, **params):
    r = requests.get(f"{GAMMA}{path}", params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def all_tags():
    tags, offset = [], 0
    while True:
        batch = get("/tags", limit=PAGE, offset=offset)
        if not batch:
            break
        tags.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
        if offset > 20_000:          # safety stop
            break
    return tags


def open_events(tag_slug):
    events, offset = [], 0
    while True:
        batch = get("/events", tag_slug=tag_slug, closed="false", limit=PAGE, offset=offset)
        if not batch:
            break
        events.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
        if offset > 5_000:           # safety stop
            break
    return events


def vol24(e):
    try:
        return float(e.get("volume24hr") or 0)
    except (TypeError, ValueError):
        return 0.0


def show_tag(tag_slug, top=10):
    ev = open_events(tag_slug)
    ev.sort(key=vol24, reverse=True)
    flag = "  <-- OVER 100: monitor would truncate" if len(ev) > 100 else ""
    print(f"\n[{tag_slug}]  {len(ev)} open events{flag}")
    for e in ev[:top]:
        print(f"   ${vol24(e):>12,.0f}  {e.get('slug','')}")
        print(f"   {'':13}  {str(e.get('title',''))[:90]}")
    return len(ev)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def show_event(slug):
    ev = get("/events", slug=slug)
    if not ev:
        print(f"\n[{slug}]  not found")
        return
    e = ev[0]
    print(f"\n[{slug}]  {e.get('title')}")
    print(f"   ends {str(e.get('endDate',''))[:10]}   closed={e.get('closed')}   "
          f"vol24 ${vol24(e):,.0f}   markets {len(e.get('markets', []))}")
    rows = []
    for m in e.get("markets", []):
        bid, ask = num(m.get("bestBid")), num(m.get("bestAsk"))
        mid = (bid + ask) / 2 if bid and ask else num(m.get("lastTradePrice"))
        rows.append((mid, m, bid, ask))
    rows.sort(key=lambda r: r[0], reverse=True)
    print(f"   {'mid%':>6} {'spr':>5} {'vol24':>10}  question")
    for mid, m, bid, ask in rows:
        closed = " (closed)" if m.get("closed") else ""
        print(f"   {mid*100:6.1f} {(ask-bid)*100:5.1f} {num(m.get('volume24hr')):10,.0f}  "
              f"{str(m.get('question',''))[:75]}{closed}")


def main():
    args = sys.argv[1:]
    if args[:1] == ["--event"]:
        for s in args[1:]:
            show_event(s)
        return
    if args[:1] == ["--tag"]:
        for t in args[1:]:
            show_tag(t, top=40)
        return

    keywords = [k.lower() for k in (args or DEFAULT_KEYWORDS)]
    tags = all_tags()
    print(f"{len(tags)} tags on Polymarket")
    hits = []
    for t in tags:
        label = str(t.get("label") or "").lower()
        slug = str(t.get("slug") or "").lower()
        if any(k in label or k in slug for k in keywords):
            hits.append(t)
    print(f"{len(hits)} tags match {keywords}")
    for t in sorted(hits, key=lambda t: str(t.get("slug"))):
        print(f"   {str(t.get('slug')):40}  {t.get('label')}")

    print("\nOpen-event counts (tags with 0 open events are skipped):")
    for t in sorted(hits, key=lambda t: str(t.get("slug"))):
        slug = t.get("slug")
        if not slug:
            continue
        n = len(open_events(slug))
        if n:
            flag = "  <-- OVER 100" if n > 100 else ""
            print(f"   {n:5d}  {slug}{flag}")

    print("\nNext: python polymarket_explore.py --tag <slug>  to see a tag's events.")


if __name__ == "__main__":
    main()