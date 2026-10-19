"""
context_snapshot.py — dated "what the crowd expects" snapshot, frozen before events.

Reads the latest collector cycle from data/polymarket.db, groups contracts into
agenda buckets by keyword, prints probabilities as the MID ((bid+ask)/2) — NOT
yes_prob, which is the bid and reads ~3.5pp low on average — and appends
HYPE/BTC prices from the TWAP analyzer's twap.db.

Writes notes/context/<UTC date>_<label>.md and prints the same to stdout.
Read-only on both databases.

Usage (on the box, from ~/bots/Polymarket_Monitor):
    python3 scripts/context_snapshot.py --label pre_nov3
    python3 scripts/context_snapshot.py --label pre_nov3 --top 40      # more legs per bucket
    python3 scripts/context_snapshot.py --label pre_nov3 --top 0       # everything
"""
import argparse
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone

POLY_DB = "data/polymarket.db"
TWAP_DB = os.path.expanduser("~/bots/Hyperliquid_TWAP_Analyzer/data/twap.db")

# First match wins, so order matters (Hormuz/ceasefire before generic oil, etc.)
BUCKETS = [
    ("Ceasefire / deal / Hormuz", r"ceasefire|peace|deal|agreement|hormuz|pezeshkian|al udeid|withdraw|negotiat|talks"),
    ("Red Sea / Houthis",          r"houthi|bab el|mandeb|red sea|yanbu"),
    ("Fed",                        r"\bfed\b|fed-|fomc|rate cut|rate hike|interest rate|powell"),
    ("CPI / inflation / yields",   r"\bcpi\b|cpi-|inflation|treasury|yield"),
    ("Oil / fuel",                 r"\bwti\b|wti-|crude|brent|\boil\b|oil-|diesel|gas price|gasoline"),
    ("Congress (midterms)",        r"senate|house|midterm|democrat|republican|congress"),
]

MID_MIN, MID_MAX = 0.015, 0.985   # drop near-resolved legs
WIDE_SPREAD = 0.05                 # flag legs whose spread > 5c


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)


def latest_rows(con, window_min):
    (mx,) = con.execute("SELECT MAX(snapshot_time) FROM snapshots").fetchone()
    if mx is None:
        raise SystemExit("snapshots table is empty")
    # Build the cutoff in the SAME string format as the stored values
    # (ISO 'T' separator + offset) so string comparison is valid.
    cutoff = (datetime.fromisoformat(mx) - timedelta(minutes=window_min)).isoformat()
    rows = con.execute(
        """
        SELECT s.snapshot_time, s.event_slug, s.question, s.condition_id,
               s.best_bid, s.best_ask, s.spread, s.liquidity, s.volume_24h, s.change_1w
        FROM snapshots s
        JOIN (SELECT condition_id, MAX(snapshot_time) AS mt
              FROM snapshots WHERE snapshot_time >= ?
              GROUP BY condition_id) m
          ON s.condition_id = m.condition_id AND s.snapshot_time = m.mt
        """,
        (cutoff,),
    ).fetchall()
    return mx, cutoff, rows


def bucket_of(event_slug, question):
    text = f"{event_slug} {question}".lower()
    for name, pat in BUCKETS:
        if re.search(pat, text):
            return name
    return None


def fmt_pct(x):
    return f"{x * 100:5.1f}%"


def hype_btc(path):
    """Latest close for HYPE and BTC from market_candles, with schema introspection."""
    if not os.path.exists(path):
        return [f"_twap.db not found at {path}_"]
    con = ro(path)
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(market_candles)")]
        if not cols:
            return ["_market_candles table not found in twap.db_"]
        low = {c.lower(): c for c in cols}
        coin = next((low[c] for c in ("coin", "symbol", "asset") if c in low), None)
        close = next((low[c] for c in ("close", "c", "close_px", "close_price") if c in low), None)
        tcol = next((low[c] for c in ("ts", "time", "timestamp", "open_time", "t", "candle_time") if c in low), None)
        if not (coin and close and tcol):
            return [f"_could not map market_candles columns: {cols}_"]
        out = []
        for sym in ("HYPE", "BTC"):
            r = con.execute(
                f"SELECT {tcol}, {close} FROM market_candles WHERE {coin}=? ORDER BY {tcol} DESC LIMIT 1",
                (sym,),
            ).fetchone()
            out.append(f"- **{sym}**: {r[1]} (candle {r[0]})" if r else f"- **{sym}**: no rows")
        return out
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="snapshot")
    ap.add_argument("--top", type=int, default=25, help="legs per bucket by liquidity; 0 = all")
    ap.add_argument("--window", type=int, default=30, help="minutes back from latest snapshot")
    ap.add_argument("--db", default=POLY_DB)
    ap.add_argument("--twap-db", default=TWAP_DB)
    ap.add_argument("--outdir", default="notes/context")
    args = ap.parse_args()

    con = ro(args.db)
    mx, cutoff, rows = latest_rows(con, args.window)
    con.close()

    groups = {name: [] for name, _ in BUCKETS}
    unmatched = dropped = 0
    for (t, ev, q, cid, bid, ask, spread, liq, vol24, ch1w) in rows:
        if bid is None or ask is None:
            dropped += 1
            continue
        mid = (bid + ask) / 2
        if not (MID_MIN <= mid <= MID_MAX):
            dropped += 1
            continue
        b = bucket_of(ev or "", q or "")
        if b is None:
            unmatched += 1
            continue
        groups[b].append(dict(ev=ev, q=q, mid=mid, spread=spread or 0.0,
                              liq=liq or 0.0, vol24=vol24 or 0.0, ch1w=ch1w))

    now = datetime.now(timezone.utc)
    lines = [
        f"# Context snapshot — {args.label}",
        "",
        f"- Generated: {now.isoformat(timespec='seconds')}",
        f"- Latest collector cycle: {mx}  (window from {cutoff})",
        f"- Contracts in window: {len(rows)} | shown buckets: {sum(len(v) for v in groups.values())} "
        f"| unmatched: {unmatched} | dropped (near-resolved / no book): {dropped}",
        "- Probability = mid (bid+ask)/2. `~` = spread > 5c, treat as soft.",
        "",
    ]
    for name, _ in BUCKETS:
        legs = groups[name]
        if not legs:
            lines += [f"## {name}", "", "_none in window_", ""]
            continue
        legs.sort(key=lambda d: d["liq"], reverse=True)
        shown = legs if args.top == 0 else legs[: args.top]
        shown.sort(key=lambda d: (d["ev"] or "", -d["mid"]))
        lines += [f"## {name}  ({len(shown)} of {len(legs)} legs, by liquidity)", "",
                  "| prob | spr | liq $ | vol24 $ | chg 1w | question |",
                  "|---:|---:|---:|---:|---:|---|"]
        for d in shown:
            flag = "~" if d["spread"] > WIDE_SPREAD else ""
            ch = "" if d["ch1w"] is None else f"{d['ch1w']:+.3f}"
            lines.append(f"| {flag}{fmt_pct(d['mid'])} | {d['spread']:.3f} | {d['liq']:,.0f} | "
                         f"{d['vol24']:,.0f} | {ch} | {d['q']} |")
        lines.append("")

    lines += ["## HYPE / BTC (twap.db market_candles)", ""] + hype_btc(args.twap_db) + [""]

    text = "\n".join(lines)
    print(text)
    os.makedirs(args.outdir, exist_ok=True)
    path = os.path.join(args.outdir, f"{now:%Y-%m-%d}_{args.label}.md")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    print(f"\n→ written to {path}")


if __name__ == "__main__":
    main()