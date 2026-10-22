#!/usr/bin/env python3
"""Polymarket movers: change in mid price between now and N hours ago.

Uses the corrected method (Oct 20 2026): both ends must be liquid.
  - each end averages the mid over a 1-hour window, not a single snapshot
  - only snapshots with spread <= --max-spread count
  - each end needs at least --min-snaps clean snapshots
This avoids fake moves from empty order books on one side.

Usage (from the repo root on the box):
  python3 scripts/polymarket_movers.py                      # top 25 movers, last 24h
  python3 scripts/polymarket_movers.py --like Iran --like Hormuz
  python3 scripts/polymarket_movers.py --hours 6 --limit 10
  python3 scripts/polymarket_movers.py --like UK --like nflation --all-terms
"""
import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "polymarket.db"

SQL = """
WITH now_w AS (
  SELECT condition_id, MAX(question) AS question,
         AVG((best_bid + best_ask) / 2) AS mid, COUNT(*) AS n,
         MAX(volume_24h) AS vol
  FROM snapshots
  WHERE snapshot_time >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-1 hours')
    AND spread <= :max_spread
  GROUP BY condition_id HAVING n >= :min_snaps),
then_w AS (
  SELECT condition_id, AVG((best_bid + best_ask) / 2) AS mid, COUNT(*) AS n
  FROM snapshots
  WHERE snapshot_time >= strftime('%Y-%m-%dT%H:%M:%S', 'now', :start)
    AND snapshot_time <  strftime('%Y-%m-%dT%H:%M:%S', 'now', :end)
    AND spread <= :max_spread
  GROUP BY condition_id HAVING n >= :min_snaps)
SELECT n.question, t.mid, n.mid, n.mid - t.mid AS d, n.vol
FROM now_w n JOIN then_w t USING (condition_id)
{where}
ORDER BY ABS(n.mid - t.mid) DESC
LIMIT :limit
"""


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--hours", type=int, default=24, help="compare now with N hours ago (default 24)")
    p.add_argument("--like", action="append", default=[],
                   help="keyword in the question (repeatable; matches ANY unless --all-terms)")
    p.add_argument("--all-terms", action="store_true", help="question must contain ALL --like terms")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--max-spread", type=float, default=0.05)
    p.add_argument("--min-snaps", type=int, default=2)
    a = p.parse_args()

    params = {
        "max_spread": a.max_spread,
        "min_snaps": a.min_snaps,
        "start": f"-{a.hours + 1} hours",
        "end": f"-{a.hours} hours",
        "limit": a.limit,
    }
    where = ""
    if a.like:
        joiner = " AND " if a.all_terms else " OR "
        clauses = []
        for i, term in enumerate(a.like):
            params[f"k{i}"] = f"%{term}%"
            clauses.append(f"n.question LIKE :k{i}")
        where = "WHERE " + joiner.join(clauses)

    con = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    rows = con.execute(SQL.format(where=where), params).fetchall()
    con.close()

    if not rows:
        print("No contracts with clean data on both ends.")
        return
    print(f"{'then':>6} {'now':>6} {'d_pp':>6} {'vol24h':>10}  question  (vs {a.hours}h ago)")
    for q, then_mid, now_mid, d, vol in rows:
        vol_s = f"{vol:,.0f}" if vol is not None else "-"
        print(f"{then_mid*100:6.1f} {now_mid*100:6.1f} {d*100:+6.1f} {vol_s:>10}  {q[:90]}")


if __name__ == "__main__":
    main()