#!/usr/bin/env python3
"""Read-only queue diagnostics; stdout contains aggregates, never job contents or keys."""

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import sqlite3

UTC = timezone.utc
TAIPEI = timezone(timedelta(hours=8))
STAGES = {
    "queue": ("created", "started"),
    "prepare": ("started", "upstream_started"),
    "headers": ("upstream_started", "upstream_headers"),
    "receive": ("upstream_headers", "upstream_completed"),
    "finish": ("upstream_completed", "completed"),
    "execution": ("started", "completed"),
    "total": ("created", "completed"),
}

# Ranking stays inside SQLite: neither member keys nor account IDs leave the query.
# Error text is classified here, never selected or included in the output.
ROWS_SQL = """
SELECT created_at AS created,
  json_extract(data, '$.startedAt') AS started,
  json_extract(data, '$.completedAt') AS completed,
  json_extract(data, '$.upstreamStartedAt') AS upstream_started,
  json_extract(data, '$.upstreamHeadersAt') AS upstream_headers,
  json_extract(data, '$.upstreamCompletedAt') AS upstream_completed,
  json_extract(data, '$.deadlineAt') AS deadline,
  json_extract(data, '$.upstreamHttpStatus') AS http_status,
  COALESCE(json_extract(data, '$.retryCount'), 0) AS retries,
  status, model, source, stats_excluded,
  CASE WHEN user_token IS NULL OR user_token = '' THEN 0
    ELSE DENSE_RANK() OVER (ORDER BY user_token) END AS member,
  CASE WHEN account_id IS NULL OR account_id = '' THEN 0
    ELSE DENSE_RANK() OVER (ORDER BY account_id) END AS account,
  CASE
    WHEN status IN ('cancelled', 'canceled') THEN 'cancelled'
    WHEN status != 'failed' THEN 'none'
    WHEN LOWER(COALESCE(json_extract(data, '$.errorDetail'), '') || ' ' ||
      COALESCE(json_extract(data, '$.error'), '')) LIKE '%cancel%' THEN 'cancel_text'
    WHEN LOWER(COALESCE(json_extract(data, '$.errorDetail'), '') || ' ' ||
      COALESCE(json_extract(data, '$.error'), '')) LIKE '%timeout%'
      OR LOWER(COALESCE(json_extract(data, '$.errorDetail'), '')) LIKE '%timed out%'
      OR LOWER(COALESCE(json_extract(data, '$.errorDetail'), '')) LIKE '%deadline%'
      OR COALESCE(json_extract(data, '$.error'), '') LIKE '%超时%' THEN 'timeout_text'
    WHEN json_extract(data, '$.upstreamHttpStatus') = 429
      OR json_extract(data, '$.errorStatus') = 429 THEN 'http_429'
    ELSE 'other_failure' END AS error_kind
FROM jobs WHERE created_at >= ? AND created_at < ?
ORDER BY created_at
"""


def timestamp(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(UTC) if result.tzinfo else None
    except (ValueError, TypeError):
        return None


def iso(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def distribution(values, population=None):
    known = sorted(value for value in values if value is not None)
    total = len(values) if population is None else population
    # Nearest-rank percentiles avoid implying interpolation accuracy in small groups.
    percentile = lambda q: round(known[math.ceil(len(known) * q) - 1], 3) if known else None
    return {"n": len(known), "missing": total - len(known),
            "missing_pct": round(100 * (total - len(known)) / total, 2) if total else None,
            "p50_s": percentile(.50), "p95_s": percentile(.95),
            "max_s": round(known[-1], 3) if known else None}


def elapsed(row, start, end):
    a, b = row.get(start), row.get(end)
    return (b - a).total_seconds() if a is not None and b is not None and b >= a else None


def summarize(rows, as_of, full=True):
    result = {"n": len(rows), "states": dict(Counter(r["status"] for r in rows)),
              "queue": distribution([elapsed(r, "created", "started") for r in rows])}
    if not full:
        return result
    result["stages"] = {name: distribution([elapsed(r, *fields) for r in rows])
                        for name, fields in STAGES.items()}
    result["negative_stage_counts"] = {
        name: sum(r.get(a) is not None and r.get(b) is not None and r[b] < r[a] for r in rows)
        for name, (a, b) in STAGES.items()}
    result["errors"] = dict(Counter(r["error_kind"] for r in rows if r["error_kind"] != "none"))
    result["http_statuses"] = dict(Counter(str(r["http_status"] or "unknown") for r in rows))
    result["stats_excluded"] = sum(bool(r["stats_excluded"]) for r in rows)
    result["retry_jobs"] = sum(r["retries"] > 0 for r in rows)
    result["without_start"] = dict(Counter(r["status"] for r in rows if r["started"] is None))
    result["queued_age"] = distribution([(as_of - r["created"]).total_seconds()
                                          for r in rows if r["status"] == "queued"])
    result["running_age"] = distribution([(as_of - r["started"]).total_seconds()
                                           if r["started"] else None
                                           for r in rows if r["status"] == "running"])
    waits = [elapsed(r, "created", "started") for r in rows]
    result["wait_over_s"] = {str(t): sum(v is not None and v > t for v in waits)
                             for t in (10, 30, 60, 120, 300)}
    result["deadline_passed"] = sum(r["deadline"] is not None and r["completed"] is not None
                                     and r["completed"] > r["deadline"] for r in rows)
    return result


def idle_measurements(rows):
    previous = {}
    measurements = []
    for row in sorted((r for r in rows if r["started"]), key=lambda r: r["started"]):
        prior = previous.get(row["account"])
        if prior and row["account"] and prior["completed"] and prior["retries"] == 0 and row["retries"] == 0:
            if row["started"] >= prior["completed"]:
                ready = max(row["created"], prior["completed"] + timedelta(seconds=10))
                measurements.append({"created": row["created"],
                    "backlogged": row["created"] <= prior["completed"],
                    "idle_s": max(0, (row["started"] - ready).total_seconds()),
                    "gap_s": (row["started"] - prior["completed"]).total_seconds(),
                    "prior_failed": prior["status"] == "failed"})
        previous[row["account"]] = row
    return measurements


def queue_peaks(rows, since, as_of):
    events = []
    unknown = 0
    for row in rows:
        end = row["started"] or (as_of if row["status"] == "queued" else row["completed"])
        if end is None or row["retries"] or end < row["created"]:
            unknown += 1
            continue
        start, end = max(since, row["created"]), min(as_of, end)
        if start < end:
            events.extend(((start, 1, row["member"]), (end, -1, row["member"])))
    current, peak, per_member_peak = 0, 0, 0
    members = Counter()
    for _, change, member in sorted(events):
        current += change
        members[member] += change
        peak, per_member_peak = max(peak, current), max(per_member_peak, members[member])
    return {"global": peak, "single_member": per_member_peak, "unknown_intervals": unknown}


def analyze(rows, as_of):
    for row in rows:
        for name in ("created", "started", "completed", "upstream_started",
                     "upstream_headers", "upstream_completed", "deadline"):
            row[name] = timestamp(row[name])
        # Unknown free-form labels must not accidentally become an output channel.
        row["model"] = row["model"] if re.fullmatch(r"nai-diffusion-[a-z0-9-]+", row["model"] or "") else "other"
        row["source"] = row["source"] if row["source"] in ("web", "novelai-native", "openai", "direct") else "other"
        row["status"] = row["status"] if row["status"] in ("done", "failed", "queued", "running", "cancelled", "canceled") else "other"
    invalid_created = sum(r["created"] is None for r in rows)
    rows = [r for r in rows if r["created"] is not None]
    idle = idle_measurements(rows)
    output = {"as_of_utc": iso(as_of), "timezone": "Asia/Shanghai",
              "percentile": "nearest-rank", "invalid_created_in_selected_rows": invalid_created, "windows": {}}
    for days, label in ((1, "24h"), (7, "7d")):
        since = as_of - timedelta(days=days)
        selected = [r for r in rows if since <= r["created"] < as_of]
        summary = summarize(selected, as_of)
        summary["since_utc"] = iso(since)
        summary["observed_queue_peak"] = queue_peaks(rows, since, as_of)
        summary["latest_created_utc"] = iso(max(r["created"] for r in selected)) if selected else None
        for dimension in ("model", "source", "member", "hour"):
            groups = defaultdict(list)
            for row in selected:
                key = row["created"].astimezone(TAIPEI).strftime("%Y-%m-%d %H:00") if dimension == "hour" else row[dimension]
                if dimension == "member":
                    key = f"M{key:02d}" if key else "unknown"
                groups[key].append(row)
            summary["by_" + dimension] = {str(k): summarize(v, as_of, full=False) for k, v in sorted(groups.items())}
        measures = [r for r in idle if since <= r["created"] < as_of]
        started_count = sum(r["started"] is not None for r in selected)
        summary["extra_idle_after_10s"] = {
            "all_pairs": distribution([r["idle_s"] for r in measures], started_count),
            "backlogged_pairs": distribution([r["idle_s"] for r in measures if r["backlogged"]]),
            "pairs_after_failure": sum(r["prior_failed"] for r in measures),
            "over_1s": sum(r["idle_s"] > 1 for r in measures),
            "over_5s": sum(r["idle_s"] > 5 for r in measures),
            "completion_to_start_gap": distribution([r["gap_s"] for r in measures if r["backlogged"]])}
        output["windows"][label] = summary
    return output


def read_database(path, as_of):
    # mode=ro refuses creation/writes; query_only also rejects mutations on this connection.
    uri = path.resolve(strict=True).as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        coverage = dict(connection.execute("""SELECT COUNT(*) AS rows, MIN(created_at) AS first_created,
          MAX(created_at) AS last_created, SUM(NOT json_valid(data)) AS invalid_json FROM jobs""").fetchone())
        if coverage["invalid_json"]:
            raise ValueError("Invalid job JSON prevents reliable extraction; no raw rows were printed.")
        rows = [dict(r) for r in connection.execute(ROWS_SQL, (iso(as_of - timedelta(days=7)), iso(as_of)))]
        connection.rollback()
    result = analyze(rows, as_of)
    result["database_coverage"] = coverage
    return result


def self_test():
    import tempfile
    assert distribution([1, 2, 10, None]) == {"n": 3, "missing": 1, "missing_pct": 25.0,
                                              "p50_s": 2, "p95_s": 10, "max_s": 10}
    assert elapsed({"a": timestamp("2026-01-01T00:01:00Z"),
                    "b": timestamp("2026-01-01T00:00:00Z")}, "a", "b") is None
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "synthetic.sqlite"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("CREATE TABLE jobs(created_at, data, status, model, source, stats_excluded, user_token, account_id)")
            for second, start, end in ((0, 1, 11), (2, 22, 32)):
                stamp = lambda n: f"2026-01-01T00:00:{n:02d}.000Z"
                connection.execute("INSERT INTO jobs VALUES (?, ?, 'done', 'nai-diffusion-5-full', 'novelai-native', 0, 'SECRET_KEY', 'SECRET_ACCOUNT')",
                    (stamp(second), json.dumps({"startedAt": stamp(start), "completedAt": stamp(end),
                        "upstreamStartedAt": stamp(start), "upstreamHeadersAt": stamp(start),
                        "upstreamCompletedAt": stamp(end), "prompt": "SECRET_PROMPT"})))
            for second, status, timing in ((34, "queued", {}),
                    (35, "running", {"startedAt": stamp(44)}),
                    (45, "failed", {"startedAt": stamp(46), "completedAt": stamp(47),
                                    "upstreamStartedAt": stamp(46), "upstreamHeadersAt": stamp(45)})):
                connection.execute("INSERT INTO jobs VALUES (?, ?, ?, 'nai-diffusion-5-full', 'novelai-native', 0, 'SECRET_KEY', 'SECRET_ACCOUNT')",
                                   (stamp(second), json.dumps(timing), status))
            connection.commit()
        before = path.read_bytes()
        result = read_database(path, timestamp("2026-01-01T01:00:00Z"))
        assert path.read_bytes() == before
        summary = result["windows"]["24h"]
        assert summary["n"] == 5 and summary["queue"]["p95_s"] == 20
        assert summary["queue"]["missing"] == 1
        assert summary["without_start"] == {"queued": 1}
        assert summary["queued_age"]["n"] == summary["running_age"]["n"] == 1
        assert summary["negative_stage_counts"]["headers"] == 1
        assert summary["stages"]["headers"]["missing"] == 3
        assert summary["extra_idle_after_10s"]["backlogged_pairs"]["max_s"] == 1
        # First task has no predecessor; the failed task follows an unfinished one.
        assert summary["extra_idle_after_10s"]["all_pairs"]["missing"] == 2
        assert summary["observed_queue_peak"] == {"global": 2, "single_member": 2, "unknown_intervals": 0}
        assert "SECRET" not in json.dumps(result)
    print("queue-baseline self-test passed (percentiles, invalid time, read-only extraction, anonymity, idle)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", nargs="?", type=Path)
    parser.add_argument("--as-of", help="UTC timestamp, e.g. 2026-10-16T18:30:00Z; defaults to current UTC")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        if not args.database:
            parser.error("database path is required")
        as_of = timestamp(args.as_of) if args.as_of else datetime.now(UTC)
        if not as_of:
            parser.error("--as-of must include a timezone")
        print(json.dumps(read_database(args.database, as_of), ensure_ascii=True, indent=2))
