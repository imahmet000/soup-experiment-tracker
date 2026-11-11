"""Run 50 concurrent runs at 10 metric events/second each.

Use after `docker compose up -d db app`:
  docker compose exec app python scripts/load_check.py --seconds 10 > work/load-output.txt
"""

import argparse
import asyncio
import math
import statistics
import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--rate", type=int, default=10, help="events per second per run")
    parser.add_argument("--seconds", type=int, default=10)
    args = parser.parse_args()
    timeout = httpx.Timeout(15, connect=5)
    limits = httpx.Limits(max_connections=max(100, args.runs * 2), max_keepalive_connections=max(50, args.runs))
    latencies: list[float] = []
    failures: list[str] = []
    sent_by_run: dict[str, int] = {}
    stream_sequences: dict[str, list[int]] = {}
    async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout, limits=limits) as client:
        project_response = await client.post("/api/projects", json={"name": f"load-{uuid4().hex[:8]}"})
        project_response.raise_for_status()
        api_key = project_response.json()["api_key"]
        headers = {"X-API-Key": api_key}
        runs: list[str] = []
        for number in range(args.runs):
            response = await client.post("/api/runs", headers=headers, json={"name": f"load-run-{number:02}"})
            response.raise_for_status()
            runs.append(response.json()["id"])

        expected_per_run = args.rate * args.seconds

        async def listen_to_run(run_id: str) -> None:
            seen: list[int] = []
            stream_sequences[run_id] = seen
            try:
                async with client.stream(
                    "GET",
                    f"/api/runs/{run_id}/events?after=0",
                    headers={**headers, "Accept": "text/event-stream"},
                ) as response:
                    if response.status_code != 200:
                        failures.append(f"SSE {run_id}: HTTP {response.status_code}")
                        return
                    pending_id = None
                    async for line in response.aiter_lines():
                        if line.startswith("id: "):
                            pending_id = int(line[4:])
                        elif line == "" and pending_id is not None:
                            seen.append(pending_id)
                            pending_id = None
                            if len(seen) >= expected_per_run:
                                return
            except Exception as error:
                failures.append(f"SSE {run_id}: {type(error).__name__}: {error}")

        start = time.monotonic() + 1

        async def send_for_run(run_id: str) -> None:
            expected = expected_per_run
            for index in range(expected):
                target = start + index / args.rate
                delay = target - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                now = datetime.now(timezone.utc).isoformat()
                payload = {
                    "batch_id": str(uuid4()),
                    "events": [{
                        "event_id": str(uuid4()),
                        "metric_name": "throughput_probe",
                        "value": float(index),
                        "client_time": now,
                    }],
                }
                before = time.monotonic()
                try:
                    response = await client.post(f"/api/runs/{run_id}/metrics", headers=headers, json=payload)
                    latencies.append((time.monotonic() - before) * 1000)
                    if response.status_code != 200:
                        failures.append(f"{run_id}: HTTP {response.status_code} {response.text[:180]}")
                    else:
                        sent_by_run[run_id] = sent_by_run.get(run_id, 0) + 1
                except Exception as error:  # Keep the raw failure visible in the summary.
                    failures.append(f"{run_id}: {type(error).__name__}: {error}")

        stream_tasks = [asyncio.create_task(listen_to_run(run_id)) for run_id in runs]
        await asyncio.sleep(0.5)
        await asyncio.gather(*(send_for_run(run_id) for run_id in runs))
        try:
            await asyncio.wait_for(asyncio.gather(*stream_tasks), timeout=30)
        except asyncio.TimeoutError:
            failures.append("SSE readers did not receive all events within 30 seconds")
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)

        mismatches = []
        for run_id in runs:
            response = await client.get(f"/api/runs/{run_id}", headers=headers)
            response.raise_for_status()
            snapshot = response.json()
            if snapshot["cursor"] != expected_per_run or len(snapshot["events"]) != expected_per_run:
                mismatches.append(f"{run_id}: cursor={snapshot['cursor']} rows={len(snapshot['events'])}")
            stream_ids = stream_sequences.get(run_id, [])
            if stream_ids != list(range(1, expected_per_run + 1)):
                mismatches.append(f"{run_id}: SSE received {len(stream_ids)} events; IDs/order mismatch")

    total = args.runs * args.rate * args.seconds
    ordered = sorted(latencies)
    p95 = ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)] if ordered else float("nan")
    print(f"runs={args.runs} rate_per_run={args.rate}/s duration={args.seconds}s")
    print(f"target_events={total} accepted_http={sum(sent_by_run.values())} failures={len(failures)}")
    print(f"concurrent_sse_readers={len(stream_sequences)} sse_events={sum(len(items) for items in stream_sequences.values())}")
    if ordered:
        print(f"request_latency_ms median={statistics.median(ordered):.1f} p95={p95:.1f} max={max(ordered):.1f}")
    print(f"database_snapshot_mismatches={len(mismatches)}")
    for failure in failures[:20]:
        print(f"FAIL {failure}")
    for mismatch in mismatches[:20]:
        print(f"MISMATCH {mismatch}")
    if failures or mismatches or sum(sent_by_run.values()) != total:
        raise SystemExit(1)
    print("PASS: all accepted events match the persisted run snapshots")


if __name__ == "__main__":
    asyncio.run(main())
