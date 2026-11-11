# Soup Take-Home — Submission Note

## What is included

A small experiment-metrics tracker: a FastAPI service, PostgreSQL storage, a browser UI, Docker Compose setup, API documentation at `/docs`, integration tests, a browser reconnect check, and a load-check script. Start it with `docker compose up --build`; open `http://localhost:8000`.

The database is the source of truth. Each batch has a stable `batch_id`, each event a stable `event_id`, and the server assigns increasing `server_seq` values per run. A retry of identical content is idempotent; reusing an ID with different content is rejected. Events and batch receipts are written atomically. The UI resumes its SSE stream from the last server sequence after reconnecting.

## Verification and result

- Integration suite: **6 passed** (`outputs/test-results.txt`).
- Browser reconnect scenario: **passed**; after going offline, the browser caught up to `server_seq=3` and matched the 3-event database snapshot (`outputs/browser-e2e-output.txt`).
- Load check: **passed** for 50 runs × 10 events/second over 10 seconds: 5,000 events accepted and delivered through 50 concurrent SSE readers, with no failures or snapshot mismatches. HTTP request latency was 45.6 ms median and 57.9 ms p95 (`outputs/load-results.txt`).

This is one short local run for the stated target. It supports a “ship for this tested load” decision, not a claim about longer runs, other environments, or higher loads.

## What surprised me, and what still concerns me

The subtle part was keeping the snapshot-to-stream handoff correct: the browser must receive every event committed around the time it fetches the initial snapshot, so it needs a cursor from the same database snapshot and must resume with events strictly after that cursor. The remaining concern is the SSE implementation's periodic PostgreSQL polling. The short local load check did not establish its behavior over long periods or under database/network degradation; before increasing load, I would run a longer soak and monitor stream lag, query-pool waits, and request latency.

## AI assistance

OpenAI Codex was used to translate and clarify the assignment, discuss the architecture, assist with implementation, and create and run verification scenarios. The implementation uses stable batch/event IDs, per-run server sequencing, atomic PostgreSQL writes, and cursor-based SSE recovery. The architecture was kept deliberately small, with PostgreSQL as the source of truth and polling for SSE instead of adding a separate broker. The included integration, browser, and load outputs record the checks performed on the resulting implementation.
