from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import UUID, uuid4

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator


DATABASE_URL = os.getenv("DATABASE_URL", "postgres://soup:soup-local-only@localhost:5432/soup")
SSE_POLL_SECONDS = float(os.getenv("SSE_POLL_SECONDS", "0.5"))
MAX_BATCH_EVENTS = 500
STATIC_DIR = Path(__file__).parent / "static"

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS api_keys_project_active_idx ON api_keys(project_id) WHERE revoked_at IS NULL;
CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    next_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_project_created_idx ON runs(project_id, created_at DESC);
CREATE TABLE IF NOT EXISTS metric_batches (
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    batch_id UUID NOT NULL,
    payload_hash TEXT NOT NULL,
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, batch_id)
);
CREATE TABLE IF NOT EXISTS metric_events (
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    server_seq BIGINT NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    client_time TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, event_id),
    UNIQUE (run_id, server_seq)
);
CREATE INDEX IF NOT EXISTS metric_events_run_seq_idx ON metric_events(run_id, server_seq);
"""


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class RunCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)


class MetricEvent(BaseModel):
    event_id: UUID
    metric_name: str = Field(min_length=1, max_length=120)
    value: float
    client_time: Optional[datetime] = None

    @field_validator("value")
    @classmethod
    def finite_value(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("metric value must be a finite number")
        return value

    @field_validator("client_time")
    @classmethod
    def timezone_aware_client_time(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is not None:
            if value.tzinfo is None:
                raise ValueError("client_time must include a timezone")
            return value.astimezone(timezone.utc)
        return value


class MetricBatch(BaseModel):
    batch_id: UUID
    events: list[MetricEvent] = Field(min_length=1, max_length=MAX_BATCH_EVENTS)


def key_digest(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def create_raw_key() -> str:
    return "soup_" + secrets.token_urlsafe(32)


def json_default(value: Any) -> Any:
    if isinstance(value, (UUID, datetime)):
        return str(value)
    raise TypeError("Value is not JSON serializable")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=json_default)


def event_payload(event: MetricEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "metric_name": event.metric_name,
        "value": event.value,
        "client_time": event.client_time.isoformat() if event.client_time else None,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=30, command_timeout=30)
    async with app.state.pool.acquire() as connection:
        await connection.execute(SCHEMA)
    yield
    await app.state.pool.close()


app = FastAPI(title="Soup Experiment Tracker", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.middleware("http")
async def protect_api_responses_from_shared_caches(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "private, no-store, no-transform"
        response.headers["Vary"] = "X-API-Key"
    return response


async def current_project(
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> asyncpg.Record:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    digest = key_digest(x_api_key)
    async with request.app.state.pool.acquire() as connection:
        row = await connection.fetchrow(
            """SELECT p.id, p.name FROM projects p
               JOIN api_keys k ON k.project_id = p.id
               WHERE k.key_hash = $1 AND k.revoked_at IS NULL""",
            digest,
        )
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return row


async def owned_run(connection: asyncpg.Connection, run_id: UUID, project_id: UUID) -> asyncpg.Record:
    row = await connection.fetchrow(
        "SELECT id, project_id, name, next_seq, created_at FROM runs WHERE id=$1 AND project_id=$2",
        run_id,
        project_id,
    )
    if row is None:
        # Same response for missing and foreign runs avoids confirming another project's IDs.
        raise HTTPException(status_code=404, detail="Run not found")
    return row


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    async with request.app.state.pool.acquire() as connection:
        await connection.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/api/projects", status_code=201)
async def create_project(body: ProjectCreate, request: Request) -> dict[str, Any]:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Project name cannot be blank")
    project_id, key_id, raw_key = uuid4(), uuid4(), create_raw_key()
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute("INSERT INTO projects(id, name) VALUES($1, $2)", project_id, name)
            await connection.execute(
                "INSERT INTO api_keys(id, project_id, key_hash) VALUES($1, $2, $3)",
                key_id,
                project_id,
                key_digest(raw_key),
            )
    return {"project_id": str(project_id), "name": name, "api_key": raw_key}


@app.get("/api/project")
async def get_project(project: asyncpg.Record = Depends(current_project)) -> dict[str, str]:
    return {"id": str(project["id"]), "name": project["name"]}


@app.post("/api/keys/rotate")
async def rotate_key(request: Request, project: asyncpg.Record = Depends(current_project)) -> dict[str, str]:
    raw_key, key_id = create_raw_key(), uuid4()
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                "UPDATE api_keys SET revoked_at=now() WHERE project_id=$1 AND revoked_at IS NULL",
                project["id"],
            )
            await connection.execute(
                "INSERT INTO api_keys(id, project_id, key_hash) VALUES($1, $2, $3)",
                key_id,
                project["id"],
                key_digest(raw_key),
            )
    return {"api_key": raw_key}


@app.post("/api/runs", status_code=201)
async def create_run(
    body: RunCreate,
    request: Request,
    project: asyncpg.Record = Depends(current_project),
) -> dict[str, Any]:
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Run name cannot be blank")
    run_id = uuid4()
    async with request.app.state.pool.acquire() as connection:
        row = await connection.fetchrow(
            "INSERT INTO runs(id, project_id, name) VALUES($1, $2, $3) RETURNING id, name, created_at",
            run_id,
            project["id"],
            name,
        )
    return {"id": str(row["id"]), "name": row["name"], "created_at": row["created_at"].isoformat(), "event_count": 0}


@app.get("/api/runs")
async def list_runs(request: Request, project: asyncpg.Record = Depends(current_project)) -> dict[str, Any]:
    async with request.app.state.pool.acquire() as connection:
        rows = await connection.fetch(
            """SELECT r.id, r.name, r.created_at, r.next_seq AS event_count
               FROM runs r WHERE r.project_id=$1 ORDER BY r.created_at DESC""",
            project["id"],
        )
    return {"runs": [
        {"id": str(row["id"]), "name": row["name"], "created_at": row["created_at"].isoformat(), "event_count": row["event_count"]}
        for row in rows
    ]}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: UUID, request: Request, project: asyncpg.Record = Depends(current_project)) -> dict[str, Any]:
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            run = await owned_run(connection, run_id, project["id"])
            rows = await connection.fetch(
                """SELECT event_id, server_seq, metric_name, value, client_time, received_at
                   FROM metric_events WHERE run_id=$1 ORDER BY server_seq DESC LIMIT 500""",
                run_id,
            )
            events = [dict(row) for row in reversed(rows)]
    return {
        "id": str(run["id"]),
        "name": run["name"],
        "created_at": run["created_at"].isoformat(),
        "cursor": run["next_seq"],
        "events": [
            {
                "event_id": str(event["event_id"]),
                "server_seq": event["server_seq"],
                "metric_name": event["metric_name"],
                "value": event["value"],
                "client_time": event["client_time"].isoformat() if event["client_time"] else None,
                "received_at": event["received_at"].isoformat(),
            }
            for event in events
        ],
    }


@app.post("/api/runs/{run_id}/metrics")
async def ingest_metrics(
    run_id: UUID,
    body: MetricBatch,
    request: Request,
    project: asyncpg.Record = Depends(current_project),
) -> dict[str, Any]:
    batch_json = canonical_json({"events": [event_payload(event) for event in body.events]})
    batch_hash = hashlib.sha256(batch_json.encode("utf-8")).hexdigest()
    event_ids = [event.event_id for event in body.events]

    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            # This row lock serializes sequence allocation for one run.
            run = await connection.fetchrow(
                "SELECT id, next_seq FROM runs WHERE id=$1 AND project_id=$2 FOR UPDATE",
                run_id,
                project["id"],
            )
            if run is None:
                raise HTTPException(status_code=404, detail="Run not found")

            prior = await connection.fetchrow(
                "SELECT payload_hash, response_json FROM metric_batches WHERE run_id=$1 AND batch_id=$2",
                run_id,
                body.batch_id,
            )
            if prior is not None:
                if prior["payload_hash"] != batch_hash:
                    raise HTTPException(status_code=409, detail="batch_id already used with different content")
                saved_response = prior["response_json"]
                response_data = json.loads(saved_response) if isinstance(saved_response, str) else dict(saved_response)
                response_data["replayed"] = True
                return response_data

            existing_rows = await connection.fetch(
                """SELECT event_id, server_seq, metric_name, value, client_time
                   FROM metric_events WHERE run_id=$1 AND event_id=ANY($2::uuid[])""",
                run_id,
                event_ids,
            )
            existing = {row["event_id"]: row for row in existing_rows}
            new_events: list[MetricEvent] = []
            seq_by_id: dict[str, int] = {}
            seen_in_batch: dict[UUID, MetricEvent] = {}
            duplicates = 0
            for event in body.events:
                earlier = seen_in_batch.get(event.event_id)
                if earlier is not None:
                    if event_payload(earlier) != event_payload(event):
                        raise HTTPException(status_code=409, detail="event_id repeated with different content in batch")
                    duplicates += 1
                    continue
                seen_in_batch[event.event_id] = event
                old = existing.get(event.event_id)
                if old is None:
                    new_events.append(event)
                    continue
                same_time = (old["client_time"].isoformat() if old["client_time"] else None) == (
                    event.client_time.isoformat() if event.client_time else None
                )
                if old["metric_name"] != event.metric_name or old["value"] != event.value or not same_time:
                    raise HTTPException(status_code=409, detail="event_id already used with different content")
                duplicates += 1
                seq_by_id[str(event.event_id)] = old["server_seq"]

            next_seq = run["next_seq"]
            for event in new_events:
                next_seq += 1
                seq_by_id[str(event.event_id)] = next_seq
                await connection.execute(
                    """INSERT INTO metric_events(run_id, event_id, server_seq, metric_name, value, client_time)
                       VALUES($1, $2, $3, $4, $5, $6)""",
                    run_id,
                    event.event_id,
                    next_seq,
                    event.metric_name,
                    event.value,
                    event.client_time,
                )
            if new_events:
                await connection.execute("UPDATE runs SET next_seq=$2 WHERE id=$1", run_id, next_seq)

            response_data = {
                "batch_id": str(body.batch_id),
                "accepted": len(new_events),
                "duplicates": duplicates,
                "server_seq": seq_by_id,
                "replayed": False,
            }
            await connection.execute(
                """INSERT INTO metric_batches(run_id, batch_id, payload_hash, response_json)
                   VALUES($1, $2, $3, $4::jsonb)""",
                run_id,
                body.batch_id,
                batch_hash,
                canonical_json(response_data),
            )
    return response_data


@app.get("/api/runs/{run_id}/events")
async def stream_events(
    run_id: UUID,
    request: Request,
    after: int = Query(default=0, ge=0),
    last_event_id: Optional[str] = Header(default=None, alias="Last-Event-ID"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> StreamingResponse:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    digest = key_digest(x_api_key)
    cursor = after
    if last_event_id and last_event_id.isdigit():
        cursor = max(cursor, int(last_event_id))

    async with request.app.state.pool.acquire() as connection:
        auth = await connection.fetchrow(
            """SELECT r.id FROM runs r JOIN api_keys k ON k.project_id=r.project_id
               WHERE r.id=$1 AND k.key_hash=$2 AND k.revoked_at IS NULL""",
            run_id,
            digest,
        )
    if auth is None:
        raise HTTPException(status_code=404, detail="Run not found")

    async def event_stream():
        nonlocal cursor
        while True:
            if await request.is_disconnected():
                break
            async with request.app.state.pool.acquire() as connection:
                rows = await connection.fetch(
                    """SELECT k.project_id AS authorized_project, r.id AS authorized_run,
                              m.event_id, m.server_seq, m.metric_name, m.value,
                              m.client_time, m.received_at
                       FROM api_keys k
                       JOIN runs r ON r.project_id=k.project_id AND r.id=$2
                       LEFT JOIN metric_events m ON m.run_id=r.id AND m.server_seq>$3
                       WHERE k.key_hash=$1 AND k.revoked_at IS NULL
                       ORDER BY m.server_seq NULLS LAST LIMIT 501""",
                    digest,
                    run_id,
                    cursor,
                )
            if not rows:
                yield "event: auth-error\ndata: {}\n\n"
                break
            if rows[0]["authorized_run"] is None:
                yield "event: auth-error\ndata: {}\n\n"
                break
            found = False
            for row in rows:
                if row["server_seq"] is None:
                    continue
                found = True
                cursor = row["server_seq"]
                payload = {
                    "event_id": str(row["event_id"]),
                    "server_seq": row["server_seq"],
                    "metric_name": row["metric_name"],
                    "value": row["value"],
                    "client_time": row["client_time"].isoformat() if row["client_time"] else None,
                    "received_at": row["received_at"].isoformat(),
                }
                yield f"id: {cursor}\nevent: metric\ndata: {canonical_json(payload)}\n\n"
            if not found:
                yield ": keep-alive\n\n"
            await asyncio.sleep(SSE_POLL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


@app.get("/api/openapi.json", include_in_schema=False)
async def openapi_document() -> dict[str, Any]:
    return app.openapi()
