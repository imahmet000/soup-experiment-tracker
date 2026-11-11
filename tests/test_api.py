import asyncio
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app, stream_events


@pytest_asyncio.fixture
async def client():
    async with app.router.lifespan_context(app):
        async with app.state.pool.acquire() as connection:
            await connection.execute("TRUNCATE projects CASCADE")
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
            yield http


async def make_project(client: AsyncClient, name: str):
    response = await client.post("/api/projects", json={"name": name})
    assert response.status_code == 201, response.text
    return response.json()


async def make_run(client: AsyncClient, key: str, name: str = "run"):
    response = await client.post("/api/runs", headers={"X-API-Key": key}, json={"name": name})
    assert response.status_code == 201, response.text
    return response.json()


def batch(events, batch_id=None):
    return {
        "batch_id": str(batch_id or uuid4()),
        "events": events,
    }


def event(event_id=None, metric="accuracy", value=0.9, client_time="2026-11-10T10:00:00Z"):
    return {
        "event_id": str(event_id or uuid4()),
        "metric_name": metric,
        "value": value,
        "client_time": client_time,
    }


@pytest.mark.asyncio
async def test_retry_is_idempotent_and_snapshot_matches_database(client):
    project = await make_project(client, "alpha")
    run = await make_run(client, project["api_key"])
    batch_id = uuid4()
    payload = batch([event(metric="loss", value=0.4), event(metric="accuracy", value=0.95)], batch_id)
    url = f"/api/runs/{run['id']}/metrics"

    first = await client.post(url, headers={"X-API-Key": project["api_key"]}, json=payload)
    retry = await client.post(url, headers={"X-API-Key": project["api_key"]}, json=payload)
    assert first.status_code == 200
    assert retry.status_code == 200
    assert retry.json()["replayed"] is True
    assert first.json()["server_seq"] == retry.json()["server_seq"]

    changed = batch([event(metric="loss", value=0.1)], batch_id)
    conflict = await client.post(url, headers={"X-API-Key": project["api_key"]}, json=changed)
    assert conflict.status_code == 409

    snapshot = await client.get(f"/api/runs/{run['id']}", headers={"X-API-Key": project["api_key"]})
    assert snapshot.status_code == 200
    assert snapshot.json()["cursor"] == 2
    assert [item["server_seq"] for item in snapshot.json()["events"]] == [1, 2]
    async with app.state.pool.acquire() as connection:
        count = await connection.fetchval("SELECT count(*) FROM metric_events WHERE run_id=$1", run["id"])
    assert count == 2


@pytest.mark.asyncio
async def test_event_id_duplicate_across_batches_is_not_applied_twice(client):
    project = await make_project(client, "alpha")
    run = await make_run(client, project["api_key"])
    event_id = uuid4()
    url = f"/api/runs/{run['id']}/metrics"
    first = await client.post(url, headers={"X-API-Key": project["api_key"]}, json=batch([event(event_id)]))
    duplicate = await client.post(url, headers={"X-API-Key": project["api_key"]}, json=batch([event(event_id)]))
    assert first.status_code == duplicate.status_code == 200
    assert duplicate.json()["accepted"] == 0
    assert duplicate.json()["duplicates"] == 1
    assert duplicate.json()["server_seq"][str(event_id)] == 1


@pytest.mark.asyncio
async def test_concurrent_batches_get_unique_per_run_sequence(client):
    project = await make_project(client, "alpha")
    run = await make_run(client, project["api_key"])
    url = f"/api/runs/{run['id']}/metrics"

    async def send(index: int):
        return await client.post(
            url,
            headers={"X-API-Key": project["api_key"]},
            json=batch([event(metric="step", value=index)]),
        )

    responses = await asyncio.gather(*(send(index) for index in range(12)))
    assert all(response.status_code == 200 for response in responses)
    snapshot = await client.get(f"/api/runs/{run['id']}", headers={"X-API-Key": project["api_key"]})
    data = snapshot.json()
    assert data["cursor"] == 12
    assert [item["server_seq"] for item in data["events"]] == list(range(1, 13))


@pytest.mark.asyncio
async def test_projects_cannot_read_write_or_subscribe_to_other_project(client):
    alpha = await make_project(client, "alpha")
    beta = await make_project(client, "beta")
    run = await make_run(client, alpha["api_key"])

    read = await client.get(f"/api/runs/{run['id']}", headers={"X-API-Key": beta["api_key"]})
    write = await client.post(
        f"/api/runs/{run['id']}/metrics",
        headers={"X-API-Key": beta["api_key"]},
        json=batch([event()]),
    )
    subscribe = await client.get(
        f"/api/runs/{run['id']}/events",
        headers={"X-API-Key": beta["api_key"]},
    )
    assert read.status_code == write.status_code == subscribe.status_code == 404


@pytest.mark.asyncio
async def test_sse_replays_database_events_after_cursor(client):
    project = await make_project(client, "alpha")
    run = await make_run(client, project["api_key"])
    await client.post(
        f"/api/runs/{run['id']}/metrics",
        headers={"X-API-Key": project["api_key"]},
        json=batch([event()]),
    )
    snapshot = (await client.get(f"/api/runs/{run['id']}", headers={"X-API-Key": project["api_key"]})).json()
    cursor = snapshot["cursor"]
    await client.post(
        f"/api/runs/{run['id']}/metrics",
        headers={"X-API-Key": project["api_key"]},
        json=batch([event(metric="loss", value=0.2)]),
    )

    fake_request = SimpleNamespace(app=app, is_disconnected=lambda: asyncio.sleep(0, result=False))
    response = await stream_events(
        run_id=UUID(run["id"]),
        request=fake_request,
        after=cursor,
        last_event_id=None,
        x_api_key=project["api_key"],
    )
    iterator = response.body_iterator
    first_message = await asyncio.wait_for(anext(iterator), timeout=2)
    await iterator.aclose()
    assert "id: 2" in first_message
    assert '"metric_name":"loss"' in first_message


@pytest.mark.asyncio
async def test_rotated_key_is_revoked(client):
    project = await make_project(client, "alpha")
    rotated = await client.post("/api/keys/rotate", headers={"X-API-Key": project["api_key"]})
    assert rotated.status_code == 200
    assert (await client.get("/api/project", headers={"X-API-Key": project["api_key"]})).status_code == 401
    assert (await client.get("/api/project", headers={"X-API-Key": rotated.json()["api_key"]})).status_code == 200
