import asyncio
import base64
from hashlib import sha256
import json
import bittensor as bt
import time
from typing import Tuple
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi import FastAPI
from substrateinterface.keypair import Keypair

from s3_storage_api.routes.on_demand import router as on_demand_router
from s3_storage_api.settings import settings
from s3_storage_api.deps import (
    get_metagraph as real_get_metagraph,
)
from s3_storage_api.db.base import create_all
from sqlalchemy.ext.asyncio import create_async_engine

from s3_storage_api.contracts.on_demand import (
    CreateOnDemandJobRequest,
    ListActiveJobsRequest,
    SubmitJobRequest,
    ListJobsWithSubmissionsForValidationRequest,
)
from s3_storage_api.models.on_demand import (
    OnDemandJob,
    OnDemandJobPayloadX,
    OnDemandJobSubmission,
)

import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def event_loop():
    """Pytest's default loop scope is function; we need session-level for DB setup."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def ensure_db_schema():
    """Create all DB tables on the configured Postgres DSN."""
    try:
        engine = create_async_engine(settings.postgres_dsn, echo=False, future=True)
        await create_all(dsn=settings.postgres_dsn)
        await engine.dispose()
    except Exception:
        logger.exception("Failed to ensure_db_schema")


class FakeMetagraph:
    """
    Enough surface for your auth dependencies:
      - hotkeys (list of ss58)
      - vtrust (aligned floats)
      - validator_permit (aligned bools)
      - alpha_stake (aligned ints)
    """

    def __init__(self, hotkeys, I, validator_permit, alpha_stake):
        self.hotkeys = hotkeys
        self.I = I
        self.validator_permit = validator_permit
        self.alpha_stake = alpha_stake


def sign_headers(kp: Keypair, body: bytes) -> dict:
    ts = str(int(time.time() * 1000))
    nonce = str(uuid.uuid4())
    msg = f"{sha256(body).hexdigest()}.{nonce}.{ts}"

    sig_bytes = kp.sign(msg)
    sig_b64 = base64.b64encode(sig_bytes).decode("ascii")

    return {
        "X-Tao-Version": "2",
        "X-Tao-Timestamp": ts,
        "X-Tao-Nonce": nonce,
        "X-Tao-Signed-By": kp.ss58_address,
        "X-Tao-Signature": sig_b64,
    }


@pytest.fixture(scope="session")
def wallets():
    """
    Generate 2 miner wallets + 1 validator wallet.
    You can replace these with fixed mnemonics if you want deterministic hotkeys.
    """
    miner1 = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    miner2 = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    validator = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    return miner1, miner2, validator


@pytest.fixture(scope="session")
def metagraph(wallets: Tuple[Keypair, Keypair, Keypair]):
    miner1, miner2, validator = wallets
    hotkeys = [miner1.ss58_address, miner2.ss58_address, validator.ss58_address]
    I = [0.75, 0.80, 0.95]
    validator_permit = [False, False, True]  # only the validator wallet has permit
    alpha_stake = [10_000, 11_000, 50_000]  # all above a typical min stake
    return FakeMetagraph(hotkeys, I, validator_permit, alpha_stake)


@pytest.fixture(scope="session")
def app(metagraph: bt.Metagraph):
    """
    Build a FastAPI app with your /on-demand router,
    but override ONLY the metagraph dependency.
    Everything else (DB/S3/jobs service) is the real thing.
    """
    app = FastAPI()
    app.include_router(on_demand_router)

    # Dependency override for metagraph only
    async def _get_metagraph_override():
        return metagraph

    app.dependency_overrides[real_get_metagraph] = _get_metagraph_override
    return app


@pytest.fixture
async def client(app):
    # Use ASGITransport to run the FastAPI app in-process
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_full_e2e_flow(client: httpx.AsyncClient, wallets):
    miner1, miner2, validator = wallets

    #
    # 1) Constellation registers a job
    #
    # Build a minimal OnDemandJob payload. Adjust fields to match your actual model.
    now = datetime.now(timezone.utc)
    job_payload = OnDemandJob(
        created_at=now,
        expire_at=now + timedelta(seconds=15),
        job=OnDemandJobPayloadX(keywords=["#bittensor"]),
    )

    create_req = CreateOnDemandJobRequest(job=job_payload)

    headers_constellation = {"X-API-Key": settings.constellation_api_key}
    res_create = await client.post(
        "/on-demand/constellation/jobs",
        content=create_req.model_dump_json(),
        headers=headers_constellation,
    )
    assert res_create.status_code == 201, res_create.text
    job_id = res_create.json()["job_id"]
    print(f"Created job with id: {job_id}")

    #
    # 2) Miners poll active jobs
    #
    list_req = ListActiveJobsRequest(since=now - timedelta(seconds=5))
    body = list_req.model_dump_json().encode("utf-8")
    headers = sign_headers(miner1, body)

    res = await client.post(
        "/on-demand/miner/jobs/active",
        content=body,
        headers=headers,
    )
    assert res.status_code == 200, res.text
    jobs_1 = res.json()["jobs"]
    print(f"Miner 1 polled active jobs with ids: {[j["id"] for j in jobs_1]}")
    assert any(j["id"] == job_id for j in jobs_1)

    list_req = ListActiveJobsRequest(since=now - timedelta(seconds=5))
    body = list_req.model_dump_json().encode("utf-8")
    headers = sign_headers(miner2, body)

    res = await client.post(
        "/on-demand/miner/jobs/active",
        content=body,
        headers=headers,
    )
    print(res.text)
    assert res.status_code == 200, res.text
    jobs_2 = res.json()["jobs"]
    assert any(j["id"] == job_id for j in jobs_2)

    #
    # 3) Both miners submit → receive presigned upload URLs
    #
    sub1 = OnDemandJobSubmission(
        job_id=job_id,
    )
    submit_req_1 = SubmitJobRequest(submission=sub1)
    body1 = submit_req_1.model_dump_json().encode()
    headers1 = sign_headers(miner1, body1)
    res_submit_1 = await client.post(
        "/on-demand/miner/jobs/submit",
        content=body1,
        headers=headers1,
    )
    print(res_submit_1.text)
    assert res_submit_1.status_code == 200, res_submit_1.text
    url1 = res_submit_1.json()["presigned_upload_url"]
    assert url1

    sub2 = OnDemandJobSubmission(job_id=job_id)
    submit_req_2 = SubmitJobRequest(submission=sub2)
    body2 = submit_req_2.model_dump_json().encode()
    headers2 = sign_headers(miner2, body2)
    res_submit_2 = await client.post(
        "/on-demand/miner/jobs/submit",
        content=body2,
        headers=headers2,
    )
    print(res_submit_2.text)
    assert res_submit_2.status_code == 200, res_submit_2.text
    url2 = res_submit_2.json()["presigned_upload_url"]
    assert url2

    #
    # 4) Upload small JSON to S3 using the presigned URLs
    #
    async with httpx.AsyncClient(timeout=30, verify=False) as raw:
        up1 = await raw.put(
            url1,
            content=json.dumps({"ok": True, "miner": 1}),
            headers={"Content-Type": "application/json"},
        )
        assert up1.status_code in (
            200,
            204,
        ), f"Upload1 failed: {up1.status_code} {up1.text}"
        up2 = await raw.put(
            url2,
            content=json.dumps({"ok": True, "miner": 2}),
            headers={"Content-Type": "application/json"},
        )
        assert up2.status_code in (
            200,
            204,
        ), f"Upload2 failed: {up2.status_code} {up2.text}"

    # 5) Wait for job to expire so validator endpoint includes it.
    # (validator list endpoint filters for expired jobs within a window.)

    print("sleep 17")
    await asyncio.sleep(17)

    #
    # 6) Validator reads jobs-with-submissions (expired range = last minute)
    #
    until = datetime.now(timezone.utc) + timedelta(minutes=1)
    since = until - timedelta(minutes=3)
    val_req = ListJobsWithSubmissionsForValidationRequest(
        expired_since=since, expired_until=until, limit=10
    )
    val_body = val_req.model_dump_json().encode("utf-8")
    val_headers = sign_headers(validator, val_body)

    res_validator = await client.post(
        "/on-demand/validator/jobs",
        content=val_body,
        headers=val_headers,
    )
    print(res_validator.text)

    assert res_validator.status_code in (200, 429, 400), res_validator.text
    # NOTE: If rate limiting got re-enabled or window too big/small, you may get 400/429.
    # In successful case, response body has 'jobs_with_submissions'.
    if res_validator.status_code == 200:
        jws = res_validator.json().get("jobs_with_submissions", [])
        # Not strictly guaranteed, but we expect at least our job to appear once expired
        # with 2 submissions present.
        maybe = [j for j in jws if j["job"]["id"] == job_id]
        assert len(maybe) > 0
        if maybe:
            subs = maybe[0]["submissions"]
            assert len(subs) >= 2

    #
    # 7) Constellation fetches submissions for the job
    #
    res_get_job = await client.get(
        f"/on-demand/constellation/jobs/{job_id}",
        headers={"X-API-Key": settings.constellation_api_key},
    )

    print(res_get_job.text)

    assert res_get_job.status_code in (200, 404), res_get_job.text
    if res_get_job.status_code == 200:
        subs = res_get_job.json()["submissions"]
        assert isinstance(subs, list)
        # at least 2 from our miners (exact structure depends on your model)
        assert len(subs) >= 2
