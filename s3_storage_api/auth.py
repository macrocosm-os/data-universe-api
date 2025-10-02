import base64
from hashlib import sha256
import time
import uuid
from fastapi import Depends, HTTPException, Header, Request, status
from cachetools import TTLCache
from pydantic import BaseModel
from substrateinterface.keypair import Keypair
import bittensor as bt

from s3_storage_api.deps import get_metagraph
from s3_storage_api.settings import settings

import structlog

logger = structlog.get_logger(__name__)


def _bind_entry_context(
    request: Request,
    role: str,
    x_signed_by: str | None = None,
    x_nonce: str | None = None,
    x_ts: str | int | None = None,
    x_version: str | None = None,
):
    structlog.contextvars.bind_contextvars(
        role=role,
        method=request.method,
        path=request.url.path,
        request_id=request.headers.get("x-request-id")
        or request.headers.get("x-correlation-id"),
        tao_version=x_version,
        signed_by=x_signed_by,
        nonce=x_nonce,
        ts=x_ts,
        client_ip=(request.client.host if request.client else None),
        user_agent=request.headers.get("user-agent"),
    )


def _http_unauth(detail: str, code=status.HTTP_401_UNAUTHORIZED):
    raise HTTPException(status_code=code, detail=detail)


async def verify_constellation(x_api_key: str = Header(..., alias="X-API-Key")):
    if x_api_key != settings.constellation_api_key:
        _http_unauth(
            detail="Invalid or missing X-API-Key Header",
        )

    return True


class BittensorMinerAuth(BaseModel):
    miner_hotkey: str
    miner_uid: int
    miner_incentive: float


class BittensorValidatorAuth(BaseModel):
    validator_hotkey: str
    validator_uid: int
    permit: bool
    stake: int


_nonce_cache = TTLCache(maxsize=20000, ttl=(settings.allowed_clock_skew_ms / 1000) + 5)


async def verify_bittensor_miner(
    request: Request,
    x_version: str = Header(..., alias="X-Tao-Version"),
    x_ts: str = Header(..., alias="X-Tao-Timestamp"),
    x_nonce: str = Header(..., alias="X-Tao-Nonce"),
    x_signed_by: str = Header(..., alias="X-Tao-Signed-By"),
    x_sig: str = Header(..., alias="X-Tao-Signature"),
    metagraph: bt.Metagraph = Depends(get_metagraph),
) -> BittensorMinerAuth:
    _bind_entry_context(
        request,
        role="miner",
        x_signed_by=x_signed_by,
        x_nonce=x_nonce,
        x_ts=x_ts,
        x_version=x_version,
    )

    if x_version != "2":
        await logger.awarning("auth_version_unsupported")
        _http_unauth("Unsupported X-Tao-Version")

    try:
        ts = int(x_ts)
    except Exception:
        await logger.awarning("auth_timestamp_invalid")
        _http_unauth("Invalid timestamp", status.HTTP_400_BAD_REQUEST)

    now_ms = round(time.time() * 1000)
    if ts + settings.allowed_clock_skew_ms < now_ms:
        await logger.awarning(
            "auth_request_too_old",
            now_ms=now_ms,
            allowed_skew_ms=settings.allowed_clock_skew_ms,
        )
        _http_unauth("Request too old", status.HTTP_400_BAD_REQUEST)

    # Nonce checks
    try:
        uuid.UUID(x_nonce)
    except Exception:
        await logger.awarning("auth_nonce_invalid")
        _http_unauth("Invalid nonce", status.HTTP_400_BAD_REQUEST)

    if x_nonce in _nonce_cache:
        await logger.awarning("auth_replay_detected")
        _http_unauth("Replay detected", status.HTTP_400_BAD_REQUEST)

    _nonce_cache[x_nonce] = True

    # Signature
    body = await request.body()
    body_sha = sha256(body).hexdigest()
    message = f"{body_sha}.{x_nonce}.{ts}"

    try:
        kp = Keypair(ss58_address=x_signed_by)
        try:
            sig_bytes = base64.b64decode(x_sig, validate=True)
        except Exception:
            sig_bytes = bytes.fromhex(x_sig)
        ok = kp.verify(message, sig_bytes)
    except Exception:
        await logger.aexception("auth_sig_verify_error")
        _http_unauth("Signature verification error", status.HTTP_400_BAD_REQUEST)

    if not ok:
        await logger.awarning("auth_sig_mismatch", body_sha=body_sha)
        _http_unauth("Signature mismatch")

    # Metagraph identity
    try:
        uid = list(metagraph.hotkeys).index(x_signed_by)
    except ValueError:
        await logger.awarning(
            "auth_hotkey_not_found",
            netuid=settings.netuid,
            network=settings.bittensor_network,
        )
        _http_unauth(
            f"Hotkey {x_signed_by} not found on netuid {settings.netuid} ({settings.bittensor_network})."
        )

    is_active_miner = x_signed_by in metagraph.hotkeys
    if not is_active_miner:
        await logger.awarning("auth_hotkey_not_active_miner")
        _http_unauth("Hotkey is not an active miner on this subnet")

    incentive = float(metagraph.I[uid])

    structlog.contextvars.bind_contextvars(miner_uid=uid, miner_incentive=incentive)

    return BittensorMinerAuth(
        miner_hotkey=x_signed_by, miner_uid=uid, miner_incentive=incentive
    )


async def verify_bittensor_validator(
    request: Request,
    x_version: str = Header(..., alias="X-Tao-Version"),
    x_ts: str = Header(..., alias="X-Tao-Timestamp"),
    x_nonce: str = Header(..., alias="X-Tao-Nonce"),
    x_signed_by: str = Header(..., alias="X-Tao-Signed-By"),
    x_sig: str = Header(..., alias="X-Tao-Signature"),
    metagraph: bt.Metagraph = Depends(get_metagraph),
) -> BittensorValidatorAuth:
    """
    Verifies a request signed by a *validator* hotkey on the configured subnet.
    The signed message is: sha256(body).hexdigest() + "." + nonce + "." + timestamp

    Returns BittensorValidatorAuth on success; raises HTTPException otherwise.
    """

    _bind_entry_context(request, role="validator", x_signed_by=x_signed_by, x_nonce=x_nonce, x_ts=x_ts, x_version=x_version)

    if x_version != "2":
        await logger.awarning("auth_version_unsupported")
        _http_unauth("Unsupported X-Tao-Version")

    try:
        ts = int(x_ts)
    except Exception:
        await logger.awarning("auth_timestamp_invalid")
        _http_unauth("Invalid timestamp", status.HTTP_400_BAD_REQUEST)

    now_ms = round(time.time() * 1000)
    if ts + settings.allowed_clock_skew_ms < now_ms:
        await logger.awarning("auth_request_too_old", now_ms=now_ms, allowed_skew_ms=settings.allowed_clock_skew_ms)
        _http_unauth("Request too old", status.HTTP_400_BAD_REQUEST)

    try:
        uuid.UUID(x_nonce)
    except Exception:
        await logger.awarning("auth_nonce_invalid")
        _http_unauth("Invalid nonce", status.HTTP_400_BAD_REQUEST)

    if x_nonce in _nonce_cache:
        await logger.awarning("auth_replay_detected")
        _http_unauth("Replay detected", status.HTTP_400_BAD_REQUEST)
    _nonce_cache[x_nonce] = True

    body = await request.body()
    body_sha = sha256(body).hexdigest()
    message = f"{body_sha}.{x_nonce}.{ts}"

    try:
        kp = Keypair(ss58_address=x_signed_by)
        try:
            sig_bytes = base64.b64decode(x_sig, validate=True)
        except Exception:
            sig_bytes = bytes.fromhex(x_sig)
        ok = kp.verify(message, sig_bytes)
    except Exception:
        await logger.aexception("auth_sig_verify_error")
        _http_unauth("Signature verification error", status.HTTP_400_BAD_REQUEST)

    if not ok:
        await logger.awarning("auth_sig_mismatch", body_sha=body_sha)
        _http_unauth("Signature mismatch")

    try:
        uid = list(metagraph.hotkeys).index(x_signed_by)
    except ValueError:
        await logger.awarning("auth_hotkey_not_found", netuid=settings.netuid, network=settings.bittensor_network)
        _http_unauth(f"Hotkey {x_signed_by} not found on netuid {settings.netuid} ({settings.bittensor_network}).")

    try:
        permit = bool(metagraph.validator_permit[uid])
    except Exception:
        await logger.aerror("auth_metagraph_shape_error")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Metagraph missing validator_permit; contact operator.")

    if not permit:
        await logger.awarning("auth_validator_no_permit")
        _http_unauth("Hotkey is not a permitted validator on this subnet")

    stake = int(metagraph.alpha_stake[uid])
    structlog.contextvars.bind_contextvars(validator_uid=uid, validator_permit=permit, validator_stake=stake)

    if stake <= settings.min_validator_stake:
        await logger.awarning("auth_validator_low_stake", min_required=settings.min_validator_stake)
        _http_unauth("Hotkey does not have enough stake on this subnet")

    return BittensorValidatorAuth(validator_hotkey=x_signed_by, validator_uid=uid, permit=permit, stake=stake)
