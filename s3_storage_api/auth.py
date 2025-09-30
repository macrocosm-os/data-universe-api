import base64
from hashlib import sha256
import time
import uuid
from fastapi import Depends, HTTPException, Header, Request, status
from cachetools import TTLCache
from pydantic import BaseModel
from substrateinterface import Keypair
import bittensor as bt

from s3_storage_api.deps import get_metagraph
from s3_storage_api.settings import settings


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

    # Version
    if x_version != "2":
        _http_unauth("Unsupported X-Tao-Version")

    # Timestamp sanity + skew window
    try:
        ts = int(x_ts)
    except Exception:
        _http_unauth("Invalid timestamp", status.HTTP_400_BAD_REQUEST)
    now_ms = round(time.time() * 1000)
    if ts + settings.allowed_clock_skew_ms < now_ms:
        _http_unauth("Request too old", status.HTTP_400_BAD_REQUEST)

    # Nonce replay
    try:
        uuid.UUID(x_nonce)
    except Exception:
        _http_unauth("Invalid nonce", status.HTTP_400_BAD_REQUEST)
    if x_nonce in _nonce_cache:
        _http_unauth("Replay detected", status.HTTP_400_BAD_REQUEST)
    _nonce_cache[x_nonce] = True

    # Signature check (message = sha256(body) . nonce . timestamp)
    body = await request.body()
    message = f"{sha256(body).hexdigest()}.{x_nonce}.{ts}"

    try:
        kp = Keypair(ss58_address=x_signed_by)

        # Accept base64 or hex for convenience:
        try:
            sig_bytes = base64.b64decode(x_sig, validate=True)
        except Exception:
            # fallback: hex
            sig_bytes = bytes.fromhex(x_sig)

        ok = kp.verify(message, sig_bytes)
    except Exception:
        _http_unauth("Signature verification error", status.HTTP_400_BAD_REQUEST)

    if not ok:
        _http_unauth("Signature mismatch")

    # Chain check: only active MINERS on subnet 13 (not validators)
    try:
        uid = list(metagraph.hotkeys).index(x_signed_by)
    except ValueError:
        _http_unauth(
            f"Hotkey {x_signed_by} not found on netuid {settings.netuid} ({settings.bittensor_network})."
        )

    is_active_miner = x_signed_by in metagraph.hotkeys
    if not is_active_miner:
        _http_unauth("Hotkey is not an active miner on this subnet")

    # todo: rename
    incentive = float(metagraph.I[uid])

    # success: return metadata for downstream handlers if needed
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

    # Version
    if x_version != "2":
        _http_unauth("Unsupported X-Tao-Version")

    # Timestamp sanity + skew window
    try:
        ts = int(x_ts)
    except Exception:
        _http_unauth("Invalid timestamp", status.HTTP_400_BAD_REQUEST)

    now_ms = round(time.time() * 1000)
    if ts + settings.allowed_clock_skew_ms < now_ms:
        _http_unauth("Request too old", status.HTTP_400_BAD_REQUEST)

    # Nonce replay
    try:
        uuid.UUID(x_nonce)
    except Exception:
        _http_unauth("Invalid nonce", status.HTTP_400_BAD_REQUEST)

    if x_nonce in _nonce_cache:
        _http_unauth("Replay detected", status.HTTP_400_BAD_REQUEST)
    _nonce_cache[x_nonce] = True

    # Signature check (message = sha256(body) . nonce . timestamp)
    body = await request.body()
    message = f"{sha256(body).hexdigest()}.{x_nonce}.{ts}"

    try:
        kp = Keypair(ss58_address=x_signed_by)

        # Accept base64 or hex for convenience:
        try:
            sig_bytes = base64.b64decode(x_sig, validate=True)
        except Exception:
            # fallback: hex
            sig_bytes = bytes.fromhex(x_sig)

        ok = kp.verify(message, sig_bytes)
    except Exception:
        _http_unauth("Signature verification error", status.HTTP_400_BAD_REQUEST)
        
    if not ok:
        _http_unauth("Signature mismatch")

    # Metagraph checks: must be a known hotkey AND have validator permit on this subnet
    try:
        uid = list(metagraph.hotkeys).index(x_signed_by)
    except ValueError:
        _http_unauth(
            f"Hotkey {x_signed_by} not found on netuid {settings.netuid} ({settings.bittensor_network})."
        )

    # Validator-only gate
    try:
        permit = bool(metagraph.validator_permit[uid])
    except Exception:
        # Defensive: surface a clear server error if metagraph isn't shaped as expected
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Metagraph missing validator_permit; contact operator.",
        )

    if not permit:
        _http_unauth("Hotkey is not a permitted validator on this subnet")

    stake = int(metagraph.alpha_stake[uid])
    enough_stake = stake > settings.min_validator_stake
    if not enough_stake:
        _http_unauth("Hotkey does not have enough stake on this subnet")

    # success: return metadata for downstream handlers if needed
    return BittensorValidatorAuth(
        validator_hotkey=x_signed_by, validator_uid=uid, permit=permit, stake=stake
    )
