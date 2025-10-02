import os
import time
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List

import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import structlog

from s3_storage_api.deps import get_metagraph_syncer, get_subtensor
from s3_storage_api.middleware import RequestLoggingMiddleware
from s3_storage_api.utils.redis_utils import RedisClient
from s3_storage_api.utils.bt_utils import verify_signature, verify_validator_status
from s3_storage_api.utils.bt_utils_cached import (
    verify_signature_cached,
    verify_validator_status_cached,
)

from s3_storage_api.routes.on_demand import router as on_demand_router

from s3_storage_api.logging_config import configure_logging
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, REGISTRY

from s3_storage_api import deps

configure_logging()

load_dotenv()

from s3_storage_api.settings import settings

# Configuration
DAILY_LIMIT_PER_MINER = 2000
DAILY_LIMIT_PER_VALIDATOR = 10000
TOTAL_DAILY_LIMIT = 2000000

# Timeout configurations
VALIDATOR_VERIFICATION_TIMEOUT = 120  # 2 minutes
SIGNATURE_VERIFICATION_TIMEOUT = 60  # 1 minute
S3_OPERATION_TIMEOUT = 180  # 1 minute

# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger(__name__)

app = FastAPI(
    title="S3 Auth Server for Data Universe - 2min Timeout",
    description="Authentication server for S3 storage with 2-minute timeout protection",
    version="1.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)

instrumentator = Instrumentator(
    should_instrument_requests_inprogress=True,
    excluded_handlers={"/healthz", "/readyz"},
)

instrumentator.instrument(app)


@app.get("/metrics")
async def metrics(request: Request):
    if request.headers.get("Authorization") != f"Bearer  {settings.metrics_api_key}":
        raise HTTPException(status_code=401)
    
    # fastapi metrics + default process/GC metrics
    data = generate_latest(REGISTRY)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    tasks = [
        deps.get_rate_limiter().readyz(),
        deps.get_s3_on_demand_storage().readyz(),
        deps.readyz_pg(),
        deps.get_metagraph_syncer().readyz(),
    ]

    results = await asyncio.gather(*tasks)

    checks = {
        "redis": results[0] if not isinstance(results[0], Exception) else False,
        "s3": results[1] if not isinstance(results[1], Exception) else False,
        "pg": results[2] if not isinstance(results[2], Exception) else False,
        "metagraph": results[3] if not isinstance(results[3], Exception) else False,
    }

    all_ok = all(v is True for v in checks.values())

    return Response(
        status_code=200 if all_ok else 503,
        content={"status": "ok" if all_ok else "degraded", "checks": checks}.__str__(),
    )


redis_client = RedisClient()

# Initialize MetagraphSyncer for cached blockchain queries
logger.info("Initializing MetagraphSyncer...")
try:
    subtensor = get_subtensor()
    metagraph_syncer = get_metagraph_syncer()
    logger.info(
        f"MetagraphSyncer initialized successfully for netuid {settings.netuid}"
    )
except Exception as e:
    logger.error(f"Failed to initialize MetagraphSyncer: {str(e)}")
    logger.error("Falling back to original bt_utils functions")
    metagraph_syncer = None
    subtensor = None

s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.aws_access_key,
    aws_secret_access_key=settings.aws_secret_key,
    endpoint_url="https://nyc3.digitaloceanspaces.com",
    config=Config(
        signature_version="s3v4",
        retries={"max_attempts": 3, "mode": "adaptive"},
        connect_timeout=10,
        read_timeout=30,
    ),
)


class MinerFolderAccessRequest(BaseModel):
    coldkey: str
    hotkey: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    signature: str  # HEX string
    expiry: Optional[int] = None


class ValidatorAccessRequest(BaseModel):
    hotkey: str
    signature: str  # HEX string
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    expiry: Optional[int] = None
    miner_hotkey: Optional[str] = None


class FilePresignedRequest(BaseModel):
    hotkey: str
    signature: str  # HEX string
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    miner_hotkey: str  # Required: which miner's files to access
    file_keys: List[str]  # List of specific file keys to generate URLs for
    expiry_hours: Optional[int] = 1  # Default 1 hour expiry


# Lightweight monitoring - just counters
class SimpleMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.requests = 0
        self.errors = 0
        self.timeouts = 0

    def count_request(self, error=False, timeout=False):
        self.requests += 1
        if error:
            self.errors += 1
        if timeout:
            self.timeouts += 1

    def get_stats(self):
        uptime = time.time() - self.start_time
        return {
            "uptime_hours": round(uptime / 3600, 2),
            "total_requests": self.requests,
            "total_errors": self.errors,
            "total_timeouts": self.timeouts,
            "error_rate": self.errors / self.requests if self.requests > 0 else 0,
            "timeout_rate": self.timeouts / self.requests if self.requests > 0 else 0,
            "requests_per_hour": self.requests / (uptime / 3600) if uptime > 0 else 0,
        }


monitor = SimpleMonitor()


# Simple middleware to count requests
@app.middleware("http")
async def count_requests(request: Request, call_next):
    try:
        response = await call_next(request)
        monitor.count_request(error=response.status_code >= 400)
        return response
    except Exception as e:
        monitor.count_request(error=True)
        raise


# Optimized validation functions using cached metagraph
async def verify_validator_status_with_timeout(
    hotkey: str, netuid: int, network: str
) -> bool:
    """Verify validator status with cached metagraph and timeout fallback"""
    try:
        # Try cached version first (should be ~1ms)
        if metagraph_syncer is not None:
            try:
                metagraph = metagraph_syncer.get_metagraph(netuid)
                return verify_validator_status_cached(hotkey, metagraph)
            except Exception as e:
                logger.warning(
                    f"Cached validator verification failed for {hotkey}: {str(e)}, falling back to blockchain"
                )

        # Fallback to original method with timeout protection
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, verify_validator_status, hotkey, netuid, network
            ),
            timeout=VALIDATOR_VERIFICATION_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.error(f"Validator verification timeout for {hotkey}")
        monitor.count_request(timeout=True)
        return False
    except Exception as e:
        logger.error(f"Validator verification error for {hotkey}: {str(e)}")
        return False


async def verify_signature_with_timeout(
    commitment: str, signature: str, hotkey: str, netuid: int, network: str
) -> bool:
    """Verify signature with cached metagraph and timeout fallback"""
    try:
        # Try cached version first (should be ~1ms)
        if metagraph_syncer is not None:
            try:
                metagraph = metagraph_syncer.get_metagraph(netuid)
                return verify_signature_cached(commitment, signature, hotkey, metagraph)
            except Exception as e:
                logger.warning(
                    f"Cached signature verification failed for {hotkey}: {str(e)}, falling back to blockchain"
                )

        # Fallback to original method with timeout protection
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, verify_signature, commitment, signature, hotkey, netuid, network
            ),
            timeout=SIGNATURE_VERIFICATION_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.error(f"Signature verification timeout for {hotkey}")
        monitor.count_request(timeout=True)
        return False
    except Exception as e:
        logger.error(f"Signature verification error for {hotkey}: {str(e)}")
        return False


def check_rate_limit(key: str, daily_limit: int) -> Tuple[bool, Optional[str]]:
    today = time.strftime("%Y-%m-%d")
    global_key = f"GLOBAL:{today}"
    global_count = redis_client.get_counter(global_key)
    if global_count >= TOTAL_DAILY_LIMIT:
        return False, "Global request limit reached."
    entity_key = f"{key}:{today}"
    entity_count = redis_client.get_counter(entity_key)
    if entity_count >= daily_limit:
        return False, f"Daily limit of {daily_limit} exceeded."
    redis_client.increment_counter(entity_key)
    redis_client.increment_counter(global_key)
    return True, None


def generate_folder_upload_policy(
    bucket: str, folder_prefix: str, expiry_hours: int = 3
) -> Dict:
    """Generate upload policy for job-based folder structure"""
    fields = {"acl": "private", "x-amz-storage-class": "STANDARD"}

    conditions = [
        {"acl": "private"},
        ["starts-with", "$key", folder_prefix],
        ["content-length-range", 1024, 5368709120],
        {"x-amz-storage-class": "STANDARD"},
    ]

    post = s3_client.generate_presigned_post(
        Bucket=bucket,
        Key=f"{folder_prefix}${{filename}}",
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=expiry_hours * 3600,
    )

    post["url"] = f"https://{bucket}.nyc3.digitaloceanspaces.com"
    return post


def generate_validator_access_urls(
    validator_hotkey: str, expiry_hours: int = 24
) -> Dict:
    """Generate validator access URLs for job-based structure"""
    expiration = datetime.utcnow() + timedelta(hours=expiry_hours)
    expiry_seconds = expiry_hours * 3600
    urls = {"global": {}, "miners": {}}

    # Global listing - all data (with data/ prefix)
    urls["global"]["list_all_data"] = s3_client.generate_presigned_url(
        "list_objects_v2",
        Params={"Bucket": settings.s3_bucket, "Prefix": "data/hotkey="},
        ExpiresIn=expiry_seconds,
    )

    # List all miners (hotkeys) (with data/ prefix)
    urls["miners"]["list_all_miners"] = s3_client.generate_presigned_url(
        "list_objects_v2",
        Params={
            "Bucket": settings.s3_bucket,
            "Prefix": "data/hotkey=",
            "Delimiter": "/",
        },
        ExpiresIn=expiry_seconds,
    )

    return {
        "bucket": settings.s3_bucket,
        "region": settings.s3_region,
        "validator_hotkey": validator_hotkey,
        "expiry": expiration.isoformat(),
        "expiry_seconds": expiry_seconds,
        "urls": urls,
        "structure_info": {
            "folder_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
            "description": "Job-based folder structure with explicit hotkey and job_id labels under data/ prefix",
        },
    }


app.include_router(on_demand_router)


@app.post("/get-folder-access")
async def get_folder_access(request: MinerFolderAccessRequest):
    try:
        coldkey, hotkey = request.coldkey, request.hotkey
        timestamp = request.timestamp
        expiry = request.expiry or (timestamp + 86400)
        signature = request.signature

        # Folder path with data/ prefix
        folder_path = f"data/hotkey={hotkey}/"

        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_MINER)
        if not is_allowed:
            raise HTTPException(status_code=429, detail=msg)

        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        commitment = f"s3:data:access:{coldkey}:{hotkey}:{timestamp}"

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(
            commitment, signature, hotkey, settings.netuid, settings.bittensor_network
        )
        if not signature_valid:
            logger.warning(f"MINER SIGNATURE FAILED: {hotkey} (coldkey: {coldkey})")
            raise HTTPException(status_code=401, detail="Invalid signature")

        policy = generate_folder_upload_policy(
            settings.s3_bucket, folder_path, expiry_hours=24
        )
        list_url = s3_client.generate_presigned_url(
            "list_objects_v2",
            Params={"Bucket": settings.s3_bucket, "Prefix": folder_path},
            ExpiresIn=60 * 60 * 3,
        )

        return {
            "folder": folder_path,
            "url": policy["url"],
            "fields": policy["fields"],
            "expiry": datetime.fromtimestamp(expiry).isoformat(),
            "list_url": list_url,
            "structure_info": {
                "folder_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
                "description": "Upload files to job_id folders within your hotkey directory under data/ prefix",
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_folder_access: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-validator-access")
async def get_validator_access(request: ValidatorAccessRequest):
    try:
        hotkey, timestamp = request.hotkey, request.timestamp
        signature = request.signature
        expiry = request.expiry or (timestamp + 86400)

        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_VALIDATOR)
        if not is_allowed:
            raise HTTPException(status_code=429, detail=msg)

        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        commitment = f"s3:validator:access:{timestamp}"

        # Use timeout-protected validator verification (2 minutes)
        validator_status = await verify_validator_status_with_timeout(
            hotkey, settings.netuid, settings.bittensor_network
        )
        if not validator_status:
            logger.warning(f"VALIDATOR ACCESS DENIED: {hotkey} - not a validator")
            raise HTTPException(status_code=401, detail="You are not validator")

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(
            commitment, signature, hotkey, settings.netuid, settings.bittensor_network
        )
        if not signature_valid:
            logger.warning(f"VALIDATOR SIGNATURE FAILED: {hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature")

        return generate_validator_access_urls(hotkey, expiry_hours=24)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_validator_access: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-folder-presigned-urls")
async def get_folder_presigned_urls(request: ValidatorAccessRequest):
    """Generate presigned URLs for job folders to enable content validation"""
    try:
        hotkey, timestamp = request.hotkey, request.timestamp
        signature = request.signature
        expiry = request.expiry or (timestamp + 86400)

        # Rate limiting check
        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_VALIDATOR)
        if not is_allowed:
            raise HTTPException(status_code=429, detail=msg)

        # Timestamp validation
        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        # Commitment for folder access
        commitment = f"s3:validator:folders:{hotkey}:{timestamp}"

        # Use timeout-protected validator verification (2 minutes)
        validator_status = await verify_validator_status_with_timeout(
            hotkey, settings.netuid, settings.bittensor_network
        )
        if not validator_status:
            logger.warning(f"VALIDATOR ACCESS DENIED: {hotkey} - not a validator")
            raise HTTPException(status_code=401, detail="You are not validator")

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(
            commitment, signature, hotkey, settings.netuid, settings.bittensor_network
        )
        if not signature_valid:
            logger.warning(f"VALIDATOR SIGNATURE FAILED: {hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature")

        # List all miner folders with timeout protection
        try:
            response = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: s3_client.list_objects_v2(
                        Bucket=settings.s3_bucket, Prefix="data/hotkey=", Delimiter="/"
                    ),
                ),
                timeout=S3_OPERATION_TIMEOUT,
            )

            miner_folders = []
            if "CommonPrefixes" in response:
                for prefix in response["CommonPrefixes"]:
                    miner_path = prefix["Prefix"]
                    # Extract miner hotkey from path like 'data/hotkey=5F3...xyz/'
                    if "hotkey=" in miner_path:
                        miner_hotkey = miner_path.split("hotkey=")[1].rstrip("/")
                        miner_folders.append(
                            {"miner_hotkey": miner_hotkey, "path": miner_path}
                        )

            # Generate presigned URLs for each miner's job folders
            folder_urls = {}
            expiry_seconds = 3 * 3600  # 3 hours

            for miner_info in miner_folders:
                miner_hotkey = miner_info["miner_hotkey"]
                miner_path = miner_info["path"]

                # List job folders for this miner with timeout protection
                job_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: s3_client.list_objects_v2(
                            Bucket=settings.s3_bucket, Prefix=miner_path, Delimiter="/"
                        ),
                    ),
                    timeout=S3_OPERATION_TIMEOUT,
                )

                job_folders = []
                if "CommonPrefixes" in job_response:
                    for job_prefix in job_response["CommonPrefixes"]:
                        job_path = job_prefix["Prefix"]
                        # Extract job_id from path like 'data/hotkey=5F3...xyz/job_id=default_0/'
                        if "job_id=" in job_path:
                            job_id = job_path.split("job_id=")[1].rstrip("/")

                            # Generate presigned URL for this job folder
                            presigned_url = s3_client.generate_presigned_url(
                                "list_objects_v2",
                                Params={
                                    "Bucket": settings.s3_bucket,
                                    "Prefix": job_path,
                                },
                                ExpiresIn=expiry_seconds,
                            )

                            job_folders.append(
                                {
                                    "job_id": job_id,
                                    "path": job_path,
                                    "presigned_url": presigned_url,
                                }
                            )

                if job_folders:
                    folder_urls[miner_hotkey] = {
                        "miner_path": miner_path,
                        "job_folders": job_folders,
                        "total_jobs": len(job_folders),
                    }

            return {
                "validator_hotkey": hotkey,
                "bucket": settings.s3_bucket,
                "expiry_seconds": expiry_seconds,
                "expiry_time": datetime.fromtimestamp(
                    timestamp + expiry_seconds
                ).isoformat(),
                "total_miners": len(folder_urls),
                "folder_urls": folder_urls,
                "commitment_used": commitment,
                "usage_info": {
                    "description": "Use presigned URLs to list files in job folders for content validation",
                    "folder_structure": "data/hotkey={miner_hotkey}/job_id={job_id}/",
                    "validation_flow": [
                        "1. Select random miners and jobs from the response",
                        "2. Use presigned URLs to list parquet files in job folders",
                        "3. Download individual files using additional presigned URLs",
                        "4. Validate content with DuckDB queries for duplicates/quality",
                    ],
                },
            }

        except asyncio.TimeoutError:
            logger.error(f"S3 operations timeout for folder listing")
            monitor.count_request(timeout=True)
            raise HTTPException(
                status_code=504, detail="S3 operation timeout - try again"
            )
        except Exception as s3_error:
            logger.error(f"S3 listing error: {str(s3_error)}")
            raise HTTPException(
                status_code=500, detail=f"S3 access error: {str(s3_error)}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Folder presigned URLs error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-file-presigned-urls")
async def get_file_presigned_urls(request: FilePresignedRequest):
    """Generate presigned URLs for individual files using aioboto3 - PER MINER REQUEST"""
    try:
        hotkey, timestamp = request.hotkey, request.timestamp
        signature = request.signature
        miner_hotkey = request.miner_hotkey
        file_keys = request.file_keys
        expiry_hours = request.expiry_hours or 1
        expiry = timestamp + (expiry_hours * 3600)

        print(
            f"🔧 [FILE-PRESIGNED] Starting file presigned URLs for miner: {miner_hotkey[:10]}..."
        )
        print(
            f"🔧 [FILE-PRESIGNED] Validator: {hotkey[:10]}... requesting {len(file_keys)} files"
        )

        # Rate limiting check
        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_VALIDATOR)
        if not is_allowed:
            print(f"🔧 [FILE-PRESIGNED] Rate limit failed: {msg}")
            raise HTTPException(status_code=429, detail=msg)

        # Timestamp validation
        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            print(f"🔧 [FILE-PRESIGNED] Timestamp validation failed")
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        # Commitment for file access
        commitment = f"s3:validator:files:{miner_hotkey}:{hotkey}:{timestamp}"
        print(f"🔧 [FILE-PRESIGNED] Commitment: {commitment}")

        # Validator verification
        print(f"🔧 [FILE-PRESIGNED] Starting validator verification...")
        validator_status = await verify_validator_status_with_timeout(
            hotkey, settings.netuid, settings.bittensor_network
        )
        if not validator_status:
            logger.warning(f"VALIDATOR ACCESS DENIED: {hotkey} - not a validator")
            print(f"🔧 [FILE-PRESIGNED] Validator verification FAILED")
            raise HTTPException(status_code=401, detail="You are not validator")
        print(f"🔧 [FILE-PRESIGNED] Validator verification PASSED")

        # Signature verification
        print(f"🔧 [FILE-PRESIGNED] Starting signature verification...")
        signature_valid = await verify_signature_with_timeout(
            commitment, signature, hotkey, settings.netuid, settings.bittensor_network
        )
        if not signature_valid:
            logger.warning(f"VALIDATOR SIGNATURE FAILED: {hotkey}")
            print(f"🔧 [FILE-PRESIGNED] Signature verification FAILED")
            raise HTTPException(status_code=401, detail="Invalid signature")
        print(f"🔧 [FILE-PRESIGNED] Signature verification PASSED")

        # Validate that files belong to the specified miner
        print(f"🔧 [FILE-PRESIGNED] Validating files belong to miner...")
        miner_prefix = f"data/hotkey={miner_hotkey}/"
        valid_files = []
        invalid_files = []

        for file_key in file_keys:
            if file_key.startswith(miner_prefix):
                valid_files.append(file_key)
            else:
                invalid_files.append(file_key)

        print(
            f"🔧 [FILE-PRESIGNED] Valid files: {len(valid_files)}, Invalid files: {len(invalid_files)}"
        )

        if len(valid_files) == 0:
            raise HTTPException(
                status_code=400, detail="No valid files for specified miner"
            )

        # Generate presigned URLs using aiobotocore (like your example)
        print(f"🔧 [FILE-PRESIGNED] Generating presigned URLs with aiobotocore...")
        file_urls = {}
        failed_files = []

        try:
            from aiobotocore.session import get_session

            session = get_session()
            async with session.create_client(
                "s3",
                region_name=settings.s3_region,
                endpoint_url="https://nyc3.digitaloceanspaces.com",
                aws_access_key_id=settings.aws_access_key,
                aws_secret_access_key=settings.aws_secret_key,
                config=Config(signature_version="s3v4", max_pool_connections=50),
            ) as s3_client:

                for i, file_key in enumerate(valid_files):
                    try:
                        print(
                            f"🔧 [FILE-PRESIGNED] Generating URL {i+1}/{len(valid_files)}: {file_key.split('/')[-1]}"
                        )

                        presigned_url = await s3_client.generate_presigned_url(
                            ClientMethod="get_object",
                            Params={"Bucket": settings.s3_bucket, "Key": file_key},
                            ExpiresIn=expiry_hours * 3600,
                        )

                        file_urls[file_key] = {
                            "presigned_url": presigned_url,
                            "file_key": file_key,
                            "expires_in_seconds": expiry_hours * 3600,
                            "file_name": file_key.split("/")[-1],
                        }

                    except Exception as file_error:
                        logger.error(
                            f"Failed to generate presigned URL for {file_key}: {str(file_error)}"
                        )
                        print(
                            f"🔧 [FILE-PRESIGNED] ERROR generating URL for {file_key}: {str(file_error)}"
                        )
                        failed_files.append(
                            {"file_key": file_key, "error": str(file_error)}
                        )

            print(
                f"🔧 [FILE-PRESIGNED] Successfully generated {len(file_urls)} presigned URLs"
            )

            return {
                "validator_hotkey": hotkey,
                "miner_hotkey": miner_hotkey,
                "bucket": settings.s3_bucket,
                "expiry_hours": expiry_hours,
                "expiry_time": datetime.fromtimestamp(
                    timestamp + (expiry_hours * 3600)
                ).isoformat(),
                "total_requested_files": len(file_keys),
                "valid_files_count": len(valid_files),
                "invalid_files_count": len(invalid_files),
                "successful_urls": len(file_urls),
                "failed_files": len(failed_files),
                "file_urls": file_urls,
                "invalid_files": invalid_files,
                "failed_files_details": failed_files,
                "commitment_used": commitment,
                "usage_info": {
                    "description": "Use presigned URLs to download individual files for content validation",
                    "miner_validation_flow": [
                        "1. Get list of files for miner using /get-folder-presigned-urls",
                        "2. Select specific files to validate",
                        "3. Request presigned URLs for those files using this endpoint",
                        "4. Download files using presigned URLs",
                        "5. Analyze parquet content with pandas/DuckDB",
                    ],
                },
            }

        except Exception as s3_error:
            logger.error(f"aioboto3 S3 error: {str(s3_error)}")
            print(f"🔧 [FILE-PRESIGNED] S3 ERROR: {str(s3_error)}")
            raise HTTPException(
                status_code=500,
                detail=f"S3 presigned URL generation failed: {str(s3_error)}",
            )

    except HTTPException:
        print(f"🔧 [FILE-PRESIGNED] HTTP Exception raised")
        raise
    except Exception as e:
        logger.error(f"File presigned URLs error: {str(e)}")
        print(f"🔧 [FILE-PRESIGNED] UNEXPECTED ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-miner-specific-access")
async def get_miner_specific_access(request: ValidatorAccessRequest):
    """Get presigned URL for a specific miner's data with 2-minute timeout protection"""
    try:
        hotkey, timestamp = request.hotkey, request.timestamp
        signature = request.signature
        miner_hotkey = request.miner_hotkey
        expiry = request.expiry or (timestamp + 86400)

        if not miner_hotkey:
            raise HTTPException(status_code=400, detail="miner_hotkey is required")

        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_VALIDATOR)
        if not is_allowed:
            raise HTTPException(status_code=429, detail=msg)

        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        commitment = f"s3:validator:miner:{miner_hotkey}:{timestamp}"

        # Use timeout-protected validator verification (2 minutes)
        validator_status = await verify_validator_status_with_timeout(
            hotkey, settings.netuid, settings.bittensor_network
        )
        if not validator_status:
            logger.warning(
                f"VALIDATOR ACCESS DENIED: {hotkey} - not a validator (requested miner: {miner_hotkey})"
            )
            raise HTTPException(status_code=401, detail="You are not validator")

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(
            commitment, signature, hotkey, settings.netuid, settings.bittensor_network
        )
        if not signature_valid:
            logger.warning(
                f"VALIDATOR SIGNATURE FAILED: {hotkey} (requested miner: {miner_hotkey})"
            )
            raise HTTPException(status_code=401, detail="Invalid signature")

        # Generate presigned URL with specific miner prefix
        miner_prefix = f"data/hotkey={miner_hotkey}/"

        try:
            presigned_url = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: s3_client.generate_presigned_url(
                        "list_objects_v2",
                        Params={
                            "Bucket": settings.s3_bucket,
                            "Prefix": miner_prefix,
                            "MaxKeys": 10000,
                        },
                        ExpiresIn=3 * 3600,
                    ),
                ),
                timeout=S3_OPERATION_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error(f"S3 presigned URL generation timeout for {miner_hotkey}")
            monitor.count_request(timeout=True)
            raise HTTPException(
                status_code=504, detail="S3 operation timeout - try again"
            )

        return {
            "bucket": settings.s3_bucket,
            "region": settings.s3_region,
            "miner_hotkey": miner_hotkey,
            "miner_url": presigned_url,
            "prefix": miner_prefix,
            "expiry": datetime.fromtimestamp(expiry).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_miner_specific_access: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthcheck")
async def health_check():
    # Quick S3 test with timeout
    s3_ok = True
    s3_latency = 0
    start_time = time.time()

    try:
        await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, lambda: s3_client.head_bucket(Bucket=settings.s3_bucket)
            ),
            timeout=5.0,
        )
        s3_latency = time.time() - start_time
    except Exception as e:
        s3_ok = False
        logger.error(f"S3 health check failed: {str(e)}")

    # Quick Redis test
    redis_ok = True
    try:
        redis_client.set("ping", "pong", expire=1)
        redis_ok = redis_client.get("ping") is not None
    except Exception as e:
        redis_ok = False
        logger.error(f"Redis health check failed: {str(e)}")

    stats = monitor.get_stats()

    # Check metagraph syncer status
    metagraph_ok = False
    metagraph_info = {}
    if metagraph_syncer is not None:
        try:
            metagraph = metagraph_syncer.get_metagraph(settings.netuid)
            metagraph_ok = True
            metagraph_info = {
                "enabled": True,
                "netuid": settings.netuid,
                "sync_interval": settings.metagraph_sync_interval,
                "hotkeys_count": len(metagraph.hotkeys) if metagraph else 0,
                "last_sync": "recent" if metagraph else "unknown",
            }
        except Exception as e:
            metagraph_info = {"enabled": True, "error": str(e)}
    else:
        metagraph_info = {
            "enabled": False,
            "reason": "Initialization failed, using fallback methods",
        }

    return {
        "status": "ok" if s3_ok and redis_ok else "degraded",
        "timestamp": time.time(),
        "bucket": settings.s3_bucket,
        "region": settings.s3_region,
        "folder_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
        "s3_ok": s3_ok,
        "s3_latency_ms": round(s3_latency * 1000, 2),
        "redis_ok": redis_ok,
        "metagraph_syncer": metagraph_info,
        "stats": stats,
        "timeouts": {
            "validator_verification": f"{VALIDATOR_VERIFICATION_TIMEOUT}s",
            "signature_verification": f"{SIGNATURE_VERIFICATION_TIMEOUT}s",
            "s3_operations": f"{S3_OPERATION_TIMEOUT}s",
        },
    }


@app.get("/commitment-formats")
async def commitment_formats():
    return {
        "miner_format": "s3:data:access:{coldkey}:{hotkey}:{timestamp}",
        "validator_format": "s3:validator:access:{timestamp}",
        "miner_specific_format": "s3:validator:miner:{miner_hotkey}:{timestamp}",
        "folder_presigned_format": "s3:validator:folders:{hotkey}:{timestamp}",
        "file_presigned_format": "s3:validator:files:{miner_hotkey}:{hotkey}:{timestamp}",
        "example_miner": "s3:data:access:5F3...coldkey:5H2...hotkey:1682345678",
        "example_validator": "s3:validator:access:1682345678",
        "example_miner_specific": "s3:validator:miner:5F3...miner_hotkey:1682345678",
        "example_folder_presigned": "s3:validator:folders:5F3...validator_hotkey:1682345678",
        "example_file_presigned": "s3:validator:files:5F3...miner_hotkey:5F3...validator_hotkey:1682345678",
        "folder_structure": {
            "new_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
            "description": "Job-based folder structure with explicit labels under data/ prefix",
            "example_paths": [
                "data/hotkey=5F3...xyz/job_id=default_0/data_20250620_143052_150.parquet",
                "data/hotkey=5F3...xyz/job_id=crawler-7-h4rptebsja6qbdmocrt98/data_20250620_143055_67.parquet",
            ],
        },
        "instructions": "1. Generate timestamp\n2. Sign commitment\n3. Make API request\n4. Upload to job_id folders with explicit labels under data/ prefix",
        "timeout_protection": {
            "validator_verification": f"{VALIDATOR_VERIFICATION_TIMEOUT} seconds",
            "signature_verification": f"{SIGNATURE_VERIFICATION_TIMEOUT} seconds",
            "description": "All validation operations have timeout protection to prevent hanging",
        },
    }


@app.get("/structure-info")
async def structure_info():
    """Endpoint to get information about the new folder structure"""
    return {
        "folder_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
        "changes": {
            "old_structure": "hotkey={hotkey_id}/job_id={job_id}/",
            "new_structure": "data/hotkey={hotkey_id}/job_id={job_id}/",
            "benefits": [
                "Explicit hotkey and job_id labeling",
                "Cleaner path structure with data/ prefix",
                "Better organization for miners and validators",
                "2-minute timeout protection for validator verification",
                "Comprehensive error handling and monitoring",
            ],
        },
        "example_paths": [
            "data/hotkey=5F3...xyz/job_id=default_0/data_20250620_143052_150.parquet",
            "data/hotkey=5F3...xyz/job_id=crawler-7-h4rptebsja6qbdmocrt98/data_20250620_143055_67.parquet",
        ],
        "upload_flow": [
            "1. Get job IDs from Gravity",
            "2. Request S3 credentials via API",
            "3. Upload files to data/hotkey={hotkey_id}/job_id={job_id}/ folders",
            "4. Each job gets its own folder with explicit labels under data/ prefix",
        ],
        "timeout_protection": {
            "validator_verification": f"{VALIDATOR_VERIFICATION_TIMEOUT} seconds (2 minutes)",
            "signature_verification": f"{SIGNATURE_VERIFICATION_TIMEOUT} seconds",
            "s3_operations": f"{S3_OPERATION_TIMEOUT} seconds",
            "description": "All operations have timeout protection to prevent server hanging",
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        workers=max(2, (os.cpu_count() or 2) // 2),  # e.g. 2–4
        loop="asyncio",
        timeout_keep_alive=180,
        timeout_graceful_shutdown=30,
        access_log=False,
    )
