import os
import time
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from s3_storage_api.utils.redis_utils import RedisClient
from s3_storage_api.utils.bt_utils import verify_signature, verify_validator_status

load_dotenv()

# Configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'data-universe-storage')
S3_REGION = os.getenv('S3_REGION', 'nyc3')
AWS_ACCESS_KEY = os.getenv("DO_SPACES_KEY", "your-access-key-here")
AWS_SECRET_KEY = os.getenv("DO_SPACES_SECRET", "your-secret-key-here")
SERVER_PORT = int(os.getenv('PORT', '8501'))
BT_NETWORK = os.getenv("BT_NETWORK", "finney")
NET_UID = int(os.getenv('NET_UID', '13'))
COMMITMENT_VALIDITY_SECONDS = 60

DAILY_LIMIT_PER_MINER = 20
DAILY_LIMIT_PER_VALIDATOR = 10000
TOTAL_DAILY_LIMIT = 200000

# Timeout configurations
VALIDATOR_VERIFICATION_TIMEOUT = 120  # 2 minutes
SIGNATURE_VERIFICATION_TIMEOUT = 60  # 1 minute
S3_OPERATION_TIMEOUT = 60  # 1 minute

# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

redis_client = RedisClient()

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    endpoint_url="https://nyc3.digitaloceanspaces.com",
    config=Config(
        signature_version='s3v4',
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        connect_timeout=10,
        read_timeout=30
    )
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
            'uptime_hours': round(uptime / 3600, 2),
            'total_requests': self.requests,
            'total_errors': self.errors,
            'total_timeouts': self.timeouts,
            'error_rate': self.errors / self.requests if self.requests > 0 else 0,
            'timeout_rate': self.timeouts / self.requests if self.requests > 0 else 0,
            'requests_per_hour': self.requests / (uptime / 3600) if uptime > 0 else 0
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


# Timeout-protected validation functions
async def verify_validator_status_with_timeout(hotkey: str, netuid: int, network: str) -> bool:
    """Verify validator status with 2-minute timeout protection"""
    try:
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, verify_validator_status, hotkey, netuid, network
            ),
            timeout=VALIDATOR_VERIFICATION_TIMEOUT
        )
    except asyncio.TimeoutError:
        logger.error(f"Validator verification timeout for {hotkey}")
        monitor.count_request(timeout=True)
        return False
    except Exception as e:
        logger.error(f"Validator verification error for {hotkey}: {str(e)}")
        return False


async def verify_signature_with_timeout(commitment: str, signature: str, hotkey: str, netuid: int,
                                        network: str) -> bool:
    """Verify signature with timeout protection"""
    try:
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, verify_signature, commitment, signature, hotkey, netuid, network
            ),
            timeout=SIGNATURE_VERIFICATION_TIMEOUT
        )
    except asyncio.TimeoutError:
        logger.error(f"Signature verification timeout for {hotkey}")
        monitor.count_request(timeout=True)
        return False
    except Exception as e:
        logger.error(f"Signature verification error for {hotkey}: {str(e)}")
        return False


def check_rate_limit(key: str, daily_limit: int) -> Tuple[bool, Optional[str]]:
    today = time.strftime('%Y-%m-%d')
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


def generate_folder_upload_policy(bucket: str, folder_prefix: str, expiry_hours: int = 3) -> Dict:
    """Generate upload policy for job-based folder structure"""
    fields = {
        "acl": "private",
        "x-amz-storage-class": "STANDARD"
    }

    conditions = [
        {"acl": "private"},
        ["starts-with", "$key", folder_prefix],
        ["content-length-range", 1024, 5368709120],
        {"x-amz-storage-class": "STANDARD"}
    ]

    post = s3_client.generate_presigned_post(
        Bucket=bucket,
        Key=f"{folder_prefix}${{filename}}",
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=expiry_hours * 3600
    )

    post["url"] = f"https://{bucket}.nyc3.digitaloceanspaces.com"
    return post


def generate_validator_access_urls(validator_hotkey: str, expiry_hours: int = 24) -> Dict:
    """Generate validator access URLs for job-based structure"""
    expiration = datetime.utcnow() + timedelta(hours=expiry_hours)
    expiry_seconds = expiry_hours * 3600
    urls = {'global': {}, 'miners': {}}

    # Global listing - all data (with data/ prefix)
    urls['global']['list_all_data'] = s3_client.generate_presigned_url(
        'list_objects_v2',
        Params={'Bucket': S3_BUCKET, 'Prefix': 'data/hotkey='},
        ExpiresIn=expiry_seconds
    )

    # List all miners (hotkeys) (with data/ prefix)
    urls['miners']['list_all_miners'] = s3_client.generate_presigned_url(
        'list_objects_v2',
        Params={'Bucket': S3_BUCKET, 'Prefix': 'data/hotkey=', 'Delimiter': '/'},
        ExpiresIn=expiry_seconds
    )

    return {
        'bucket': S3_BUCKET,
        'region': S3_REGION,
        'validator_hotkey': validator_hotkey,
        'expiry': expiration.isoformat(),
        'expiry_seconds': expiry_seconds,
        'urls': urls,
        'structure_info': {
            'folder_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
            'description': 'Job-based folder structure with explicit hotkey and job_id labels under data/ prefix'
        }
    }


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
        signature_valid = await verify_signature_with_timeout(commitment, signature, hotkey, NET_UID, BT_NETWORK)
        if not signature_valid:
            raise HTTPException(status_code=401, detail="Invalid signature")

        policy = generate_folder_upload_policy(S3_BUCKET, folder_path, expiry_hours=24)
        list_url = s3_client.generate_presigned_url(
            'list_objects_v2',
            Params={'Bucket': S3_BUCKET, 'Prefix': folder_path},
            ExpiresIn=60 * 60 * 3
        )

        return {
            'folder': folder_path,
            'url': policy['url'],
            'fields': policy['fields'],
            'expiry': datetime.fromtimestamp(expiry).isoformat(),
            'list_url': list_url,
            'structure_info': {
                'folder_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
                'description': 'Upload files to job_id folders within your hotkey directory under data/ prefix'
            }
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
        validator_status = await verify_validator_status_with_timeout(hotkey, NET_UID, BT_NETWORK)
        if not validator_status:
            raise HTTPException(status_code=401, detail="You are not validator")

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(commitment, signature, hotkey, NET_UID, BT_NETWORK)
        if not signature_valid:
            raise HTTPException(status_code=401, detail="Invalid signature")

        return generate_validator_access_urls(hotkey, expiry_hours=24)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_validator_access: {str(e)}")
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
        validator_status = await verify_validator_status_with_timeout(hotkey, NET_UID, BT_NETWORK)
        if not validator_status:
            raise HTTPException(status_code=401, detail="You are not validator")

        # Use timeout-protected signature verification
        signature_valid = await verify_signature_with_timeout(commitment, signature, hotkey, NET_UID, BT_NETWORK)
        if not signature_valid:
            raise HTTPException(status_code=401, detail="Invalid signature")

        # Generate presigned URL with specific miner prefix
        miner_prefix = f"data/hotkey={miner_hotkey}/"

        try:
            presigned_url = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: s3_client.generate_presigned_url(
                        'list_objects_v2',
                        Params={
                            'Bucket': S3_BUCKET,
                            'Prefix': miner_prefix,
                            'MaxKeys': 10000
                        },
                        ExpiresIn=3 * 3600
                    )
                ),
                timeout=S3_OPERATION_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"S3 presigned URL generation timeout for {miner_hotkey}")
            monitor.count_request(timeout=True)
            raise HTTPException(status_code=504, detail="S3 operation timeout - try again")

        return {
            'bucket': S3_BUCKET,
            'region': S3_REGION,
            'miner_hotkey': miner_hotkey,
            'miner_url': presigned_url,
            'prefix': miner_prefix,
            'expiry': datetime.fromtimestamp(expiry).isoformat(),
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
                None,
                lambda: s3_client.head_bucket(Bucket=S3_BUCKET)
            ),
            timeout=5.0
        )
        s3_latency = time.time() - start_time
    except Exception as e:
        s3_ok = False
        logger.error(f"S3 health check failed: {str(e)}")

    # Quick Redis test
    redis_ok = True
    try:
        redis_client.set('ping', 'pong', expire=1)
        redis_ok = redis_client.get('ping') is not None
    except Exception as e:
        redis_ok = False
        logger.error(f"Redis health check failed: {str(e)}")

    stats = monitor.get_stats()

    return {
        'status': 'ok' if s3_ok and redis_ok else 'degraded',
        'timestamp': time.time(),
        'bucket': S3_BUCKET,
        'region': S3_REGION,
        'folder_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
        's3_ok': s3_ok,
        's3_latency_ms': round(s3_latency * 1000, 2),
        'redis_ok': redis_ok,
        'stats': stats,
        'timeouts': {
            'validator_verification': f"{VALIDATOR_VERIFICATION_TIMEOUT}s",
            'signature_verification': f"{SIGNATURE_VERIFICATION_TIMEOUT}s",
            's3_operations': f"{S3_OPERATION_TIMEOUT}s"
        }
    }


@app.get("/commitment-formats")
async def commitment_formats():
    return {
        'miner_format': "s3:data:access:{coldkey}:{hotkey}:{timestamp}",
        'validator_format': "s3:validator:access:{timestamp}",
        'miner_specific_format': "s3:validator:miner:{miner_hotkey}:{timestamp}",
        'example_miner': "s3:data:access:5F3...coldkey:5H2...hotkey:1682345678",
        'example_validator': "s3:validator:access:1682345678",
        'example_miner_specific': "s3:validator:miner:5F3...miner_hotkey:1682345678",
        'folder_structure': {
            'new_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
            'description': 'Job-based folder structure with explicit labels under data/ prefix',
            'example_paths': [
                'data/hotkey=5F3...xyz/job_id=default_0/data_20250620_143052_150.parquet',
                'data/hotkey=5F3...xyz/job_id=crawler-7-h4rptebsja6qbdmocrt98/data_20250620_143055_67.parquet'
            ]
        },
        'instructions': "1. Generate timestamp\n2. Sign commitment\n3. Make API request\n4. Upload to job_id folders with explicit labels under data/ prefix",
        'timeout_protection': {
            'validator_verification': f"{VALIDATOR_VERIFICATION_TIMEOUT} seconds",
            'signature_verification': f"{SIGNATURE_VERIFICATION_TIMEOUT} seconds",
            'description': "All validation operations have timeout protection to prevent hanging"
        }
    }


@app.get("/structure-info")
async def structure_info():
    """Endpoint to get information about the new folder structure"""
    return {
        'folder_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
        'changes': {
            'old_structure': 'hotkey={hotkey_id}/job_id={job_id}/',
            'new_structure': 'data/hotkey={hotkey_id}/job_id={job_id}/',
            'benefits': [
                'Explicit hotkey and job_id labeling',
                'Cleaner path structure with data/ prefix',
                'Better organization for miners and validators',
                '2-minute timeout protection for validator verification',
                'Comprehensive error handling and monitoring'
            ]
        },
        'example_paths': [
            'data/hotkey=5F3...xyz/job_id=default_0/data_20250620_143052_150.parquet',
            'data/hotkey=5F3...xyz/job_id=crawler-7-h4rptebsja6qbdmocrt98/data_20250620_143055_67.parquet'
        ],
        'upload_flow': [
            '1. Get job IDs from Gravity',
            '2. Request S3 credentials via API',
            '3. Upload files to data/hotkey={hotkey_id}/job_id={job_id}/ folders',
            '4. Each job gets its own folder with explicit labels under data/ prefix'
        ],
        'timeout_protection': {
            'validator_verification': f"{VALIDATOR_VERIFICATION_TIMEOUT} seconds (2 minutes)",
            'signature_verification': f"{SIGNATURE_VERIFICATION_TIMEOUT} seconds",
            's3_operations': f"{S3_OPERATION_TIMEOUT} seconds",
            'description': "All operations have timeout protection to prevent server hanging"
        }
    }


if __name__ == "__main__":
    import uvicorn

    # Run with optimized settings
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=SERVER_PORT,
        reload=False,
        workers=1,
        loop="asyncio",
        timeout_keep_alive=180,  # 3 minutes to handle long operations
        timeout_graceful_shutdown=30
    )