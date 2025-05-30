import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException
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
BT_NETWORK = os.getenv("BT_NETWORK", "finney")  # fallback to 'finney' if not set
NET_UID = int(os.getenv('NET_UID', '13'))
COMMITMENT_VALIDITY_SECONDS = 60


DAILY_LIMIT_PER_MINER = 20
DAILY_LIMIT_PER_VALIDATOR = 30
TOTAL_DAILY_LIMIT = 2000

app = FastAPI(
    title="S3 Auth Server for Data Universe",
    description="Authentication server for S3 storage in Data Universe subnet",
    version="1.0.0",
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
    config=Config(signature_version='s3v4')
)

class MinerFolderAccessRequest(BaseModel):
    coldkey: str
    hotkey: str
    source: str = "x"
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    signature: str  # HEX string
    expiry: Optional[int] = None



class ValidatorAccessRequest(BaseModel):
    hotkey: str
    signature: str  # HEX string
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    expiry: Optional[int] = None


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
    fields = {
        "acl": "private",
        "x-amz-storage-class": "STANDARD"  # ← ADD THIS
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
    expiration = datetime.utcnow() + timedelta(hours=expiry_hours)
    expiry_seconds = expiry_hours * 3600
    urls = {'global': {}, 'sources': {}, 'miners': {}}

    urls['global']['list_all_data'] = s3_client.generate_presigned_url(
        'list_objects_v2',
        Params={'Bucket': S3_BUCKET, 'Prefix': 'data/', 'Delimiter': '/'},
        ExpiresIn=expiry_seconds
    )

    for source in ['x', 'reddit']:
        source_prefix = f'data/{source}/'
        urls['sources'][source] = {
            'list_miners': s3_client.generate_presigned_url(
                'list_objects_v2',
                Params={'Bucket': S3_BUCKET, 'Prefix': source_prefix, 'Delimiter': '/'},
                ExpiresIn=expiry_seconds
            ),
            'prefix': source_prefix
        }

    return {
        'bucket': S3_BUCKET,
        'region': S3_REGION,
        'validator_hotkey': validator_hotkey,
        'expiry': expiration.isoformat(),
        'expiry_seconds': expiry_seconds,
        'urls': urls
    }


@app.post("/get-folder-access")
async def get_folder_access(request: MinerFolderAccessRequest):
    try:
        coldkey, hotkey, source = request.coldkey, request.hotkey, request.source
        timestamp = request.timestamp
        expiry = request.expiry or (timestamp + 86400)
        signature = request.signature
        folder_path = f"data/{source}/{coldkey}/"

        is_allowed, msg = check_rate_limit(hotkey, DAILY_LIMIT_PER_MINER)
        if not is_allowed:
            raise HTTPException(status_code=429, detail=msg)

        now = int(time.time())
        if now > expiry or now - timestamp > 300 or timestamp > now + 60:
            raise HTTPException(status_code=400, detail="Invalid timestamp")

        # Verify hotkey signature (no on-chain commitment needed)
        commitment = f"s3:access:{coldkey}:{source}:{timestamp}"
        if not verify_signature(commitment, signature, hotkey, NET_UID, BT_NETWORK):
            raise HTTPException(status_code=401, detail="Invalid signature")

        policy = generate_folder_upload_policy(S3_BUCKET, folder_path, expiry_hours=24)
        list_url = s3_client.generate_presigned_url(
            'list_objects_v2',
            Params={'Bucket': S3_BUCKET, 'Prefix': folder_path},
            ExpiresIn=60 * 60 * 3 # three hours
        )
        return {
            'folder': folder_path,
            'url': policy['url'],
            'fields': policy['fields'],
            'expiry': datetime.fromtimestamp(expiry).isoformat(),
            'list_url': list_url
        }

    except HTTPException:
        raise
    except Exception as e:
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

        if not verify_validator_status(hotkey=hotkey, netuid=NET_UID, network=BT_NETWORK):
            raise HTTPException(status_code=401, detail="You are not validator")
        

        if not verify_signature(commitment, signature, hotkey, NET_UID, BT_NETWORK):
            raise HTTPException(status_code=401, detail="Invalid signature")
        

        return generate_validator_access_urls(hotkey, expiry_hours=24)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthcheck")
async def health_check():
    return {'status': 'ok', 'timestamp': time.time(), 'bucket': S3_BUCKET, 'region': S3_REGION}

@app.get("/commitment-formats")
async def commitment_formats():
    return {
        'miner_format': "s3:access:{coldkey}:{source}:{timestamp}",
        'validator_format': "s3:validator:access:{timestamp}",
        'example_miner': "s3:access:5F3...:x:1682345678",
        'example_validator': "s3:validator:access:1682345678",
        'instructions': "1. Generate timestamp\n2. Commit to chain\n3. Make API request"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=SERVER_PORT, reload=True)