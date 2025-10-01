from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, Optional

from botocore.exceptions import ClientError
from aiobotocore.session import get_session


async def _maybe_await(value):
    # Works whether generate_presigned_url returns a str (sync) or a coroutine
    return await value if inspect.isawaitable(value) else value


@dataclass
class S3OnDemandStorage:
    bucket: str
    region: str

    aws_access_key: str
    aws_secret_key: str

    endpoint_url: Optional[str] = None
    presign_ttl_seconds: int = 3600

    @property
    def _is_https(self) -> bool:
        if not self.endpoint_url:
            return True

        return "https" in self.endpoint_url

    @property
    def _verify(self) -> bool:
        if not self.endpoint_url:
            return True

        return "localhost" not in self.endpoint_url

    async def presign_put(
        self, key: str, content_type: str = "application/json"
    ) -> str:
        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:
            url = await _maybe_await(
                s3.generate_presigned_url(
                    "put_object",
                    Params={
                        "Bucket": self.bucket,
                        "Key": key,
                        "ContentType": content_type,
                    },
                    ExpiresIn=self.presign_ttl_seconds,
                )
            )
            return str(url)

    async def presign_get(self, key: str) -> str:
        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:
            url = await _maybe_await(
                s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=self.presign_ttl_seconds,
                )
            )
            return str(url)

    async def exists(self, key: Optional[str]) -> bool:
        if not key:
            return False

        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:
            try:
                await s3.head_object(Bucket=self.bucket, Key=key)
                return True
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code in ("404", "NoSuchKey", "NotFound"):
                    return False
                raise

    async def presign_get_many(self, keys: Iterable[str]) -> Dict[str, str]:
        """
        Generate presigned GET URLs for many keys. Falsy keys are skipped.
        Returns: {key: presigned_url}
        """
        keys = [k for k in keys if k]
        if not keys:
            return {}

        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:
            out: Dict[str, str] = {}
            # No threads; this is CPU/crypto-only and cheap—handle async/sync uniformly.
            for k in keys:
                url = await _maybe_await(
                    s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": self.bucket, "Key": k},
                        ExpiresIn=self.presign_ttl_seconds,
                    )
                )
                out[k] = str(url)
            return out

    async def exists_many(
        self, keys: Iterable[Optional[str]], concurrency: int = 20
    ) -> Dict[str, bool]:
        """
        Check existence for many keys using HeadObject concurrently.
        Falsy keys are dropped (treated as non-existent and skipped).
        Returns: {key: exists}
        """
        keys = [k for k in keys if k]
        if not keys:
            return {}

        semaphore = asyncio.Semaphore(max(1, concurrency))

        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:

            async def _head(k: str) -> bool:
                async with semaphore:
                    try:
                        await s3.head_object(Bucket=self.bucket, Key=k)
                        return True
                    except ClientError as e:
                        code = e.response.get("Error", {}).get("Code")
                        if code in ("404", "NoSuchKey", "NotFound"):
                            return False
                        raise

            results = await asyncio.gather(*(_head(k) for k in keys))
            return dict(zip(keys, results))

    @staticmethod
    def build_submission_key(
        job_id: str, miner_hotkey: str, created_at: datetime
    ) -> str:
        dt = created_at.astimezone(timezone.utc)
        date_part = dt.strftime("%Y-%m-%d")
        hour_part = dt.strftime("%H")
        return f"on-demand-jobs/date={date_part}/hour={hour_part}/job_id={job_id}/{miner_hotkey}.json"

    async def readyz(self) -> bool:
        async with get_session().create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            verify=self._verify,
            use_ssl=self._is_https,
        ) as s3:
            await s3.head_bucket(Bucket=self.bucket)
            return True

