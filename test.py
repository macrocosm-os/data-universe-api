import aioboto3
from botocore.config import Config

from s3_storage_api.services.s3_on_demand_storage import S3OnDemandStorage


async def main():
    def session_factory():
        return aioboto3.Session(
            aws_access_key_id="aws_access_key",
            aws_secret_access_key="aws_secret_key",
            region_name="nyc3",
        )

    session = session_factory()

    key = "on-demand-jobs/date=2025-09-29/hour=17/job_id=0fa78fd4-fc3c-41d9-a89e-c8755c3e7da7/5DUkURJ54otPEcodx1MiGgNvAWCkq7DY6SAvQXcMMXXcidiL.json"

    async with session.client(
        "s3",
        region_name="nyc3",
        endpoint_url="https://localhost:9000",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3},
        ),
        verify=False
    ) as s3:
        resp = await s3.head_object(
            Bucket="data-universe-storage",
            Key=key,
        )
        print(resp)

    stg = S3OnDemandStorage(
        bucket="data-universe-storage",
        region="nyc3",
        aws_access_key="aws_access_key",
        aws_secret_key="aws_secret_key",
        endpoint_url="https://localhost:9000",
    )

    resp = await stg.exists(key=key)
    print(resp)

    # resp = await stg.exists_many(keys=[key])
    # print(resp)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
