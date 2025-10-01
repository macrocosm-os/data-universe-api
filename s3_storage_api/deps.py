import redis.asyncio as redis

from s3_storage_api.services.on_demand_jobs_service import OnDemandJobsService
from s3_storage_api.services.rate_limiter import (
    NoRateLimiter,
    RateLimiter,
    RedisRateLimiter,
)
from s3_storage_api.services.s3_on_demand_storage import S3OnDemandStorage
from s3_storage_api.settings import settings
from s3_storage_api.utils.metagraph_syncer import MetagraphSyncer
from s3_storage_api.db.base import build_async_session_factory
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)
from s3_storage_api.services.db_and_s3_on_demand_jobs_service import (
    DBAndS3OnDemandJobsService,
)
import bittensor as bt
import structlog
from sqlalchemy import text

logger = structlog.get_logger(__name__)

_subtensor: bt.subtensor = None
_metagraph_syncer: MetagraphSyncer = None


def get_subtensor() -> bt.subtensor:
    global _subtensor

    if _subtensor is None:
        _subtensor = bt.subtensor(network=settings.bittensor_network)

    return _subtensor


def get_metagraph_syncer() -> MetagraphSyncer:
    global _metagraph_syncer

    if _metagraph_syncer is None:
        _metagraph_syncer = MetagraphSyncer(
            get_subtensor(), config={settings.netuid: settings.metagraph_sync_interval}
        )
        _metagraph_syncer.do_initial_sync()
        _metagraph_syncer.start()

    return _metagraph_syncer


def get_metagraph() -> bt.metagraph:
    return get_metagraph_syncer().get_metagraph(netuid=settings.netuid)


_redis: redis.Redis = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis:
        return _redis

    _redis = redis.from_url(settings.redis_url)
    return _redis


_rate_limiter: RateLimiter = None


def get_rate_limiter() -> RateLimiter:
    global _rate_limiter
    if _rate_limiter:
        return _rate_limiter
    if settings.disable_rate_limiting:
        _rate_limiter = NoRateLimiter()
    else:
        _rate_limiter = RedisRateLimiter(client=get_redis(), prefix="data-universe-api")

    return _rate_limiter


_s3_on_demand_storage: S3OnDemandStorage = None


def get_s3_on_demand_storage() -> S3OnDemandStorage:
    global _s3_on_demand_storage
    if _s3_on_demand_storage:
        return _s3_on_demand_storage

    _s3_on_demand_storage = S3OnDemandStorage(
        bucket=settings.s3_bucket,
        region=settings.s3_region,
        aws_access_key=settings.aws_access_key,
        aws_secret_key=settings.aws_secret_key,
        endpoint_url=settings.s3_endpoint,
    )
    return _s3_on_demand_storage


def get_pg_async_session_factory() -> async_sessionmaker[AsyncSession]:
    return build_async_session_factory(dsn=settings.postgres_dsn)


async def readyz_pg(session_factory: async_sessionmaker[AsyncSession]) -> bool:
    """
    Read-only readiness check for Postgres.
    """
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


_on_demand_jobs_service: OnDemandJobsService = None


def get_on_demand_jobs_service() -> OnDemandJobsService:
    global _on_demand_jobs_service
    if _on_demand_jobs_service:
        return _on_demand_jobs_service

    _on_demand_jobs_service = DBAndS3OnDemandJobsService(
        s3=get_s3_on_demand_storage(), session_factory=get_pg_async_session_factory()
    )
    return _on_demand_jobs_service
