from __future__ import annotations

from typing import Dict, Tuple, List, Optional, Type, Union
from datetime import datetime, timezone

from s3_storage_api.db.on_demand_orm import OnDemandJobORM, OnDemandSubmissionORM
from s3_storage_api.models.on_demand import (
    OnDemandJob,
    OnDemandJobSubmission,
    OnDemandJobPayloadX,
    OnDemandJobPayloadReddit,
    OnDemandJobPayloadYoutube,
)


from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


PayloadModel = Union[
    OnDemandJobPayloadX, OnDemandJobPayloadReddit, OnDemandJobPayloadYoutube
]


class PayloadRegistry:
    """Registry for platform payload models; extend to support new platforms."""

    _registry: Dict[str, Type[PayloadModel]] = {
        "x": OnDemandJobPayloadX,
        "reddit": OnDemandJobPayloadReddit,
        "youtube": OnDemandJobPayloadYoutube,
    }

    @classmethod
    def get_model(cls, platform: str) -> Type[PayloadModel]:
        if platform not in cls._registry:
            raise ValueError(
                f"Unsupported platform '{platform}'. Register a payload model."
            )
        return cls._registry[platform]

    @classmethod
    def serialize(cls, payload: PayloadModel) -> Tuple[str, dict]:
        """Return (platform, serialized_dict)."""
        platform = payload.platform
        return platform, payload.model_dump(mode="json")

    @classmethod
    def deserialize(cls, platform: str, raw: dict) -> PayloadModel:
        model = cls.get_model(platform)
        raw = {
            "platform": platform,
            **{k: v for k, v in raw.items() if k != "platform"},
        }
        return model.model_validate(raw)


def orm_on_demand_job_to_domain(orm: OnDemandJobORM) -> OnDemandJob:
    payload = PayloadRegistry.deserialize(orm.platform, orm.payload)
    return OnDemandJob.model_construct(
        id=orm.id,
        created_at=orm.created_at,
        expire_at=orm.expire_at,
        job=payload,
        start_date=orm.start_date,
        end_date=orm.end_date,
        limit=orm.limit,
        keyword_mode=orm.keyword_mode,
    )


def orm_on_demand_submission_to_domain(
    orm: OnDemandSubmissionORM, presigned_url: str | None = None
) -> OnDemandJobSubmission:
    return OnDemandJobSubmission.model_construct(
        job_id=orm.job_id,
        miner_hotkey=orm.miner_hotkey,
        miner_vtrust=orm.miner_vtrust,
        submitted_at=orm.submitted_at,
        s3_path=orm.s3_path,
        s3_presigned_url=presigned_url,
    )


class OnDemandJobRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def insert(self, job: OnDemandJob) -> None:
        platform, payload = PayloadRegistry.serialize(job.job)
        obj = OnDemandJobORM(
            id=job.id,
            created_at=job.created_at,
            expire_at=job.expire_at,
            platform=platform,
            payload=payload,
            start_date=job.start_date,
            end_date=job.end_date,
            limit=job.limit,
            keyword_mode=job.keyword_mode,
        )
        async with self._session_factory() as s:
            s.add(obj)
            await s.commit()

    async def get(self, job_id: str) -> Optional[OnDemandJob]:
        async with self._session_factory() as s:
            res = await s.execute(
                select(OnDemandJobORM).where(OnDemandJobORM.id == job_id)
            )
            row = res.scalar_one_or_none()
            return orm_on_demand_job_to_domain(row) if row else None

    async def list_active(
        self, since: datetime, now: Optional[datetime] = None
    ) -> List[OnDemandJob]:
        now = now or datetime.now(timezone.utc)
        async with self._session_factory() as s:
            res = await s.execute(
                select(OnDemandJobORM)
                .where(
                    and_(
                        OnDemandJobORM.created_at >= since,
                        OnDemandJobORM.expire_at >= now,
                    )
                )
                .order_by(OnDemandJobORM.created_at.desc())
            )
            return [orm_on_demand_job_to_domain(r) for r in res.scalars().all()]

    async def list_expired_with_submissions_window(
        self, expired_since: datetime, expired_until: datetime, limit: int
    ) -> List[Tuple[OnDemandJobORM, OnDemandSubmissionORM]]:
        from sqlalchemy import select

        async with self._session_factory() as s:
            job_ids_stmt = (
                select(OnDemandJobORM.id)
                .join(
                    OnDemandSubmissionORM,
                    OnDemandSubmissionORM.job_id == OnDemandJobORM.id,
                )
                .where(
                    and_(
                        OnDemandJobORM.expire_at >= expired_since,
                        OnDemandJobORM.expire_at <= expired_until,
                    )
                )
                .group_by(OnDemandJobORM.id)
                .order_by(func.min(OnDemandSubmissionORM.submitted_at).asc())
                .limit(limit)
            ).subquery()

            rows = await s.execute(
                select(OnDemandJobORM, OnDemandSubmissionORM)
                .join(job_ids_stmt, job_ids_stmt.c.id == OnDemandJobORM.id)
                .join(
                    OnDemandSubmissionORM,
                    OnDemandSubmissionORM.job_id == OnDemandJobORM.id,
                )
                .order_by(
                    OnDemandJobORM.expire_at.asc(),
                    OnDemandSubmissionORM.submitted_at.asc(),
                )
            )
            return rows.all()


class OnDemandSubmissionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def insert(
        self, submission: OnDemandJobSubmission, s3_path: str | None
    ) -> None:
        obj = OnDemandSubmissionORM(
            job_id=submission.job_id,
            miner_hotkey=submission.miner_hotkey,
            miner_vtrust=submission.miner_vtrust,
            submitted_at=submission.submitted_at,
            s3_path=s3_path,
        )
        async with self._session_factory() as s:
            s.add(obj)
            try:
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    async def list_by_job(self, job_id: str) -> List[OnDemandSubmissionORM]:
        async with self._session_factory() as s:
            res = await s.execute(
                select(OnDemandSubmissionORM)
                .where(OnDemandSubmissionORM.job_id == job_id)
                .order_by(OnDemandSubmissionORM.submitted_at.asc())
            )
            return list(res.scalars().all())

    async def exists(self, job_id: str, miner_hotkey: str) -> bool:
        async with self._session_factory() as s:
            res = await s.execute(
                select(func.count(OnDemandSubmissionORM.id)).where(
                    and_(
                        OnDemandSubmissionORM.job_id == job_id,
                        OnDemandSubmissionORM.miner_hotkey == miner_hotkey,
                    )
                )
            )
            return res.scalar_one() > 0
