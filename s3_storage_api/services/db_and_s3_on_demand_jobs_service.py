from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from s3_storage_api.contracts.on_demand import (
    CreateOnDemandJobRequest,
    CreateOnDemandJobResponse,
    GetJobSubmissionsResponse,
    ListActiveJobsRequest,
    ListActiveJobsResponse,
    ListJobsWithSubmissionForValidationResponse,
    ListJobsWithSubmissionsForValidationRequest,
    JobWithSubmissions,
    SubmitOnDemandJobResponse,
)
from s3_storage_api.db.on_demand_orm import OnDemandJobORM, OnDemandSubmissionORM
from s3_storage_api.models.on_demand import OnDemandJobSubmission, OnDemandJob
from s3_storage_api.repositories.on_demand_repos import (
    OnDemandJobRepository,
    OnDemandSubmissionRepository,
    orm_on_demand_job_to_domain,
    orm_on_demand_submission_to_domain,
)
from s3_storage_api.services.on_demand_jobs_service import OnDemandJobsService
from s3_storage_api.services.s3_on_demand_storage import S3OnDemandStorage


class DBAndS3OnDemandJobsService(OnDemandJobsService):
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        s3: S3OnDemandStorage,
    ):
        super().__init__()
        self._jobs = OnDemandJobRepository(session_factory)
        self._subs = OnDemandSubmissionRepository(session_factory)
        self._s3 = s3

    async def register_new_job(
        self, request: CreateOnDemandJobRequest
    ) -> CreateOnDemandJobResponse:
        await self._jobs.insert(request.job)
        return CreateOnDemandJobResponse(job_id=request.job.id)

    async def list_active_jobs(
        self, request: ListActiveJobsRequest
    ) -> ListActiveJobsResponse:
        jobs = await self._jobs.list_active(since=request.since)
        return ListActiveJobsResponse(jobs=jobs)

    async def get_job_submissions(self, job_id: str) -> GetJobSubmissionsResponse:
        subs = await self._subs.list_by_job(job_id)

        # existence_map: {key: bool}
        existence_map = await self._s3.exists_many([s.s3_path for s in subs])

        # Build the presign set only for keys that exist
        keys_to_presign = [
            s.s3_path for s in subs if s.s3_path and existence_map.get(s.s3_path, False)
        ]
        urls_map = await self._s3.presign_get_many(keys_to_presign)  # {key: url}

        # Rebuild in original order: None where not existing/missing
        urls: List[Optional[str]] = [
            (
                urls_map.get(s.s3_path)
                if (s.s3_path and existence_map.get(s.s3_path, False))
                else None
            )
            for s in subs
        ]

        # Map back to domain objects keeping original order
        domain = [orm_on_demand_submission_to_domain(s, u) for s, u in zip(subs, urls)]
        return GetJobSubmissionsResponse(submissions=domain)

    async def submission_exists(self, job_id: str, miner_hotkey: str) -> bool:
        return await self._subs.exists(job_id, miner_hotkey)

    async def get_job(self, job_id: str) -> Optional[OnDemandJob]:
        return await self._jobs.get(job_id)

    async def miner_submit_job(
        self, submission: OnDemandJobSubmission
    ) -> SubmitOnDemandJobResponse:
        key = self._s3.build_submission_key(
            job_id=submission.job_id,
            miner_hotkey=submission.miner_hotkey,
            created_at=submission.submitted_at,
        )

        upload_url = await self._s3.presign_put(key, "application/json")

        await self._subs.insert(submission, s3_path=key)

        return SubmitOnDemandJobResponse(presigned_upload_url=upload_url)

    async def list_jobs_with_submissions_for_validation(
        self, request: ListJobsWithSubmissionsForValidationRequest
    ) -> ListJobsWithSubmissionForValidationResponse:
        """
        Returns expired jobs (within the window) together with their submissions.
        This version batches S3 existence checks and presign operations across all
        submissions to avoid N+1 calls.
        """
        rows = await self._jobs.list_expired_with_submissions_window(
            expired_since=request.expired_since,
            expired_until=request.expired_until,
            limit=request.limit,
        )

        grouped: Dict[str, Tuple[OnDemandJobORM, List[OnDemandSubmissionORM]]] = {}
        for j, sub in rows:
            grouped.setdefault(j.id, (j, []))[1].append(sub)

        # Flatten all submissions to batch S3 operations
        all_subs: List[OnDemandSubmissionORM] = [
            sub for _, subs in grouped.values() for sub in subs
        ]

        # Short-circuit if there are no submissions at all
        if not all_subs:
            return ListJobsWithSubmissionForValidationResponse(jobs_with_submissions=[])

        # Batch existence check
        all_keys = [s.s3_path for s in all_subs]
        existence_map = await self._s3.exists_many(all_keys)

        # Batch presign only for existing keys
        keys_to_presign = [
            s.s3_path
            for s in all_subs
            if s.s3_path and existence_map.get(s.s3_path, False)
        ]
        presigned_map = await self._s3.presign_get_many(keys_to_presign)  # {key: url}

        # Build url per submission in original order
        def url_for(sub: OnDemandSubmissionORM) -> Optional[str]:
            if not sub.s3_path:
                return None
            if not existence_map.get(sub.s3_path, False):
                return None
            return presigned_map.get(sub.s3_path)

        out: List[JobWithSubmissions] = []
        for job_orm, subs in grouped.values():
            job = orm_on_demand_job_to_domain(job_orm)
            # Domain mapping is cheap; no per-item S3 calls
            dom_subs = [orm_on_demand_submission_to_domain(s, url_for(s)) for s in subs]
            out.append(JobWithSubmissions(job=job, submissions=dom_subs))

        return ListJobsWithSubmissionForValidationResponse(jobs_with_submissions=out)
