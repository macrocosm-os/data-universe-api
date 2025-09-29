from datetime import datetime, timedelta, timezone
from typing import cast
from fastapi import APIRouter, Depends, HTTPException, status

from s3_storage_api.auth import (
    BittensorMinerAuth,
    BittensorValidatorAuth,
    verify_bittensor_miner,
    verify_bittensor_validator,
    verify_constellation,
)
from s3_storage_api.contracts.on_demand import (
    CreateOnDemandJobRequest,
    CreateOnDemandJobResponse,
    GetJobSubmissionsResponse,
    ListActiveJobsRequest,
    ListActiveJobsResponse,
    ListJobsWithSubmissionForValidationResponse,
    ListJobsWithSubmissionsForValidationRequest,
    SubmitJobRequest,
    SubmitOnDemandJobResponse,
)
from s3_storage_api.deps import get_on_demand_jobs_service, get_rate_limiter
from s3_storage_api.models.on_demand import OnDemandJob
from s3_storage_api.services.on_demand_jobs_service import OnDemandJobsService
from s3_storage_api.services.rate_limiter import RateLimiter

router = APIRouter(prefix="/on-demand")

RATE_LIMIT_OP_ON_DEMAND_ACTIVE_JOB_POLL = "on_demand_job_poll"
RATE_LIMIT_OP_ON_DEMAND_MINER_FAILURE = "on_demand_miner_failure"
RATE_LIMIT_OP_VALIDATOR_READ = "validator_read"


@router.post(
    "/constellation/jobs",
    response_model=CreateOnDemandJobResponse,
    status_code=201,
)
async def register_new_job_route(
    req: CreateOnDemandJobRequest,
    svc: OnDemandJobsService = Depends(get_on_demand_jobs_service),
    _auth=Depends(verify_constellation),
) -> CreateOnDemandJobResponse:
    return await svc.register_new_job(req)


@router.get("/constellation/jobs/{job_id}", response_model=GetJobSubmissionsResponse)
async def get_job(
    job_id: str,
    svc: OnDemandJobsService = Depends(get_on_demand_jobs_service),
    _auth=Depends(verify_constellation),
) -> GetJobSubmissionsResponse:
    return await svc.get_job_submissions(job_id=job_id)


@router.post(
    "/miner/jobs/active",
    response_model=ListActiveJobsResponse,
)
async def list_active_jobs_route(
    req: ListActiveJobsRequest,
    svc: OnDemandJobsService = Depends(get_on_demand_jobs_service),
    auth: BittensorMinerAuth = Depends(verify_bittensor_miner),
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
) -> ListActiveJobsResponse:
    pass_rate_limit = await rate_limiter.hit(
        key=auth.miner_hotkey,
        op=RATE_LIMIT_OP_ON_DEMAND_ACTIVE_JOB_POLL,
        limit=1,
        window=20,
    )
    if not pass_rate_limit:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS)

    return await svc.list_active_jobs(req)


@router.post("/miner/jobs/submit", response_model=SubmitOnDemandJobResponse)
async def miner_job_submit(
    req: SubmitJobRequest,
    svc: OnDemandJobsService = Depends(get_on_demand_jobs_service),
    auth: BittensorMinerAuth = Depends(verify_bittensor_miner),
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
):
    pass_failure_rate_limit = await rate_limiter.check(
        key=auth.miner_hotkey, op=RATE_LIMIT_OP_ON_DEMAND_MINER_FAILURE, limit=2
    )
    if not pass_failure_rate_limit:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS)

    req.submission.miner_hotkey = auth.miner_hotkey
    req.submission.miner_vtrust = auth.miner_vtrust
    req.submission.submitted_at = datetime.now(timezone.utc)

    job_maybe = await svc.get_job(job_id=req.submission.job_id)
    if not job_maybe:
        await rate_limiter.hit(
            key=auth.miner_hotkey,
            op=RATE_LIMIT_OP_ON_DEMAND_MINER_FAILURE,
            limit=2,
            window=60,
        )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    job: OnDemandJob = job_maybe
    job_upload_window_expired = datetime.now(timezone.utc) > job.expire_at - timedelta(
        seconds=5
    )
    if job_upload_window_expired:
        await rate_limiter.hit(
            key=auth.miner_hotkey,
            op=RATE_LIMIT_OP_ON_DEMAND_MINER_FAILURE,
            limit=2,
            window=60,
        )

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job's upload window expired",
        )

    submission_exists = await svc.submission_exists(
        job_id=req.submission.job_id, miner_hotkey=auth.miner_hotkey
    )
    if submission_exists:
        await rate_limiter.hit(
            key=auth.miner_hotkey,
            op=RATE_LIMIT_OP_ON_DEMAND_MINER_FAILURE,
            limit=2,
            window=60,
        )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Submission already exists for this miner hotkey and job id pair.",
        )

    return await svc.miner_submit_job(submission=req.submission)


@router.post(
    "/validator/jobs", response_model=ListJobsWithSubmissionForValidationResponse
)
async def get_jobs_with_submissions(
    req: ListJobsWithSubmissionsForValidationRequest,
    svc: OnDemandJobsService = Depends(get_on_demand_jobs_service),
    auth: BittensorValidatorAuth = Depends(verify_bittensor_validator),
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
) -> ListJobsWithSubmissionForValidationResponse:
    rate_pass = await rate_limiter.hit(
        key=auth.validator_hotkey, op=RATE_LIMIT_OP_VALIDATOR_READ, limit=2, window=60
    )
    if not rate_pass:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    return await svc.list_jobs_with_submissions_for_validation(request=req)
