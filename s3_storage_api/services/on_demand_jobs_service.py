from abc import ABC, abstractmethod
from typing import Optional

from s3_storage_api.contracts.on_demand import (
    CreateOnDemandJobResponse,
    GetJobSubmissionsResponse,
    ListActiveJobsRequest,
    CreateOnDemandJobRequest,
    ListActiveJobsResponse,
    ListJobsWithSubmissionForValidationResponse,
    ListJobsWithSubmissionsForValidationRequest,
    SubmitOnDemandJobResponse,
)
from s3_storage_api.models.on_demand import OnDemandJob, OnDemandJobSubmission


class OnDemandJobsService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    async def register_new_job(
        self, request: CreateOnDemandJobRequest
    ) -> CreateOnDemandJobResponse:
        pass

    @abstractmethod
    async def list_active_jobs(
        self, request: ListActiveJobsRequest
    ) -> ListActiveJobsResponse:
        pass

    @abstractmethod
    async def get_job_submissions(self, job_id: str) -> GetJobSubmissionsResponse:
        pass

    @abstractmethod
    async def submission_exists(self, job_id: str, miner_hotkey: str) -> bool:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Optional[OnDemandJob]:
        pass

    @abstractmethod
    async def miner_submit_job(
        self, submission: OnDemandJobSubmission
    ) -> SubmitOnDemandJobResponse:
        pass

    @abstractmethod
    async def list_jobs_with_submissions_for_validation(
        self, request: ListJobsWithSubmissionsForValidationRequest
    ) -> ListJobsWithSubmissionForValidationResponse:
        pass
