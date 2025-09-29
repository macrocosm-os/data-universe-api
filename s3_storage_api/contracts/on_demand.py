from datetime import datetime, timedelta, timezone
from typing import List
from pydantic import BaseModel, Field, field_validator, model_validator

from s3_storage_api.models.on_demand import OnDemandJobSubmission, OnDemandJob


class CreateOnDemandJobRequest(BaseModel):
    job: OnDemandJob


class CreateOnDemandJobResponse(BaseModel):
    job_id: str


class ListActiveJobsRequest(BaseModel):
    since: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc) - timedelta(minutes=2)
    )

    @field_validator("since")
    def must_not_lag_more_than_two_minutes(cls, v):
        now = datetime.now(timezone.utc)
        if now - v > timedelta(minutes=10):
            raise ValueError("`since` cannot lag more than 10 minutes behind now.")
        return v


class ListActiveJobsResponse(BaseModel):
    jobs: List[OnDemandJob]


class SubmitJobRequest(BaseModel):
    submission: OnDemandJobSubmission

class SubmitOnDemandJobResponse(BaseModel):
    presigned_upload_url: str

class GetJobSubmissionsResponse(BaseModel):
    submissions: List[OnDemandJobSubmission]


class ListJobsWithSubmissionsForValidationRequest(BaseModel):
    expired_since: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc) - timedelta(minutes=1)
    )
    expired_until: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    limit: int = Field(default=10, ge=1, le=10)

    @model_validator(mode="after")
    def validate_expired_range(self) -> "ListJobsWithSubmissionsForValidationRequest":
        now = datetime.now(timezone.utc)

        if self.expired_since > self.expired_until:
            raise ValueError("`expired_since` cannot be after `expired_until`.")

        if self.expired_until - self.expired_since > timedelta(hours=1):
            raise ValueError("The range between `expired_since` and `expired_until` must not exceed 1 hour.")

        if self.expired_since > now + timedelta(minutes=1):
            raise ValueError("`expired_since` cannot be in the future.")
        if self.expired_until > now + timedelta(minutes=1):
            raise ValueError("`expired_until` cannot be in the future.")

        return self
    
class JobWithSubmissions(BaseModel):
    job: OnDemandJob
    submissions: List[OnDemandJobSubmission]

class ListJobsWithSubmissionForValidationResponse(BaseModel):
    jobs_with_submissions: List[JobWithSubmissions]