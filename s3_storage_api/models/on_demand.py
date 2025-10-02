from datetime import datetime, timedelta, timezone
from typing import List, Literal, Optional, Union
from uuid import uuid4
from pydantic import BaseModel, Field, JsonValue, field_validator, model_validator


class OnDemandJobPayloadX(BaseModel):
    platform: Literal["x"] = "x"

    usernames: Optional[List[str]] = None
    keywords: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_dates_and_keywords(self) -> "OnDemandJobPayloadYoutube":
        if self.usernames and len(self.usernames) > 10:
            raise ValueError("len(usernames) can not be greater than 10.")

        if self.usernames:
            # remove any leading '@' characters
            self.usernames = [ch.lstrip("@") for ch in self.usernames]

        if self.keywords and len(self.keywords) > 10:
            raise ValueError("len(keywords) can not be greater than 10.")

        return self


class OnDemandJobPayloadReddit(BaseModel):
    platform: Literal["reddit"] = "reddit"

    subreddit: Optional[str] = None

    usernames: Optional[List[str]] = None
    keywords: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_dates_and_keywords(self) -> "OnDemandJobPayloadYoutube":
        if self.subreddit and not self.subreddit.startswith("r/"):
            raise ValueError("subreddit must start with r/")

        if self.usernames and len(self.usernames) > 10:
            raise ValueError("len(usernames) can not be greater than 10.")

        if self.usernames:
            # remove any leading '@' characters
            self.usernames = [ch.lstrip("@") for ch in self.usernames]

        if self.keywords and len(self.keywords) > 10:
            raise ValueError("len(keywords) can not be greater than 10.")

        return self


class OnDemandJobPayloadYoutube(BaseModel):
    platform: Literal["youtube"] = "youtube"

    channels: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_dates_and_keywords(self) -> "OnDemandJobPayloadYoutube":
        if self.channels and len(self.channels) > 10:
            raise ValueError("len(channels) can not be greater than 10.")

        if self.channels:
            # remove any leading '@' characters
            self.channels = [ch.lstrip("@") for ch in self.channels]

        return self


class OnDemandJob(BaseModel):
    id: str = "<auto_generated>"

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expire_at: Optional[datetime] = None

    job: Union[
        OnDemandJobPayloadX, OnDemandJobPayloadReddit, OnDemandJobPayloadYoutube
    ] = Field(discriminator="platform")

    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    limit: int = Field(default=100, ge=1, le=1000)
    keyword_mode: Literal["any"] | Literal["all"] = "any"

    @field_validator("id", mode="before")
    @classmethod
    def always_generate_id(cls, v):
        return str(uuid4())

    @model_validator(mode="after")
    def validate_dates_and_keywords(self) -> "OnDemandJob":
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date cannot be earlier than start_date.")

        now = datetime.now(timezone.utc)
        if self.created_at > now:
            raise ValueError("created_at cannot be in the future.")

        if self.expire_at:
            if self.expire_at < self.created_at + timedelta(seconds=30):
                self.expire_at = self.created_at + timedelta(seconds=30)
        else:
            self.expire_at = self.created_at + timedelta(minutes=2)

        return self


class OnDemandJobSubmission(BaseModel):
    job_id: str
    miner_hotkey: str = "will_be_auto_filled"
    miner_incentive: float = 0.0  # will_be_auto_filled

    s3_path: Optional[str] = None
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
