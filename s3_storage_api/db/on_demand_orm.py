from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from s3_storage_api.db.base import Base


class OnDemandJobORM(Base):
    __tablename__ = "on_demand_jobs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    expire_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    platform: Mapped[str] = mapped_column(String(256), nullable=False, index=True)

    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    limit: Mapped[int] = mapped_column(Integer, nullable=False)
    keyword_mode: Mapped[str] = mapped_column(String(256), nullable=False)

    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    submissions: Mapped[list["OnDemandSubmissionORM"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", lazy="selectin"
    )

    __table_args__ = (
        Index("ix_on_demand_jobs_created_at", "created_at"),
        Index("ix_on_demand_jobs_expire_at", "expire_at"),
        Index("ix_on_demand_jobs_platform", "platform"),
    )


class OnDemandSubmissionORM(Base):
    __tablename__ = "on_demand_job_submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        ForeignKey("on_demand_jobs.id", ondelete="CASCADE"), index=True, nullable=False
    )

    miner_hotkey: Mapped[str] = mapped_column(String(256), nullable=False)
    miner_vtrust: Mapped[float] = mapped_column()

    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    s3_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    job: Mapped[OnDemandJobORM] = relationship(back_populates="submissions")

    __table_args__ = (
        UniqueConstraint("job_id", "miner_hotkey", name="uq_submission_job_miner"),
        Index("ix_submissions_job_id_submitted_at", "job_id", "submitted_at"),
    )
