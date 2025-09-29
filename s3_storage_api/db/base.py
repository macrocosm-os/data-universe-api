from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for ORM models."""
    pass


def build_async_session_factory(dsn: str) -> async_sessionmaker[AsyncSession]:
    """
    Create an async session factory.
    Example DSN (Postgres): "postgresql+asyncpg://user:pass@host:5432/dbname"
    """
    engine = create_async_engine(dsn, pool_pre_ping=True, future=True)
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_all(dsn: str, base: type[DeclarativeBase] = Base) -> None:
    """
    Create tables in the database. Prefer Alembic for production.
    """
    engine = create_async_engine(dsn, pool_pre_ping=True, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)
