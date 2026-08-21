"""Database connection and session management."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from packages.config import get_settings

if TYPE_CHECKING:
    pass


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""

    pass


class Database:
    """Database connection manager.

    Handles:
    - Engine creation
    - Session management
    - Connection pooling
    """

    def __init__(self, database_url: str) -> None:
        """Initialize database.

        Args:
            database_url: Async database URL
        """
        self._url = database_url
        self._engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def create_tables(self) -> None:
        """Create all tables."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self) -> None:
        """Drop all tables."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Get database session.

        Usage:
            async with db.session() as session:
                # use session
        """
        session = self._session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Get transaction context.

        Usage:
            async with db.transaction() as session:
                # use session
        """
        async with self.session() as session:
            async with session.begin():
                yield session

    async def close(self) -> None:
        """Close database connections."""
        await self._engine.dispose()


# Global database instance
_database: Database | None = None


def get_database() -> Database:
    """Get global database instance.

    Returns:
        Database instance.
    """
    global _database
    if _database is None:
        settings = get_settings()
        _database = Database(settings.database.url)
    return _database
