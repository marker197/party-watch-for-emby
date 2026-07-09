"""Alembic environment configuration for emby-trakt-suite.

This module configures Alembic to:
  - Use our SQLAlchemy models for auto-generating migrations
  - Handle async PostgreSQL connection via SQLAlchemy's sync wrapper
  - Support both offline and online migration modes
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, create_engine
from sqlalchemy.pool import StaticPool

from alembic import context

# Add app directory to path so we can import our models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.database import Base

# Alembic config object
config = context.config

# Configure logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata from our SQLAlchemy models
target_metadata = Base.metadata

# Get sync database URL from async URL (replace postgresql+asyncpg with postgresql+psycopg)
database_url = settings.database_url
if "postgresql+asyncpg" in database_url:
    sync_database_url = database_url.replace("postgresql+asyncpg", "postgresql+psycopg")
else:
    sync_database_url = database_url

config.set_main_option("sqlalchemy.url", sync_database_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (doesn't need actual DB connection).
    
    Used for generating migration SQL without executing against a live database.
    Also used for autogenerate since we're using an async engine in production.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connects to actual database).
    
    This is used when applying migrations to a live database.
    """
    connectable = create_engine(
        sync_database_url,
        poolclass=StaticPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


# For autogenerate, always use offline mode with our models
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()


