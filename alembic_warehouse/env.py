"""Alembic env for the permits warehouse (NOT the app DB).

Sync engine on purpose: warehouse migrations are run from a workstation or
server shell, never from the async app. Version table is namespaced so it can
never collide with the app's alembic_version.
"""
import os

from alembic import context
from sqlalchemy import create_engine

config = context.config

url = os.environ.get("WAREHOUSE_DATABASE_URL") or config.get_main_option(
    "sqlalchemy.url"
)

VERSION_TABLE = "alembic_version_warehouse"


def run_migrations_offline() -> None:
    context.configure(
        url=url,
        literal_binds=True,
        version_table=VERSION_TABLE,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(url)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            version_table=VERSION_TABLE,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
