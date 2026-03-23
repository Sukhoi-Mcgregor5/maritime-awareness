import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from config import settings
from database import Base

# Import all models here so Alembic's autogenerate can detect them
from ontology.models import *  # noqa: F401
# from ingestion.models import *  # noqa: F401
# from detection.models import *  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override URL from application settings (reads DATABASE_URL env var)
config.set_main_option("sqlalchemy.url", settings.database_url)

target_metadata = Base.metadata

# Tables owned by PostGIS / Tiger geocoder extensions — never touch these
_EXCLUDED_TABLES = {
    "spatial_ref_sys", "topology", "layer",
    "state", "county", "cousub", "place", "tract", "bg", "tabblock", "tabblock20",
    "edges", "addrfeat", "faces", "featnames", "addr", "zcta5",
    "pagc_lex", "pagc_gaz", "pagc_rules",
    "loader_lookuptables", "loader_platform", "loader_variables",
    "geocode_settings", "geocode_settings_default",
    "direction_lookup", "street_type_lookup", "secondary_unit_lookup",
    "place_lookup", "county_lookup", "countysub_lookup", "state_lookup",
    "zip_lookup", "zip_lookup_all", "zip_lookup_base", "zip_state", "zip_state_loc",
}


def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name in _EXCLUDED_TABLES:
        return False
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, include_object=include_object)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
