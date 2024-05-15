import logging
from typing import Any, Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from text2sql_epi.settings import settings

logger = logging.getLogger(__name__)

snowflake_url = (
    f"snowflake://{settings.SNOWFLAKE_USER}:{settings.SNOWFLAKE_PASSWORD}@"
    f"{settings.SNOWFLAKE_ACCOUNT_IDENTIFIER}/{{database}}?warehouse={settings.SNOWFLAKE_WAREHOUSE}"
)

engine_cache = {}


def get_engine_for_db(db_name: Optional[str] = None) -> Engine:
    database = db_name if db_name else "OPTUM_CLAIMS_OMOP"
    if database not in engine_cache:
        engine_url = snowflake_url.format(database=database)
        engine_cache[database] = create_engine(
            engine_url,
            connect_args={
                "client_session_keep_alive": True,
                "timeout": settings.SNOWFLAKE_TIMEOUT,
            },
            pool_size=20,
            max_overflow=10,
        )
    return engine_cache[database]


# Configure SessionLocal to be bound later
SessionLocal = sessionmaker(autocommit=False, autoflush=False)

schema_cache = {}


def get_db(db_name: Optional[str] = None) -> Generator:
    engine = get_engine_for_db(db_name)
    SessionLocal.configure(bind=engine)
    with SessionLocal() as db:
        db.execute(
            f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {settings.SNOWFLAKE_TIMEOUT}"
        )
        try:
            if db_name:
                if db_name not in schema_cache:
                    schema_cache[db_name] = get_current_schema(db, db_name)
                schema_name = schema_cache[db_name]
                db.execute(f'USE SCHEMA "{db_name}"."{schema_name}"')
            yield db
        except Exception as e:
            logger.exception(f"Error while connecting to the database: {e}")
            raise
        finally:
            db.close()


def get_current_schema(db: Any, database_name: str) -> str:
    schema_query = f"show schemas in database {database_name};"
    _ = db.execute(schema_query)
    schema_name_query = 'select max("name") from TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "name" like \'CDM%\';'
    schema_name = db.execute(schema_name_query).scalar()
    return schema_name
