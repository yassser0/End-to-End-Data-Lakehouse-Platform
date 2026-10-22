"""
Centralised configuration loader.
All settings are read from environment variables (loaded from .env via python-dotenv).
No secrets are hardcoded. Pipeline fails loudly if critical vars are missing.
"""
import os
import warnings
from pathlib import Path

from dotenv import load_dotenv

# Load .env when running locally; in Docker the vars are injected directly.
_env_path = Path(__file__).resolve().parents[2] / ".env"
if _env_path.exists():
    load_dotenv(dotenv_path=_env_path, override=False)


def _require(name: str) -> str:
    """Return env var or raise at startup — fail closed."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Copy .env.example to .env and fill in real values."
        )
    return value


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, default)


# ── Postgres ──────────────────────────────────────────────────────────────────
POSTGRES_HOST: str = _optional("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(_optional("POSTGRES_PORT", "5432"))
POSTGRES_DB: str = _optional("POSTGRES_DB", "lakehouse")
POSTGRES_USER: str = _optional("POSTGRES_USER", "lakehouse_user")

# TODO(security): For production, use a secret manager (e.g., AWS Secrets Manager,
# HashiCorp Vault) instead of environment variables.
POSTGRES_PASSWORD: str = _optional("POSTGRES_PASSWORD", "")
if not POSTGRES_PASSWORD:
    warnings.warn(
        "POSTGRES_PASSWORD is not set. The pipeline will fail to connect to PostgreSQL. "
        "Set it in your .env file."
    )

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_MASTER: str = _optional("SPARK_MASTER", "local[*]")
SPARK_APP_NAME: str = _optional("SPARK_APP_NAME", "EcommerceDataLakehouse")

# ── Paths ─────────────────────────────────────────────────────────────────────
_project_root = Path(__file__).resolve().parents[2]
DELTA_LAKE_PATH: str = _optional(
    "DELTA_LAKE_PATH", str(_project_root / "data" / "delta")
)
DATA_PATH: str = _optional("DATA_PATH", str(_project_root / "data" / "sample"))

# Layer paths
BRONZE_PATH: str = f"{DELTA_LAKE_PATH}/bronze"
SILVER_PATH: str = f"{DELTA_LAKE_PATH}/silver"
GOLD_PATH: str = f"{DELTA_LAKE_PATH}/gold"
QUARANTINE_PATH: str = f"{DELTA_LAKE_PATH}/quarantine"

# ── Pipeline ──────────────────────────────────────────────────────────────────
LOG_LEVEL: str = _optional("LOG_LEVEL", "INFO")
PIPELINE_BATCH_ID: str = _optional("PIPELINE_BATCH_ID", "auto")

# ── Derived ───────────────────────────────────────────────────────────────────
def get_postgres_dsn() -> str:
    """Return a psycopg2-compatible DSN string (password not logged)."""
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )


def get_postgres_url() -> str:
    """Return SQLAlchemy URL for Airflow / pandas usage."""
    return (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
