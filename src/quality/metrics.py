"""
Pipeline execution metrics collector.
Records ingestion counts, quality results, and execution timing
to provide an execution summary after each pipeline run.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

_METRICS_DIR = Path(__file__).resolve().parents[2] / "data" / "metrics"


class PipelineMetrics:
    """Thread-safe accumulator for pipeline run statistics."""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        self.start_time = datetime.now(tz=timezone.utc)
        self.end_time: datetime | None = None
        self._data: dict[str, Any] = {
            "batch_id": batch_id,
            "start_time": self.start_time.isoformat(),
            "status": "running",
            "bronze_counts": {},
            "silver_counts": {},
            "rejected_counts": {},
            "gold_counts": {},
            "quality_pct": {},
            "errors": [],
        }

    def record_bronze(self, entity: str, count: int) -> None:
        self._data["bronze_counts"][entity] = count
        logger.info("Bronze %s ingested=%d", entity, count)

    def record_silver(self, entity: str, valid: int, rejected: int) -> None:
        self._data["silver_counts"][entity] = valid
        self._data["rejected_counts"][entity] = rejected
        ingested = self._data["bronze_counts"].get(entity, 0)
        pct = round(valid / ingested * 100, 2) if ingested > 0 else 0.0
        self._data["quality_pct"][entity] = pct
        logger.info(
            "Silver %s valid=%d rejected=%d quality=%.1f%%", entity, valid, rejected, pct
        )

    def record_gold(self, table: str, count: int) -> None:
        self._data["gold_counts"][table] = count

    def record_error(self, msg: str) -> None:
        self._data["errors"].append(msg)
        logger.error("Pipeline error: %s", msg)

    def finish(self, success: bool = True) -> None:
        self.end_time = datetime.now(tz=timezone.utc)
        self._data["end_time"] = self.end_time.isoformat()
        duration = (self.end_time - self.start_time).total_seconds()
        self._data["duration_seconds"] = round(duration, 2)
        self._data["status"] = "success" if success else "failed"
        self._save()
        self._log_summary()

    def _save(self) -> None:
        _METRICS_DIR.mkdir(parents=True, exist_ok=True)
        path = _METRICS_DIR / f"{self.batch_id}.json"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)
        logger.info("Metrics saved path=%s", path)

    def _log_summary(self) -> None:
        d = self._data
        logger.info(
            "Pipeline summary batch=%s status=%s duration=%.1fs "
            "bronze=%s silver=%s rejected=%s gold=%s",
            d["batch_id"],
            d["status"],
            d.get("duration_seconds", 0),
            d["bronze_counts"],
            d["silver_counts"],
            d["rejected_counts"],
            d["gold_counts"],
        )

    @property
    def bronze_counts(self) -> dict[str, int]:
        return dict(self._data["bronze_counts"])

    @property
    def silver_counts(self) -> dict[str, int]:
        return dict(self._data["silver_counts"])

    @property
    def rejected_counts(self) -> dict[str, int]:
        return dict(self._data["rejected_counts"])

    @staticmethod
    def load_latest() -> dict | None:
        """Load the most recent pipeline metrics file."""
        if not _METRICS_DIR.exists():
            return None
        files = sorted(_METRICS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
        if not files:
            return None
        with files[-1].open(encoding="utf-8") as fh:
            return json.load(fh)
