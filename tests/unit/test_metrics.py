"""
Unit tests for src/quality/metrics.py

Tests PipelineMetrics accumulator logic without Spark.
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.quality.metrics import PipelineMetrics


class TestPipelineMetrics:
    def test_initial_state(self):
        m = PipelineMetrics("batch-001")
        assert m.batch_id == "batch-001"
        assert m.bronze_counts == {}
        assert m.silver_counts == {}
        assert m.rejected_counts == {}

    def test_record_bronze(self):
        m = PipelineMetrics("b1")
        m.record_bronze("orders", 5000)
        assert m.bronze_counts["orders"] == 5000

    def test_record_silver(self):
        m = PipelineMetrics("b1")
        m.record_bronze("orders", 1000)
        m.record_silver("orders", 950, 50)
        assert m.silver_counts["orders"] == 950
        assert m.rejected_counts["orders"] == 50

    def test_quality_pct_calculation(self):
        m = PipelineMetrics("b1")
        m.record_bronze("orders", 1000)
        m.record_silver("orders", 900, 100)
        assert m._data["quality_pct"]["orders"] == 90.0

    def test_quality_pct_zero_ingested(self):
        m = PipelineMetrics("b1")
        m.record_bronze("orders", 0)
        m.record_silver("orders", 0, 0)
        assert m._data["quality_pct"]["orders"] == 0.0

    def test_finish_sets_status_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("src.quality.metrics._METRICS_DIR", Path(tmp)):
                m = PipelineMetrics("b1")
                m.finish(success=True)
                assert m._data["status"] == "success"
                assert "duration_seconds" in m._data
                assert m._data["duration_seconds"] >= 0

    def test_finish_sets_status_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("src.quality.metrics._METRICS_DIR", Path(tmp)):
                m = PipelineMetrics("b1")
                m.finish(success=False)
                assert m._data["status"] == "failed"

    def test_saves_json_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            with patch("src.quality.metrics._METRICS_DIR", tmp_path):
                m = PipelineMetrics("batch-save-test")
                m.record_bronze("orders", 100)
                m.finish(success=True)
                saved = list(tmp_path.glob("*.json"))
                assert len(saved) == 1
                data = json.loads(saved[0].read_text())
                assert data["batch_id"] == "batch-save-test"

    def test_record_error(self):
        m = PipelineMetrics("b1")
        m.record_error("Something failed")
        assert "Something failed" in m._data["errors"]

    def test_load_latest_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("src.quality.metrics._METRICS_DIR", Path(tmp)):
                result = PipelineMetrics.load_latest()
                assert result is None
