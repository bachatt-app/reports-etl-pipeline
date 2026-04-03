"""
Async BigQuery MERGE execution service.

After a CSV is uploaded to GCS, the hive-partitioned external table
automatically picks up the file.  This service runs the MERGE SQL that
deduplicates and pushes new rows into the final BigQuery table.

Usage
-----
    svc = BigQueryMergeService(project="my-project", dataset="cams_data")
    rows = await svc.run_merge(CAMS_MERGE_SQL.format(project=..., dataset=...))
"""

from __future__ import annotations

import asyncio
import functools
import logging

from src.logging_context import get_request_id

logger = logging.getLogger(__name__)


class BigQueryMergeService:
    """Thin async wrapper around google-cloud-bigquery for MERGE queries."""

    def __init__(self, project: str, dataset: str) -> None:
        self._project = project
        self._dataset = dataset
        self._client = None  # lazy — created on first use inside executor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_merge(self, merge_sql_template: str) -> int:
        """
        Execute *merge_sql_template* (with {project}/{dataset} already formatted).

        Returns the number of rows affected.
        Raises on query failure (caller should catch and send alert).
        """
        sql = merge_sql_template.format(
            project=self._project,
            dataset=self._dataset,
        )

        loop = asyncio.get_running_loop()
        rows_affected: int = await loop.run_in_executor(
            None,
            functools.partial(self._run_sync, sql),
        )
        logger.info(
            "[req_id=%s] BQ MERGE complete — %d rows affected",
            get_request_id(), rows_affected,
        )
        return rows_affected

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_sync(self, sql: str) -> int:
        """Blocking BigQuery query — called inside run_in_executor."""
        from google.cloud import bigquery  # lazy import

        if self._client is None:
            self._client = bigquery.Client(project=self._project)

        job = self._client.query(sql)
        job.result()  # blocks until done; raises on error

        # num_dml_affected_rows is set for DML (MERGE) jobs
        return job.num_dml_affected_rows or 0
