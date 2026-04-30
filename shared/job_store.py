from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


VALID_JOB_STATUSES = {
    "pending",
    "queued",
    "assigned",
    "running",
    "completed",
    "failed",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStore:
    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or os.getenv(
            "COORDINATOR_DB_PATH",
            "results/coordinator.db",
        )
        self._ensure_parent_directory()
        self.init_db()

    def _ensure_parent_directory(self) -> None:
        db_parent = Path(self.db_path).expanduser().resolve().parent
        db_parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 5,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    progress REAL NOT NULL DEFAULT 0,
                    queue_name TEXT,
                    rq_job_id TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 1,
                    result_path TEXT,
                    error_message TEXT,
                    error_type TEXT,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    queued_at TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT PRIMARY KEY,
                    output_location TEXT,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_nodes (
                    worker_id TEXT PRIMARY KEY,
                    hostname TEXT,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    current_job_id TEXT,
                    current_operation TEXT,
                    cpu_percent REAL,
                    memory_percent REAL,
                    started_at TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_worker_nodes_last_seen ON worker_nodes(last_seen);"
            )
            self._ensure_jobs_columns(conn)

    def _ensure_jobs_columns(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(jobs);").fetchall()
        }
        migrations = {
            "attempt_count": "ALTER TABLE jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;",
            "max_attempts": "ALTER TABLE jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1;",
            "error_type": "ALTER TABLE jobs ADD COLUMN error_type TEXT;",
            "retryable": "ALTER TABLE jobs ADD COLUMN retryable INTEGER NOT NULL DEFAULT 0;",
        }
        for column_name, statement in migrations.items():
            if column_name not in existing_columns:
                conn.execute(statement)

    def _validate_status(self, status: str) -> None:
        if status not in VALID_JOB_STATUSES:
            raise ValueError(f"Invalid status: {status}")

    def create_job(
        self,
        job_id: str,
        file_path: str,
        operation: str,
        priority: int = 5,
        max_attempts: int = 1,
    ) -> Dict[str, Any]:
        now = utc_now_iso()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, file_path, operation, priority, status, progress,
                    attempt_count, max_attempts, retryable,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    job_id,
                    file_path,
                    operation,
                    priority,
                    "pending",
                    0,
                    0,
                    max(max_attempts, 1),
                    0,
                    now,
                    now,
                ),
            )
            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type="job_created",
                status="pending",
                payload={"priority": priority, "max_attempts": max(max_attempts, 1)},
            )
        return self.get_job(job_id)  # type: ignore[return-value]

    def mark_job_queued(
        self,
        job_id: str,
        queue_name: str,
        rq_job_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now_iso()
        with self._connection() as conn:
            result = conn.execute(
                """
                UPDATE jobs
                SET status = ?, queue_name = ?, rq_job_id = ?, queued_at = ?, updated_at = ?
                WHERE job_id = ?;
                """,
                ("queued", queue_name, rq_job_id, now, now, job_id),
            )
            if result.rowcount == 0:
                return None
            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type="job_queued",
                status="queued",
                payload={"queue_name": queue_name, "rq_job_id": rq_job_id},
            )
        return self.get_job(job_id)

    def update_job_status(
        self,
        job_id: str,
        status: str,
        worker_id: Optional[str] = None,
        progress: Optional[float] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        retryable: Optional[bool] = None,
        event_type: str = "job_status_changed",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        self._validate_status(status)

        now = utc_now_iso()
        assignments = ["status = ?", "updated_at = ?"]
        params: List[Any] = [status, now]

        if worker_id is not None:
            assignments.append("worker_id = ?")
            params.append(worker_id)

        if progress is not None:
            assignments.append("progress = ?")
            params.append(progress)

        if error_message is not None:
            assignments.append("error_message = ?")
            params.append(error_message)

        if error_type is not None:
            assignments.append("error_type = ?")
            params.append(error_type)

        if retryable is not None:
            assignments.append("retryable = ?")
            params.append(1 if retryable else 0)

        if status == "running":
            assignments.append("started_at = COALESCE(started_at, ?)")
            params.append(now)
        if status in {"completed", "failed"}:
            assignments.append("finished_at = ?")
            params.append(now)

        params.append(job_id)

        with self._connection() as conn:
            query = f"""
                UPDATE jobs
                SET {", ".join(assignments)}
                WHERE job_id = ?;
            """
            result = conn.execute(query, params)
            if result.rowcount == 0:
                return None

            event_payload = payload or {}
            if worker_id is not None:
                event_payload["worker_id"] = worker_id
            if progress is not None:
                event_payload["progress"] = progress
            if error_message is not None:
                event_payload["error_message"] = error_message
            if error_type is not None:
                event_payload["error_type"] = error_type
            if retryable is not None:
                event_payload["retryable"] = retryable

            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type=event_type,
                status=status,
                payload=event_payload,
            )

        return self.get_job(job_id)

    def start_job_attempt(
        self,
        job_id: str,
        worker_id: str,
        *,
        progress: float = 10,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now_iso()
        with self._connection() as conn:
            result = conn.execute(
                """
                UPDATE jobs
                SET status = ?,
                    worker_id = ?,
                    progress = ?,
                    attempt_count = attempt_count + 1,
                    error_message = NULL,
                    error_type = NULL,
                    retryable = 0,
                    started_at = COALESCE(started_at, ?),
                    updated_at = ?
                WHERE job_id = ?;
                """,
                ("running", worker_id, progress, now, now, job_id),
            )
            if result.rowcount == 0:
                return None

            row = conn.execute(
                "SELECT attempt_count, max_attempts FROM jobs WHERE job_id = ?;",
                (job_id,),
            ).fetchone()
            event_payload = payload or {}
            event_payload.update(
                {
                    "worker_id": worker_id,
                    "progress": progress,
                    "attempt_count": row["attempt_count"],
                    "max_attempts": row["max_attempts"],
                }
            )
            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type="job_started",
                status="running",
                payload=event_payload,
            )

        return self.get_job(job_id)

    def mark_job_retry_scheduled(
        self,
        job_id: str,
        error_message: str,
        *,
        error_type: str,
        worker_id: Optional[str] = None,
        retries_left: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now_iso()
        with self._connection() as conn:
            result = conn.execute(
                """
                UPDATE jobs
                SET status = ?,
                    progress = ?,
                    worker_id = COALESCE(?, worker_id),
                    error_message = ?,
                    error_type = ?,
                    retryable = ?,
                    updated_at = ?
                WHERE job_id = ?;
                """,
                ("queued", 0, worker_id, error_message, error_type, 1, now, job_id),
            )
            if result.rowcount == 0:
                return None
            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type="job_retry_scheduled",
                status="queued",
                payload={
                    "error_message": error_message,
                    "error_type": error_type,
                    "retryable": True,
                    "worker_id": worker_id,
                    "retries_left": retries_left,
                },
            )
        return self.get_job(job_id)

    def mark_job_failed(
        self,
        job_id: str,
        error_message: str,
        *,
        error_type: Optional[str] = None,
        retryable: bool = False,
        worker_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.update_job_status(
            job_id=job_id,
            status="failed",
            worker_id=worker_id,
            error_message=error_message,
            error_type=error_type,
            retryable=retryable,
            event_type="job_failed",
            payload={
                "error_message": error_message,
                "error_type": error_type,
                "retryable": retryable,
            },
        )

    def record_job_result(
        self,
        job_id: str,
        output_location: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now_iso()
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO job_results (job_id, output_location, metadata_json, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id)
                DO UPDATE SET
                    output_location = excluded.output_location,
                    metadata_json = excluded.metadata_json,
                    created_at = excluded.created_at;
                """,
                (job_id, output_location, metadata_json, now),
            )

            conn.execute(
                """
                UPDATE jobs
                SET result_path = ?,
                    status = ?,
                    progress = ?,
                    error_message = NULL,
                    error_type = NULL,
                    retryable = 0,
                    finished_at = ?,
                    updated_at = ?
                WHERE job_id = ?;
                """,
                (output_location, "completed", 100, now, now, job_id),
            )

            self._add_event_with_conn(
                conn,
                job_id=job_id,
                event_type="job_result_recorded",
                status="completed",
                payload={"output_location": output_location},
            )

        return self.get_job(job_id)

    def add_event(
        self,
        job_id: str,
        event_type: str,
        status: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._connection() as conn:
            self._add_event_with_conn(conn, job_id, event_type, status, payload)

    def _add_event_with_conn(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        event_type: str,
        status: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO job_events (job_id, event_type, status, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?);
            """,
            (
                job_id,
                event_type,
                status,
                json.dumps(payload or {}, ensure_ascii=False),
                utc_now_iso(),
            ),
        )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?;",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        with self._connection() as conn:
            if status:
                rows = conn.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?;
                    """,
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM jobs
                    ORDER BY created_at DESC
                    LIMIT ?;
                    """,
                    (limit,),
                ).fetchall()
            return [dict(row) for row in rows]

    def list_job_events(self, job_id: str) -> List[Dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT event_id, job_id, event_type, status, payload_json, created_at
                FROM job_events
                WHERE job_id = ?
                ORDER BY event_id ASC;
                """,
                (job_id,),
            ).fetchall()

        events: List[Dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            payload_json = event.pop("payload_json", None)
            event["payload"] = json.loads(payload_json or "{}")
            events.append(event)
        return events

    def get_job_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT job_id, output_location, metadata_json, created_at
                FROM job_results
                WHERE job_id = ?;
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["metadata"] = json.loads(result.pop("metadata_json") or "{}")
            return result

    def upsert_worker_node(
        self,
        worker_id: str,
        *,
        hostname: Optional[str] = None,
        status: Optional[str] = None,
        current_job_id: Optional[str] = None,
        current_operation: Optional[str] = None,
        cpu_percent: Optional[float] = None,
        memory_percent: Optional[float] = None,
        clear_current_job: bool = False,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now_iso()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO worker_nodes (
                    worker_id, hostname, status, current_job_id, current_operation,
                    cpu_percent, memory_percent, started_at, last_seen, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id)
                DO NOTHING;
                """,
                (
                    worker_id,
                    hostname,
                    status or "ready",
                    current_job_id,
                    current_operation,
                    cpu_percent,
                    memory_percent,
                    now,
                    now,
                    now,
                ),
            )

            assignments = ["last_seen = ?", "updated_at = ?"]
            params: List[Any] = [now, now]

            if hostname is not None:
                assignments.append("hostname = ?")
                params.append(hostname)
            if status is not None:
                assignments.append("status = ?")
                params.append(status)
            if cpu_percent is not None:
                assignments.append("cpu_percent = ?")
                params.append(cpu_percent)
            if memory_percent is not None:
                assignments.append("memory_percent = ?")
                params.append(memory_percent)
            if clear_current_job:
                assignments.append("current_job_id = NULL")
                assignments.append("current_operation = NULL")
            else:
                if current_job_id is not None:
                    assignments.append("current_job_id = ?")
                    params.append(current_job_id)
                if current_operation is not None:
                    assignments.append("current_operation = ?")
                    params.append(current_operation)

            params.append(worker_id)
            conn.execute(
                f"""
                UPDATE worker_nodes
                SET {", ".join(assignments)}
                WHERE worker_id = ?;
                """,
                params,
            )
        return self.get_worker_node(worker_id)

    def get_worker_node(self, worker_id: str) -> Optional[Dict[str, Any]]:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM worker_nodes WHERE worker_id = ?;",
                (worker_id,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)

    def list_worker_nodes(self) -> List[Dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT worker_id, hostname, status, current_job_id, current_operation,
                       cpu_percent, memory_percent, started_at, last_seen, updated_at
                FROM worker_nodes
                ORDER BY worker_id ASC;
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def get_job_status_counts(self) -> Dict[str, int]:
        counts = {status: 0 for status in sorted(VALID_JOB_STATUSES)}
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM jobs
                GROUP BY status;
                """
            ).fetchall()
        for row in rows:
            counts[row["status"]] = row["total"]
        return counts
