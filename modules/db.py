import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DatabaseManager:
    IN_PROGRESS_STATUSES = {
        "parsing": "downloaded",
        "extracting": "parsed",
        "verifying": "extracted",
    }
    VALID_STATUSES = (
        "downloaded",
        "parsing",
        "parsed",
        "extracting",
        "extracted",
        "verifying",
        "verified",
        "synced",
        "failed",
    )

    def __init__(self, db_path: str = "data/epigraph.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        """Returns a configured SQLite connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return dict-like rows
        
        # Performance pragmas
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        """Initializes schema against the DB."""
        schema_path = Path(__file__).parent / "schema.sql"
        if not schema_path.exists():
            logger.error(f"Schema file not found at {schema_path}")
            return
            
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()

        try:
            with self.get_connection() as conn:
                conn.executescript(schema_sql)
                self._migrate_schema(conn)
            logger.info("Database schema initialized successfully.")
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize database: {e}")

    def _migrate_schema(self, conn: sqlite3.Connection):
        """Adds missing columns needed by newer pipeline versions."""
        existing_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(documents)").fetchall()
        }
        required_columns = {
            "claimed_by": "TEXT",
            "claimed_at": "TIMESTAMP",
            "parse_checkpoint_page": "INTEGER NOT NULL DEFAULT 0",
            "page_count": "INTEGER",
        }

        for column_name, ddl in required_columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(f"ALTER TABLE documents ADD COLUMN {column_name} {ddl}")

    def register_document(self, url: str | None, file_hash: str, local_path: str) -> tuple[int | None, bool]:
        """Registers a document and indicates whether it was newly inserted."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO documents (url, file_hash, local_path, status) VALUES (?, ?, ?, 'downloaded')",
                    (url, file_hash, local_path),
                )
                if cursor.rowcount == 0:
                    cursor = conn.execute(
                        "SELECT id FROM documents WHERE file_hash = ?",
                        (file_hash,),
                    )
                    row = cursor.fetchone()
                    return (row["id"] if row else None, False)
                return (cursor.lastrowid, True)
        except sqlite3.Error as e:
            logger.error(f"Failed to register document: {e}")
            return (None, False)

    def insert_document(self, url: str | None, file_hash: str, local_path: str) -> int | None:
        """Registers a downloaded document."""
        doc_id, _ = self.register_document(url, file_hash, local_path)
        return doc_id

    def update_document_status(self, doc_id: int, status: str):
        """State machine tick."""
        if status not in self.VALID_STATUSES:
            logger.warning(f"Invalid status '{status}' for document {doc_id}")
            return

        with self.get_connection() as conn:
            if status in self.IN_PROGRESS_STATUSES:
                conn.execute(
                    "UPDATE documents SET status = ? WHERE id = ?",
                    (status, doc_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE documents
                    SET status = ?, claimed_by = NULL, claimed_at = NULL
                    WHERE id = ?
                    """,
                    (status, doc_id),
                )

    def get_pending_documents(self, state: str) -> List[sqlite3.Row]:
        """Gets all documents currently sitting at a specific state machine tick."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM documents WHERE status = ?", (state,))
            return cursor.fetchall()

    def get_document(self, doc_id: int) -> sqlite3.Row | None:
        with self.get_connection() as conn:
            return conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()

    def get_document_by_local_path_suffix(self, suffix: str) -> sqlite3.Row | None:
        with self.get_connection() as conn:
            return conn.execute(
                "SELECT * FROM documents WHERE local_path LIKE ? ORDER BY id DESC LIMIT 1",
                (f"%{suffix}",),
            ).fetchone()

    def _folder_scope_sql(self, folder_filters: List[str] | None) -> tuple[str, list[str]]:
        if not folder_filters:
            return "", []

        normalized_filters = []
        for folder in folder_filters:
            normalized = str(folder).strip().replace("/", "\\").strip("\\").lower()
            if normalized:
                normalized_filters.append(normalized)

        if not normalized_filters:
            return "", []

        clause = " AND (" + " OR ".join(
            "LOWER(REPLACE(local_path, '/', '\\')) LIKE ?"
            for _ in normalized_filters
        ) + ")"
        params = [f"%\\{folder}\\%" for folder in normalized_filters]
        return clause, params

    def get_status_counts(self, folder_filters: List[str] | None = None) -> Dict[str, int]:
        """Returns document counts grouped by status."""
        scope_clause, scope_params = self._folder_scope_sql(folder_filters)
        with self.get_connection() as conn:
            rows = conn.execute(
                f"SELECT status, COUNT(*) AS count FROM documents WHERE 1=1 {scope_clause} GROUP BY status",
                scope_params,
            ).fetchall()
        return {row["status"]: row["count"] for row in rows}

    def claim_documents(
        self,
        from_status: str,
        to_status: str,
        limit: int,
        claimed_by: str,
        folder_filters: List[str] | None = None,
    ) -> List[sqlite3.Row]:
        """Atomically claims documents for a pipeline stage."""
        if from_status not in self.VALID_STATUSES or to_status not in self.VALID_STATUSES:
            logger.warning(f"Invalid claim transition '{from_status}' -> '{to_status}'")
            return []
        if limit <= 0:
            return []

        scope_clause, scope_params = self._folder_scope_sql(folder_filters)
        sql = """
            WITH picked AS (
                SELECT id
                FROM documents
                WHERE status = ? {scope_clause}
                ORDER BY
                    CASE WHEN parse_checkpoint_page > 0 THEN 0 ELSE 1 END,
                    parse_checkpoint_page DESC,
                    id DESC
                LIMIT ?
            )
            UPDATE documents
            SET status = ?, claimed_by = ?, claimed_at = CURRENT_TIMESTAMP
            WHERE id IN (SELECT id FROM picked)
            RETURNING *
        """
        sql = sql.format(scope_clause=scope_clause)
        with self.get_connection() as conn:
            params = [from_status, *scope_params, limit, to_status, claimed_by]
            return conn.execute(sql, params).fetchall()

    def touch_document_claim(self, doc_id: int, claimed_by: str):
        """Refreshes a document claim heartbeat."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE documents
                SET claimed_by = ?, claimed_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (claimed_by, doc_id),
            )

    def update_parse_progress(
        self,
        doc_id: int,
        completed_pages: int,
        page_count: int,
        claimed_by: str,
    ):
        """Stores parse checkpoints so large PDFs can resume cleanly."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE documents
                SET parse_checkpoint_page = ?, page_count = ?, claimed_by = ?, claimed_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (completed_pages, page_count, claimed_by, doc_id),
            )

    def reset_parse_progress(self, doc_id: int):
        """Clears a broken parse checkpoint when the partial markdown is missing."""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE documents SET parse_checkpoint_page = 0, page_count = NULL WHERE id = ?",
                (doc_id,),
            )

    def get_document_points(
        self,
        doc_id: int,
        run_id: int | None = None,
        active_only: bool = True,
    ) -> List[sqlite3.Row]:
        sql = """
            SELECT *
            FROM knowledge_points
            WHERE document_id = ?
        """
        params: list[Any] = [doc_id]

        if run_id is not None:
            sql += " AND run_id = ?"
            params.append(run_id)
        if active_only:
            sql += " AND superseded_by_run_id IS NULL"

        sql += " ORDER BY id"
        with self.get_connection() as conn:
            return conn.execute(sql, params).fetchall()

    def get_active_knowledge_point_count(self, doc_id: int) -> int:
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM knowledge_points
                WHERE document_id = ? AND superseded_by_run_id IS NULL
                """,
                (doc_id,),
            ).fetchone()
        return int(row["c"]) if row else 0

    def supersede_document_points(
        self,
        doc_id: int,
        superseding_run_id: int,
        keep_run_id: int | None = None,
    ) -> int:
        sql = """
            UPDATE knowledge_points
            SET superseded_by_run_id = ?
            WHERE document_id = ?
              AND superseded_by_run_id IS NULL
        """
        params: list[Any] = [superseding_run_id, doc_id]
        if keep_run_id is not None:
            sql += " AND run_id != ?"
            params.append(keep_run_id)

        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.rowcount

    def retire_run_points(self, doc_id: int, run_id: int) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE knowledge_points
                SET superseded_by_run_id = ?
                WHERE document_id = ?
                  AND run_id = ?
                  AND superseded_by_run_id IS NULL
                """,
                (run_id, doc_id, run_id),
            )
            return cursor.rowcount

    def activate_run_points(self, doc_id: int, run_id: int) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE knowledge_points
                SET superseded_by_run_id = NULL
                WHERE document_id = ?
                  AND run_id = ?
                  AND superseded_by_run_id = ?
                """,
                (doc_id, run_id, run_id),
            )
            return cursor.rowcount

    def resolve_quarantine_for_run(self, doc_id: int, run_id: int) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE quarantine
                SET resolved = TRUE
                WHERE knowledge_point_id IN (
                    SELECT id
                    FROM knowledge_points
                    WHERE document_id = ? AND run_id = ?
                )
                  AND resolved = FALSE
                """,
                (doc_id, run_id),
            )
            return cursor.rowcount

    def resolve_inactive_quarantine(self, doc_id: int | None = None) -> int:
        sql = """
            UPDATE quarantine
            SET resolved = TRUE
            WHERE resolved = FALSE
              AND knowledge_point_id IN (
                  SELECT id
                  FROM knowledge_points
                  WHERE superseded_by_run_id IS NOT NULL
              )
        """
        params: list[Any] = []
        if doc_id is not None:
            sql = """
                UPDATE quarantine
                SET resolved = TRUE
                WHERE resolved = FALSE
                  AND knowledge_point_id IN (
                      SELECT id
                      FROM knowledge_points
                      WHERE document_id = ?
                        AND superseded_by_run_id IS NOT NULL
                  )
            """
            params.append(doc_id)

        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.rowcount

    def requeue_in_progress_documents(self) -> int:
        """
        Moves in-progress documents back to their source states.
        Parsing checkpoints are preserved so the parser can resume from disk.
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE documents
                SET status = CASE status
                    WHEN 'parsing' THEN 'downloaded'
                    WHEN 'extracting' THEN 'parsed'
                    WHEN 'verifying' THEN 'extracted'
                    ELSE status
                END,
                claimed_by = NULL,
                claimed_at = NULL
                WHERE status IN ('parsing', 'extracting', 'verifying')
                """
            )
            return cursor.rowcount
            
    def start_run(self, model_used: str) -> int:
        """Records the start of a new extraction run for versioning."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO runs (model_used) VALUES (?)",
                (model_used,)
            )
            return cursor.lastrowid
            
    def finish_run(self, run_id: int, docs_processed: int):
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE runs SET is_complete = TRUE, documents_processed = ? WHERE id = ?",
                (docs_processed, run_id)
            )

    def insert_knowledge_point(self, doc_id: int, run_id: int, point: Dict[str, Any]) -> int | None:
        """Inserts a validated LLM extraction."""
        sql = """
            INSERT INTO knowledge_points 
            (document_id, run_id, claim_text, category, value, source_url, 
             page_index, snippet_start_offset, snippet_end_offset, snippet, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Keep FTS in sync
        fts_sql = "INSERT INTO knowledge_fts (rowid, claim_text, category, snippet) VALUES (?, ?, ?, ?)"
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(sql, (
                    doc_id,
                    run_id,
                    point['claim'],
                    point['category'],
                    point.get('value'),
                    point['citation']['source_url'],
                    point['citation']['page_index'],
                    point['citation'].get('line_offset') or 0,
                    (point['citation'].get('line_offset') or 0) + len(point['citation']['snippet']),
                    point['citation']['snippet'],
                    point['confidence']
                ))
                
                point_id = cursor.lastrowid
                conn.execute(fts_sql, (point_id, point['claim'], point['category'], point['citation']['snippet']))
                return point_id
                
        except sqlite3.Error as e:
            logger.error(f"Failed to insert knowledge point: {e}")
            return None
            
    def quarantine_point(self, kp_id: int, reason: str):
        """Flags a point that failed strictly citation verification."""
        with self.get_connection() as conn:
            conn.execute(
                "INSERT INTO quarantine (knowledge_point_id, reason) VALUES (?, ?)",
                (kp_id, reason)
            )
            logger.warning(f"Quarantined point {kp_id}: {reason}")
            
    # ------ Data Sync API ------

    def get_synced_claims(self, confidence_threshold: float = 0.8) -> List[Dict]:
        """Gets high-confidence, non-quarantined claims for cloud export."""
        sql = """
            SELECT kp.claim_text, kp.category, kp.value, kp.page_index, kp.snippet, kp.source_url, kp.confidence, kp.created_at
            FROM knowledge_points kp
            LEFT JOIN quarantine q ON kp.id = q.knowledge_point_id
            WHERE kp.confidence >= ? 
              AND q.id IS NULL 
              AND kp.superseded_by_run_id IS NULL
            ORDER BY kp.created_at DESC
        """
        with self.get_connection() as conn:
            return [dict(row) for row in conn.execute(sql, (confidence_threshold,)).fetchall()]

    def get_active_knowledge_points(
        self,
        confidence_threshold: float = 0.0,
        folder_filters: List[str] | None = None,
    ) -> List[Dict[str, Any]]:
        """Returns active knowledge points joined with document metadata."""
        scope_clause, scope_params = self._folder_scope_sql(folder_filters)
        sql = f"""
            SELECT
                kp.id AS knowledge_point_id,
                kp.document_id,
                kp.claim_text,
                kp.category,
                kp.value,
                kp.source_url,
                kp.page_index,
                kp.snippet,
                kp.confidence,
                kp.created_at,
                d.local_path,
                d.url AS document_url
            FROM knowledge_points kp
            JOIN documents d ON d.id = kp.document_id
            LEFT JOIN quarantine q ON q.knowledge_point_id = kp.id
            WHERE kp.confidence >= ?
              AND q.id IS NULL
              AND kp.superseded_by_run_id IS NULL
              {scope_clause}
            ORDER BY kp.created_at DESC, kp.id DESC
        """
        with self.get_connection() as conn:
            rows = conn.execute(sql, [confidence_threshold, *scope_params]).fetchall()
        return [dict(row) for row in rows]

    def get_stats(self) -> Dict[str, Any]:
        """Computes aggregates for the stats.json build."""
        stats = {
            "by_category": {},
            "total_claims": 0,
            "total_documents": 0,
            "quarantined_count": 0
        }
        
        with self.get_connection() as conn:
            # Categories
            for row in conn.execute("SELECT category, COUNT(*) as c FROM knowledge_points WHERE superseded_by_run_id IS NULL GROUP BY category"):
                stats["by_category"][row['category']] = row['c']
                
            stats["total_claims"] = conn.execute("SELECT COUNT(*) FROM knowledge_points WHERE superseded_by_run_id IS NULL").fetchone()[0]
            stats["total_documents"] = conn.execute("SELECT COUNT(*) FROM documents WHERE status = 'synced'").fetchone()[0]
            stats["quarantined_count"] = conn.execute("SELECT COUNT(*) FROM quarantine WHERE resolved = FALSE").fetchone()[0]
            
        return stats
