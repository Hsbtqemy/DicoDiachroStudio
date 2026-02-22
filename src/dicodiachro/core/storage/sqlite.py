from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from ..models import Issue, ParsedEntry, ProfileApplied, ProfileSpec, ProjectPaths, utc_now_iso
from ..utils import alpha_bucket_of, unique_slug

PROJECT_MARKER = "dicodiachro.project.yml"


def project_paths(project_dir: Path) -> ProjectPaths:
    root = project_dir.resolve()
    return ProjectPaths(
        root=root,
        db_path=root / "project.sqlite",
        raw_dir=root / "data" / "raw",
        interim_dir=root / "data" / "interim",
        derived_dir=root / "data" / "derived",
        rules_dir=root / "rules",
        logs_dir=root / "logs",
    )


def init_project(project_dir: Path) -> ProjectPaths:
    paths = project_paths(project_dir)
    for path in [
        paths.raw_dir,
        paths.interim_dir,
        paths.derived_dir,
        paths.rules_dir,
        paths.logs_dir,
        paths.raw_dir / "imports",
    ]:
        path.mkdir(parents=True, exist_ok=True)

    template_dir = Path(__file__).resolve().parents[4] / "rules" / "templates"
    if template_dir.exists():
        for template in template_dir.rglob("*.yml"):
            relative = template.relative_to(template_dir)
            target = paths.rules_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                shutil.copy2(template, target)

    marker = paths.root / PROJECT_MARKER
    if not marker.exists():
        marker.write_text(
            "\n".join(
                [
                    "name: DicoDiachro project",
                    f"created_at: {utc_now_iso()}",
                    "version: 1",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    with connect(paths.db_path) as conn:
        create_schema(conn)

    return paths


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dictionaries (
            dict_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            year INTEGER,
            edition_id TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS profiles (
            profile_id TEXT PRIMARY KEY,
            dict_id TEXT,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(dict_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS profile_applications (
            application_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dict_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            profile_version INTEGER NOT NULL,
            profile_sha256 TEXT NOT NULL,
            entries_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(dict_id) REFERENCES dictionaries(dict_id),
            FOREIGN KEY(profile_id) REFERENCES profiles(profile_id)
        );

        CREATE TABLE IF NOT EXISTS convention_applications (
            convention_application_id INTEGER PRIMARY KEY AUTOINCREMENT,
            corpus_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            profile_version INTEGER NOT NULL,
            profile_sha256 TEXT NOT NULL,
            entries_count INTEGER NOT NULL,
            issues_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(corpus_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS entries (
            entry_id TEXT PRIMARY KEY,
            dict_id TEXT NOT NULL,
            section TEXT,
            syllables INTEGER NOT NULL,
            headword_raw TEXT NOT NULL,
            pos_raw TEXT NOT NULL,
            pron_raw TEXT,
            origin_raw TEXT,
            origin_norm TEXT,
            pos_norm TEXT,
            parser_id TEXT,
            parser_version INTEGER,
            parser_sha256 TEXT,
            form_display TEXT,
            form_norm TEXT,
            headword_norm TEXT,
            pron_norm TEXT,
            pron_render TEXT,
            features_json TEXT,
            profile_id TEXT,
            profile_version INTEGER,
            profile_sha256 TEXT,
            headword_edit TEXT,
            pron_edit TEXT,
            definition_edit TEXT,
            status TEXT DEFAULT 'auto',
            is_deleted INTEGER NOT NULL DEFAULT 0,
            manual_created INTEGER NOT NULL DEFAULT 0,
            alpha_bucket TEXT,
            deleted_at TEXT,
            deleted_reason TEXT,
            source_id TEXT,
            record_key TEXT,
            source_path TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(dict_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS issues (
            issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dict_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            kind TEXT NOT NULL,
            code TEXT NOT NULL,
            raw TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(dict_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS lemma_groups (
            lemma_group_id TEXT PRIMARY KEY,
            lemma_label TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS lemma_members (
            lemma_group_id TEXT NOT NULL,
            dict_id TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            score REAL,
            status TEXT NOT NULL,
            notes TEXT,
            PRIMARY KEY (lemma_group_id, dict_id, entry_id),
            FOREIGN KEY(lemma_group_id) REFERENCES lemma_groups(lemma_group_id),
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id),
            FOREIGN KEY(dict_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS comparison_sessions (
            session_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            config_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS compare_runs (
            run_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            corpus_ids_json TEXT NOT NULL,
            key_field TEXT NOT NULL,
            mode TEXT NOT NULL,
            fuzzy_threshold INTEGER NOT NULL,
            algorithm TEXT NOT NULL,
            settings_sha256 TEXT NOT NULL,
            stats_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS compare_coverage_items (
            compare_coverage_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            headword_key TEXT NOT NULL,
            corpus_id TEXT NOT NULL,
            present INTEGER NOT NULL,
            FOREIGN KEY(run_id) REFERENCES compare_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS compare_alignment_pairs (
            compare_alignment_pair_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            corpus_a TEXT NOT NULL,
            corpus_b TEXT NOT NULL,
            headword_key TEXT NOT NULL,
            entry_id_a TEXT,
            entry_id_b TEXT,
            headword_norm_a TEXT,
            headword_norm_b TEXT,
            score REAL NOT NULL,
            method TEXT NOT NULL,
            reason TEXT,
            status_a TEXT,
            status_b TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES compare_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS compare_diff_rows (
            compare_diff_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            headword_key TEXT NOT NULL,
            entry_id_a TEXT,
            entry_id_b TEXT,
            pron_norm_a TEXT,
            pron_norm_b TEXT,
            pron_render_a TEXT,
            pron_render_b TEXT,
            features_a_json TEXT NOT NULL,
            features_b_json TEXT NOT NULL,
            delta_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES compare_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS project_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS corpus_templates (
            template_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            corpus_id TEXT NOT NULL,
            template_id TEXT NOT NULL,
            template_kind TEXT NOT NULL,
            version INTEGER NOT NULL,
            params_json TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(corpus_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS template_applications (
            template_application_id INTEGER PRIMARY KEY AUTOINCREMENT,
            corpus_id TEXT NOT NULL,
            template_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            params_json TEXT NOT NULL,
            source_ids_json TEXT,
            records_count INTEGER NOT NULL,
            entries_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(corpus_id) REFERENCES dictionaries(dict_id)
        );

        CREATE TABLE IF NOT EXISTS entry_overrides (
            override_id INTEGER PRIMARY KEY AUTOINCREMENT,
            corpus_id TEXT NOT NULL,
            scope TEXT NOT NULL CHECK(scope IN ('record', 'entry')),
            source_id TEXT,
            record_key TEXT,
            entry_id TEXT,
            op TEXT NOT NULL,
            before_json TEXT NOT NULL,
            after_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            note TEXT,
            FOREIGN KEY(corpus_id) REFERENCES dictionaries(dict_id)
        );

        CREATE INDEX IF NOT EXISTS idx_entries_dict ON entries(dict_id);
        CREATE INDEX IF NOT EXISTS idx_entries_dict_deleted ON entries(dict_id, is_deleted);
        CREATE INDEX IF NOT EXISTS idx_entries_norm ON entries(form_norm);
        CREATE INDEX IF NOT EXISTS idx_issues_dict ON issues(dict_id);
        CREATE INDEX IF NOT EXISTS idx_issues_code ON issues(code);
        CREATE INDEX IF NOT EXISTS idx_profile_apps_dict ON profile_applications(dict_id);
        CREATE INDEX IF NOT EXISTS idx_profile_apps_profile ON profile_applications(profile_id);
        CREATE INDEX IF NOT EXISTS idx_convention_apps_corpus ON convention_applications(corpus_id);
        CREATE INDEX IF NOT EXISTS idx_convention_apps_profile ON convention_applications(profile_id);
        CREATE INDEX IF NOT EXISTS idx_members_dict ON lemma_members(dict_id);
        CREATE INDEX IF NOT EXISTS idx_compare_runs_created ON compare_runs(created_at);
        CREATE INDEX IF NOT EXISTS idx_compare_coverage_run_key ON compare_coverage_items(run_id, headword_key);
        CREATE INDEX IF NOT EXISTS idx_compare_coverage_run_corpus ON compare_coverage_items(run_id, corpus_id);
        CREATE INDEX IF NOT EXISTS idx_compare_align_run_pair ON compare_alignment_pairs(run_id, corpus_a, corpus_b);
        CREATE INDEX IF NOT EXISTS idx_compare_align_run_key ON compare_alignment_pairs(run_id, headword_key);
        CREATE INDEX IF NOT EXISTS idx_compare_diff_run_key ON compare_diff_rows(run_id, headword_key);
        CREATE INDEX IF NOT EXISTS idx_corpus_templates_corpus ON corpus_templates(corpus_id);
        CREATE INDEX IF NOT EXISTS idx_corpus_templates_active ON corpus_templates(corpus_id, is_active);
        CREATE INDEX IF NOT EXISTS idx_template_apps_corpus ON template_applications(corpus_id);
        CREATE INDEX IF NOT EXISTS idx_entry_overrides_corpus_scope ON entry_overrides(corpus_id, scope);
        CREATE INDEX IF NOT EXISTS idx_entry_overrides_source_key ON entry_overrides(source_id, record_key);
        CREATE INDEX IF NOT EXISTS idx_entry_overrides_entry ON entry_overrides(entry_id);
        CREATE INDEX IF NOT EXISTS idx_entries_dict_manual_created ON entries(dict_id, manual_created);
        """
    )
    _ensure_entries_columns(conn)
    _ensure_entries_indexes(conn)
    conn.commit()


def _ensure_entries_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(entries)").fetchall()
        if row["name"] is not None
    }
    expected_columns = {
        "origin_raw": "TEXT",
        "origin_norm": "TEXT",
        "pos_norm": "TEXT",
        "parser_id": "TEXT",
        "parser_version": "INTEGER",
        "parser_sha256": "TEXT",
        "definition_raw": "TEXT",
        "source_record": "TEXT",
        "template_id": "TEXT",
        "template_version": "INTEGER",
        "template_sha256": "TEXT",
        "headword_norm": "TEXT",
        "pron_norm": "TEXT",
        "pron_render": "TEXT",
        "profile_id": "TEXT",
        "profile_version": "INTEGER",
        "profile_sha256": "TEXT",
        "headword_edit": "TEXT",
        "pron_edit": "TEXT",
        "definition_edit": "TEXT",
        "status": "TEXT DEFAULT 'auto'",
        "is_deleted": "INTEGER DEFAULT 0",
        "manual_created": "INTEGER DEFAULT 0",
        "alpha_bucket": "TEXT",
        "deleted_at": "TEXT",
        "deleted_reason": "TEXT",
        "source_id": "TEXT",
        "record_key": "TEXT",
    }
    for column_name, column_type in expected_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE entries ADD COLUMN {column_name} {column_type}")
    if "status" in existing_columns or "status" in expected_columns:
        conn.execute("UPDATE entries SET status='auto' WHERE status IS NULL OR status=''")
    if "is_deleted" in existing_columns or "is_deleted" in expected_columns:
        conn.execute("UPDATE entries SET is_deleted=0 WHERE is_deleted IS NULL")
    if "manual_created" in existing_columns or "manual_created" in expected_columns:
        conn.execute("UPDATE entries SET manual_created=0 WHERE manual_created IS NULL")
    rows_to_fix = conn.execute(
        """
        SELECT entry_id, dict_id, headword_raw, headword_edit, headword_norm
        FROM entries
        WHERE alpha_bucket IS NULL OR TRIM(alpha_bucket)=''
        """
    ).fetchall()
    for row in rows_to_fix:
        conn.execute(
            "UPDATE entries SET alpha_bucket=? WHERE entry_id=? AND dict_id=?",
            (
                _compute_alpha_bucket(
                    headword_raw=row["headword_raw"],
                    headword_edit=row["headword_edit"],
                    headword_norm=row["headword_norm"],
                ),
                row["entry_id"],
                row["dict_id"],
            ),
        )


def _ensure_entries_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_dict_alpha ON entries(dict_id, alpha_bucket)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_dict_alpha_norm ON entries(dict_id, alpha_bucket, headword_norm)"
    )


def _compute_alpha_bucket(
    *,
    headword_raw: str | None,
    headword_edit: str | None = None,
    headword_norm: str | None = None,
) -> str:
    normalized = str(headword_norm or "").strip()
    edited = str(headword_edit or "").strip()
    raw = str(headword_raw or "").strip()
    seed = normalized or edited or raw
    return alpha_bucket_of(seed)


def entry_id_for(entry: ParsedEntry) -> str:
    payload = "|".join(
        [
            entry.dict_id,
            entry.source_path,
            str(entry.line_no),
            entry.headword_raw,
            entry.pos_raw,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def lemma_group_id_for(label_norm: str) -> str:
    return hashlib.sha256(label_norm.encode("utf-8")).hexdigest()


class SQLiteStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        with connect(self.db_path) as conn:
            create_schema(conn)

    def ensure_dictionary(
        self,
        dict_id: str,
        label: str | None = None,
        year: int | None = None,
        edition_id: str | None = None,
        notes: str | None = None,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO dictionaries(dict_id, label, year, edition_id, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dict_id, label or dict_id, year, edition_id, notes, utc_now_iso()),
            )
            conn.commit()

    def create_dictionary(self, label: str) -> str:
        existing = {row["dict_id"] for row in self.list_dictionaries()}
        dict_id = unique_slug(label, existing)
        self.ensure_dictionary(dict_id=dict_id, label=label or dict_id)
        return dict_id

    def rename_dictionary_label(self, dict_id: str, label: str) -> None:
        cleaned_id = dict_id.strip()
        cleaned_label = label.strip()
        if not cleaned_id or not cleaned_label:
            return
        with connect(self.db_path) as conn:
            conn.execute(
                "UPDATE dictionaries SET label=? WHERE dict_id=?",
                (cleaned_label, cleaned_id),
            )
            conn.commit()

    def get_active_dict_id(self) -> str | None:
        with connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM project_settings WHERE key='active_dict_id'"
            ).fetchone()
        if not row:
            return None
        active = str(row["value"]).strip()
        if not active:
            return None
        return active

    def set_active_dict_id(self, dict_id: str) -> None:
        cleaned = dict_id.strip()
        if not cleaned:
            return
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO project_settings(key, value, updated_at)
                VALUES('active_dict_id', ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_at=excluded.updated_at
                """,
                (cleaned, utc_now_iso()),
            )
            conn.commit()

    def ensure_active_dict(self, suggested_name: str = "Corpus") -> str:
        active = self.get_active_dict_id()
        existing = [row["dict_id"] for row in self.list_dictionaries()]

        if active and active in existing:
            return active

        if existing:
            selected = existing[0]
            self.set_active_dict_id(selected)
            return selected

        created = self.create_dictionary(suggested_name)
        self.set_active_dict_id(created)
        return created

    def save_active_template(
        self,
        corpus_id: str,
        template_id: str,
        template_kind: str,
        version: int,
        params: dict[str, Any],
        sha256: str,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute("UPDATE corpus_templates SET is_active=0 WHERE corpus_id=?", (corpus_id,))
            conn.execute(
                """
                INSERT INTO corpus_templates(
                  corpus_id, template_id, template_kind, version,
                  params_json, sha256, created_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    corpus_id,
                    template_id,
                    template_kind,
                    version,
                    json.dumps(params, ensure_ascii=False, sort_keys=True),
                    sha256,
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def get_active_template(self, corpus_id: str) -> sqlite3.Row | None:
        with connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM corpus_templates
                WHERE corpus_id=? AND is_active=1
                ORDER BY template_row_id DESC
                LIMIT 1
                """,
                (corpus_id,),
            ).fetchone()
        return row

    def record_template_application(
        self,
        corpus_id: str,
        template_id: str,
        version: int,
        sha256: str,
        params: dict[str, Any],
        source_ids: list[str] | None,
        records_count: int,
        entries_count: int,
        status: str,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO template_applications(
                  corpus_id, template_id, version, sha256, params_json, source_ids_json,
                  records_count, entries_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    corpus_id,
                    template_id,
                    version,
                    sha256,
                    json.dumps(params, ensure_ascii=False, sort_keys=True),
                    json.dumps(source_ids or [], ensure_ascii=False, sort_keys=True),
                    records_count,
                    entries_count,
                    status,
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def list_template_applications(self, corpus_id: str, limit: int = 20) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM template_applications
                WHERE corpus_id=?
                ORDER BY template_application_id DESC
                LIMIT ?
                """,
                (corpus_id, limit),
            ).fetchall()
        return rows

    def upsert_profile(self, profile: ProfileSpec, path: Path, sha256: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO profiles(profile_id, dict_id, name, version, path, sha256, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id) DO UPDATE SET
                  dict_id=excluded.dict_id,
                  name=excluded.name,
                  version=excluded.version,
                  path=excluded.path,
                  sha256=excluded.sha256
                """,
                (
                    profile.profile_id,
                    profile.dict_id,
                    profile.name,
                    str(profile.version),
                    str(path),
                    sha256,
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def record_profile_application(
        self,
        dict_id: str,
        profile_id: str,
        profile_version: int,
        profile_sha256: str,
        entries_count: int,
        status: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO profile_applications(
                  dict_id, profile_id, profile_version, profile_sha256,
                  entries_count, status, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dict_id,
                    profile_id,
                    profile_version,
                    profile_sha256,
                    entries_count,
                    status,
                    json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def list_profile_applications(self, dict_id: str, limit: int = 20) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM profile_applications
                WHERE dict_id=?
                ORDER BY application_id DESC
                LIMIT ?
                """,
                (dict_id, limit),
            ).fetchall()
        return rows

    def record_convention_application(
        self,
        corpus_id: str,
        profile_id: str,
        profile_version: int,
        profile_sha256: str,
        entries_count: int,
        issues_count: int,
        status: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO convention_applications(
                  corpus_id, profile_id, profile_version, profile_sha256,
                  entries_count, issues_count, status, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    corpus_id,
                    profile_id,
                    profile_version,
                    profile_sha256,
                    entries_count,
                    issues_count,
                    status,
                    json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def list_convention_applications(self, corpus_id: str, limit: int = 30) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM convention_applications
                WHERE corpus_id=?
                ORDER BY convention_application_id DESC
                LIMIT ?
                """,
                (corpus_id, limit),
            ).fetchall()
        return rows

    def clear_dict_entries(self, dict_id: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute("DELETE FROM entries WHERE dict_id=?", (dict_id,))
            conn.execute("DELETE FROM issues WHERE dict_id=?", (dict_id,))
            conn.execute("DELETE FROM lemma_members WHERE dict_id=?", (dict_id,))
            conn.commit()

    def clear_issues_by_codes(self, dict_id: str, codes: set[str]) -> None:
        if not codes:
            return
        placeholders = ",".join(["?"] * len(codes))
        with connect(self.db_path) as conn:
            conn.execute(
                f"DELETE FROM issues WHERE dict_id=? AND code IN ({placeholders})",
                (dict_id, *sorted(codes)),
            )
            conn.commit()

    def insert_entries(
        self,
        entries: list[ParsedEntry],
        profile_results: dict[str, ProfileApplied] | None = None,
    ) -> None:
        profile_results = profile_results or {}
        with connect(self.db_path) as conn:
            rows = []
            for entry in entries:
                eid = entry_id_for(entry)
                applied = profile_results.get(eid)
                alpha_bucket = _compute_alpha_bucket(headword_raw=entry.headword_raw)
                rows.append(
                    (
                        eid,
                        entry.dict_id,
                        entry.section,
                        entry.syllables,
                        entry.headword_raw,
                        entry.pos_raw,
                        entry.pron_raw,
                        entry.origin_raw,
                        entry.origin_norm,
                        entry.pos_norm,
                        entry.parser_id,
                        entry.parser_version,
                        entry.parser_sha256,
                        entry.definition_raw,
                        entry.source_record,
                        entry.template_id,
                        entry.template_version,
                        entry.template_sha256,
                        applied.form_display if applied else None,
                        applied.form_norm if applied else None,
                        None,
                        applied.form_norm if applied else None,
                        applied.form_render if applied else None,
                        (
                            json.dumps(applied.features, ensure_ascii=False, sort_keys=True)
                            if applied
                            else "{}"
                        ),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "auto",
                        alpha_bucket,
                        entry.source_id or entry.source_path,
                        entry.record_key,
                        entry.source_path,
                        entry.line_no,
                        utc_now_iso(),
                    )
                )
            placeholders = ",".join(["?"] * 37)
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO entries(
                  entry_id, dict_id, section, syllables, headword_raw, pos_raw, pron_raw,
                  origin_raw, origin_norm, pos_norm, parser_id, parser_version, parser_sha256,
                  definition_raw, source_record, template_id, template_version, template_sha256,
                  form_display, form_norm, headword_norm, pron_norm, pron_render, features_json,
                  profile_id, profile_version, profile_sha256,
                  headword_edit, pron_edit, definition_edit, status, alpha_bucket, source_id, record_key,
                  source_path, line_no, created_at
                ) VALUES ({placeholders})
                """,
                rows,
            )
            conn.commit()

    def insert_entry(
        self,
        *,
        dict_id: str,
        headword_raw: str,
        pos_raw: str = "p",
        pron_raw: str | None = None,
        definition_raw: str | None = None,
        source_id: str | None = None,
        record_key: str | None = None,
        source_path: str | None = None,
        source_record: str | None = None,
        line_no: int | None = None,
        status: str = "reviewed",
        manual_created: bool = True,
        section: str = "",
        syllables: int = 1,
    ) -> str:
        cleaned_dict_id = str(dict_id or "").strip()
        cleaned_headword = str(headword_raw or "").strip()
        cleaned_pos = str(pos_raw or "").strip() or "p"
        cleaned_status = str(status or "").strip() or "reviewed"
        if not cleaned_dict_id:
            raise ValueError("dict_id is required")
        if not cleaned_headword:
            raise ValueError("headword_raw is required")

        with connect(self.db_path) as conn:
            if line_no is None:
                row = conn.execute(
                    "SELECT COALESCE(MAX(line_no), 0) AS n FROM entries WHERE dict_id=?",
                    (cleaned_dict_id,),
                ).fetchone()
                line_no_value = int((row["n"] if row else 0) or 0) + 1
            else:
                line_no_value = int(line_no)

            source_path_value = str(source_path or "").strip()
            if not source_path_value:
                source_path_value = f"manual://{cleaned_dict_id}/curation"

            source_id_value = str(source_id or "").strip() or source_path_value
            record_key_value = str(record_key or "").strip() or None
            source_record_value = str(source_record or "").strip() or cleaned_headword
            alpha_bucket = _compute_alpha_bucket(headword_raw=cleaned_headword)

            entry_id_payload = "|".join(
                [
                    cleaned_dict_id,
                    source_path_value,
                    str(line_no_value),
                    cleaned_headword,
                    cleaned_pos,
                    str(pron_raw or ""),
                    str(definition_raw or ""),
                    utc_now_iso(),
                ]
            )
            entry_id = hashlib.sha256(entry_id_payload.encode("utf-8")).hexdigest()

            conn.execute(
                """
                INSERT INTO entries(
                  entry_id, dict_id, section, syllables, headword_raw, pos_raw, pron_raw,
                  definition_raw, source_record, status, is_deleted, manual_created, alpha_bucket,
                  source_id, record_key, source_path, line_no, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry_id,
                    cleaned_dict_id,
                    section,
                    int(syllables),
                    cleaned_headword,
                    cleaned_pos,
                    (str(pron_raw).strip() if pron_raw is not None else None),
                    (str(definition_raw).strip() if definition_raw is not None else None),
                    source_record_value,
                    cleaned_status,
                    0,
                    1 if manual_created else 0,
                    alpha_bucket,
                    source_id_value,
                    record_key_value,
                    source_path_value,
                    line_no_value,
                    utc_now_iso(),
                ),
            )
            conn.commit()
        return entry_id

    def update_profile_fields(
        self,
        dict_id: str,
        applied_by_entry_id: dict[str, ProfileApplied],
    ) -> None:
        with connect(self.db_path) as conn:
            rows = [
                (
                    applied.form_display,
                    applied.form_norm,
                    applied.form_norm,
                    applied.form_render,
                    json.dumps(applied.features, ensure_ascii=False, sort_keys=True),
                    entry_id,
                    dict_id,
                )
                for entry_id, applied in applied_by_entry_id.items()
            ]
            conn.executemany(
                """
                UPDATE entries
                SET form_display=?, form_norm=?, pron_norm=?, pron_render=?, features_json=?
                WHERE entry_id=? AND dict_id=?
                """,
                rows,
            )
            conn.commit()

    def update_convention_fields(
        self,
        dict_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        with connect(self.db_path) as conn:
            conn.executemany(
                """
                UPDATE entries
                SET
                  form_display=?,
                  form_norm=?,
                  headword_norm=?,
                  alpha_bucket=?,
                  pron_norm=?,
                  pron_render=?,
                  features_json=?,
                  profile_id=?,
                  profile_version=?,
                  profile_sha256=?
                WHERE entry_id=? AND dict_id=?
                """,
                [
                    (
                        row["form_display"],
                        row["form_norm"],
                        row["headword_norm"],
                        _compute_alpha_bucket(
                            headword_raw=row.get("headword_raw"),
                            headword_edit=row.get("headword_effective"),
                            headword_norm=row.get("headword_norm"),
                        ),
                        row["pron_norm"],
                        row["pron_render"],
                        row["features_json"],
                        row["profile_id"],
                        row["profile_version"],
                        row["profile_sha256"],
                        row["entry_id"],
                        dict_id,
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def update_entry_edit_fields(
        self,
        entry_id: str,
        dict_id: str,
        field_changes: dict[str, Any],
    ) -> None:
        allowed_fields = {"headword_edit", "pron_edit", "definition_edit", "status"}
        updates: dict[str, Any] = {
            key: value for key, value in field_changes.items() if key in allowed_fields
        }
        if not updates:
            return

        clauses = ", ".join([f"{key}=?" for key in updates.keys()])
        with connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE entries SET {clauses} WHERE entry_id=? AND dict_id=?",
                (*updates.values(), entry_id, dict_id),
            )
            if "headword_edit" in updates:
                row = conn.execute(
                    """
                    SELECT headword_raw, headword_edit, headword_norm
                    FROM entries
                    WHERE entry_id=? AND dict_id=?
                    """,
                    (entry_id, dict_id),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        "UPDATE entries SET alpha_bucket=? WHERE entry_id=? AND dict_id=?",
                        (
                            _compute_alpha_bucket(
                                headword_raw=row["headword_raw"],
                                headword_edit=row["headword_edit"],
                                headword_norm=row["headword_norm"],
                            ),
                            entry_id,
                            dict_id,
                        ),
                    )
            conn.commit()

    def update_entry_raw_fields(
        self,
        entry_id: str,
        dict_id: str,
        field_changes: dict[str, Any],
    ) -> None:
        allowed_fields = {"headword_raw", "pron_raw", "definition_raw"}
        updates: dict[str, Any] = {
            key: value for key, value in field_changes.items() if key in allowed_fields
        }
        if not updates:
            return

        clauses = ", ".join([f"{key}=?" for key in updates.keys()])
        with connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE entries SET {clauses} WHERE entry_id=? AND dict_id=?",
                (*updates.values(), entry_id, dict_id),
            )
            if "headword_raw" in updates:
                row = conn.execute(
                    """
                    SELECT headword_raw, headword_edit, headword_norm
                    FROM entries
                    WHERE entry_id=? AND dict_id=?
                    """,
                    (entry_id, dict_id),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        "UPDATE entries SET alpha_bucket=? WHERE entry_id=? AND dict_id=?",
                        (
                            _compute_alpha_bucket(
                                headword_raw=row["headword_raw"],
                                headword_edit=row["headword_edit"],
                                headword_norm=row["headword_norm"],
                            ),
                            entry_id,
                            dict_id,
                        ),
                    )
            conn.commit()

    def update_entries_delete_state(
        self,
        *,
        dict_id: str,
        entry_ids: list[str],
        is_deleted: bool,
        deleted_at: str | None,
        deleted_reason: str | None,
    ) -> int:
        cleaned_ids = [str(entry_id).strip() for entry_id in entry_ids if str(entry_id).strip()]
        if not cleaned_ids:
            return 0
        updated = 0
        with connect(self.db_path) as conn:
            for entry_id in cleaned_ids:
                cursor = conn.execute(
                    """
                    UPDATE entries
                    SET is_deleted=?, deleted_at=?, deleted_reason=?
                    WHERE entry_id=? AND dict_id=?
                    """,
                    (
                        1 if is_deleted else 0,
                        deleted_at,
                        deleted_reason,
                        entry_id,
                        dict_id,
                    ),
                )
                updated += int(cursor.rowcount or 0)
            conn.commit()
        return updated

    def delete_entries(self, entry_ids: list[str]) -> None:
        if not entry_ids:
            return
        placeholders = ",".join(["?"] * len(entry_ids))
        with connect(self.db_path) as conn:
            conn.execute(
                f"DELETE FROM entries WHERE entry_id IN ({placeholders})", tuple(entry_ids)
            )
            conn.commit()

    def insert_override(
        self,
        corpus_id: str,
        scope: str,
        source_id: str | None,
        record_key: str | None,
        entry_id: str | None,
        op: str,
        before_json: dict[str, Any] | None,
        after_json: dict[str, Any] | None,
        note: str | None = None,
    ) -> int:
        with connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO entry_overrides(
                  corpus_id, scope, source_id, record_key, entry_id, op,
                  before_json, after_json, created_at, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    corpus_id,
                    scope,
                    source_id,
                    record_key,
                    entry_id,
                    op,
                    json.dumps(before_json, ensure_ascii=False, sort_keys=True),
                    json.dumps(after_json, ensure_ascii=False, sort_keys=True),
                    utc_now_iso(),
                    note,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def list_overrides(
        self,
        corpus_id: str,
        scope: str,
        source_id: str | None = None,
        entry_id: str | None = None,
        limit: int = 500,
    ) -> list[sqlite3.Row]:
        query = "SELECT * FROM entry_overrides WHERE corpus_id=? AND scope=?"
        params: list[Any] = [corpus_id, scope]
        if source_id is not None:
            query += " AND source_id=?"
            params.append(source_id)
        if entry_id is not None:
            query += " AND entry_id=?"
            params.append(entry_id)
        query += " ORDER BY override_id DESC LIMIT ?"
        params.append(limit)
        with connect(self.db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return rows

    def delete_override_by_id(self, override_id: int) -> None:
        with connect(self.db_path) as conn:
            conn.execute("DELETE FROM entry_overrides WHERE override_id=?", (override_id,))
            conn.commit()

    def delete_record_overrides(self, corpus_id: str, source_id: str, record_key: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                DELETE FROM entry_overrides
                WHERE corpus_id=? AND scope='record' AND source_id=? AND record_key=?
                """,
                (corpus_id, source_id, record_key),
            )
            conn.commit()

    def delete_entry_overrides(self, corpus_id: str, entry_id: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM entry_overrides WHERE corpus_id=? AND scope='entry' AND entry_id=?",
                (corpus_id, entry_id),
            )
            conn.commit()

    def insert_issues(self, issues: list[Issue]) -> None:
        if not issues:
            return
        with connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO issues(dict_id, source_path, line_no, kind, code, raw, details_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        issue.dict_id,
                        issue.source_path,
                        issue.line_no,
                        issue.kind,
                        issue.code,
                        issue.raw,
                        json.dumps(issue.details, ensure_ascii=False, sort_keys=True),
                        issue.created_at,
                    )
                    for issue in issues
                ],
            )
            conn.commit()

    def list_dictionaries(self) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT dict_id, label, year, edition_id, notes FROM dictionaries ORDER BY dict_id"
            ).fetchall()
        return rows

    def list_profiles(self, dict_id: str | None = None) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            if dict_id:
                rows = conn.execute(
                    "SELECT * FROM profiles WHERE dict_id=? OR dict_id IS NULL ORDER BY profile_id",
                    (dict_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM profiles ORDER BY profile_id").fetchall()
        return rows

    def list_entries(
        self,
        dict_id: str,
        limit: int = 200,
        offset: int = 0,
        search: str | None = None,
        include_deleted: bool = False,
        alpha_bucket: str | None = None,
    ) -> list[sqlite3.Row]:
        deleted_filter = "" if include_deleted else " AND COALESCE(is_deleted, 0)=0"
        bucket_filter = ""
        params_prefix: list[Any] = [dict_id]
        cleaned_bucket = str(alpha_bucket or "").strip().upper()
        if cleaned_bucket:
            bucket_filter = " AND alpha_bucket=?"
            params_prefix.append(cleaned_bucket)
        with connect(self.db_path) as conn:
            if search:
                query = (
                    f"SELECT * FROM entries WHERE dict_id=?{deleted_filter}{bucket_filter} AND "
                    "(headword_raw LIKE ? OR pron_raw LIKE ? OR definition_raw LIKE ? OR "
                    "headword_edit LIKE ? OR pron_edit LIKE ? OR definition_edit LIKE ? OR "
                    "form_display LIKE ? OR form_norm LIKE ? OR "
                    "headword_norm LIKE ? OR pron_norm LIKE ? OR pron_render LIKE ?) "
                    "ORDER BY section, line_no, entry_id LIMIT ? OFFSET ?"
                )
                needle = f"%{search}%"
                rows = conn.execute(
                    query,
                    (
                        *params_prefix,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        needle,
                        limit,
                        offset,
                    ),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT * FROM entries WHERE dict_id=?{deleted_filter}{bucket_filter} "
                    "ORDER BY section, line_no, entry_id LIMIT ? OFFSET ?",
                    (*params_prefix, limit, offset),
                ).fetchall()
        return rows

    def count_entries(
        self,
        dict_id: str,
        include_deleted: bool = False,
        alpha_bucket: str | None = None,
    ) -> int:
        deleted_filter = "" if include_deleted else " AND COALESCE(is_deleted, 0)=0"
        params: list[Any] = [dict_id]
        bucket_filter = ""
        cleaned_bucket = str(alpha_bucket or "").strip().upper()
        if cleaned_bucket:
            bucket_filter = " AND alpha_bucket=?"
            params.append(cleaned_bucket)
        with connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM entries WHERE dict_id=?{deleted_filter}{bucket_filter}",
                tuple(params),
            ).fetchone()
        return int(row["n"]) if row else 0

    def alpha_counts(self, dict_id: str, include_deleted: bool = False) -> dict[str, int]:
        deleted_filter = "" if include_deleted else " AND COALESCE(is_deleted, 0)=0"
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT alpha_bucket, COUNT(*) AS n
                FROM entries
                WHERE dict_id=?{deleted_filter}
                GROUP BY alpha_bucket
                """,
                (dict_id,),
            ).fetchall()

        counts: dict[str, int] = {}
        for row in rows:
            bucket = str(row["alpha_bucket"] or "").strip().upper() or "#"
            counts[bucket] = int(row["n"] or 0)
        return counts

    def count_issues(self, dict_id: str | None = None) -> int:
        with connect(self.db_path) as conn:
            if dict_id:
                row = conn.execute(
                    "SELECT COUNT(*) AS n FROM issues WHERE dict_id=?", (dict_id,)
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS n FROM issues").fetchone()
        return int(row["n"]) if row else 0

    def entry_details(self, entry_id: str) -> tuple[sqlite3.Row | None, list[sqlite3.Row]]:
        with connect(self.db_path) as conn:
            entry = conn.execute("SELECT * FROM entries WHERE entry_id=?", (entry_id,)).fetchone()
            issues = (
                conn.execute(
                    "SELECT * FROM issues WHERE source_path=? AND line_no=? ORDER BY issue_id",
                    (entry["source_path"], entry["line_no"]),
                ).fetchall()
                if entry
                else []
            )
        return entry, issues

    def entries_for_dict(self, dict_id: str, include_deleted: bool = False) -> list[sqlite3.Row]:
        deleted_filter = "" if include_deleted else " AND COALESCE(is_deleted, 0)=0"
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM entries WHERE dict_id=?{deleted_filter} ORDER BY line_no, entry_id",
                (dict_id,),
            ).fetchall()
        return rows

    def entry_by_id(self, entry_id: str) -> sqlite3.Row | None:
        with connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM entries WHERE entry_id=?", (entry_id,)).fetchone()
        return row

    def save_compare_run(
        self,
        *,
        run_id: str,
        corpus_ids: list[str],
        key_field: str,
        mode: str,
        fuzzy_threshold: int,
        algorithm: str,
        settings_sha256: str,
        stats: dict[str, Any],
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO compare_runs(
                  run_id, created_at, corpus_ids_json, key_field, mode,
                  fuzzy_threshold, algorithm, settings_sha256, stats_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    utc_now_iso(),
                    json.dumps(corpus_ids, ensure_ascii=False, sort_keys=True),
                    key_field,
                    mode,
                    int(fuzzy_threshold),
                    algorithm,
                    settings_sha256,
                    json.dumps(stats, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def clear_compare_run_data(self, run_id: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute("DELETE FROM compare_coverage_items WHERE run_id=?", (run_id,))
            conn.execute("DELETE FROM compare_alignment_pairs WHERE run_id=?", (run_id,))
            conn.execute("DELETE FROM compare_diff_rows WHERE run_id=?", (run_id,))
            conn.commit()

    def insert_compare_coverage_items(
        self,
        run_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        with connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO compare_coverage_items(run_id, headword_key, corpus_id, present)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        str(row["headword_key"]),
                        str(row["corpus_id"]),
                        1 if bool(row.get("present", False)) else 0,
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_compare_alignment_pairs(
        self,
        run_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        with connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO compare_alignment_pairs(
                  run_id, corpus_a, corpus_b, headword_key,
                  entry_id_a, entry_id_b, headword_norm_a, headword_norm_b,
                  score, method, reason, status_a, status_b, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        str(row["corpus_a"]),
                        str(row["corpus_b"]),
                        str(row["headword_key"]),
                        row.get("entry_id_a"),
                        row.get("entry_id_b"),
                        row.get("headword_norm_a"),
                        row.get("headword_norm_b"),
                        float(row.get("score", 0.0)),
                        str(row.get("method") or "exact"),
                        row.get("reason"),
                        row.get("status_a"),
                        row.get("status_b"),
                        utc_now_iso(),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_compare_diff_rows(
        self,
        run_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        with connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO compare_diff_rows(
                  run_id, headword_key, entry_id_a, entry_id_b,
                  pron_norm_a, pron_norm_b, pron_render_a, pron_render_b,
                  features_a_json, features_b_json, delta_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        str(row["headword_key"]),
                        row.get("entry_id_a"),
                        row.get("entry_id_b"),
                        row.get("pron_norm_a"),
                        row.get("pron_norm_b"),
                        row.get("pron_render_a"),
                        row.get("pron_render_b"),
                        json.dumps(row.get("features_a", {}), ensure_ascii=False, sort_keys=True),
                        json.dumps(row.get("features_b", {}), ensure_ascii=False, sort_keys=True),
                        json.dumps(row.get("delta", {}), ensure_ascii=False, sort_keys=True),
                        utc_now_iso(),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def list_compare_runs(self, limit: int = 30) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM compare_runs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return rows

    def compare_run_by_id(self, run_id: str) -> sqlite3.Row | None:
        with connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM compare_runs WHERE run_id=?", (run_id,)).fetchone()
        return row

    @staticmethod
    def _alpha_bucket_sql_expr(column: str) -> str:
        trimmed = f"LTRIM(COALESCE({column}, ''))"
        first_char = f"UPPER(SUBSTR({trimmed}, 1, 1))"
        return f"CASE WHEN {first_char} BETWEEN 'A' AND 'Z' THEN {first_char} ELSE '#' END"

    def compare_coverage_items(
        self, run_id: str, alpha_bucket: str | None = None
    ) -> list[sqlite3.Row]:
        query = """
            SELECT run_id, headword_key, corpus_id, present
            FROM compare_coverage_items
            WHERE run_id=?
        """
        params: list[Any] = [run_id]
        cleaned_bucket = str(alpha_bucket or "").strip().upper()
        if cleaned_bucket:
            query += f" AND {self._alpha_bucket_sql_expr('headword_key')}=?"
            params.append(cleaned_bucket)
        query += " ORDER BY headword_key, corpus_id"
        with connect(self.db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return rows

    def compare_alignment_pairs(
        self,
        run_id: str,
        include_unmatched: bool = True,
        alpha_bucket: str | None = None,
    ) -> list[sqlite3.Row]:
        cleaned_bucket = str(alpha_bucket or "").strip().upper()
        alpha_filter = ""
        params: list[Any] = [run_id]
        if cleaned_bucket:
            alpha_filter = f" AND {self._alpha_bucket_sql_expr('headword_key')}=?"
            params.append(cleaned_bucket)
        with connect(self.db_path) as conn:
            if include_unmatched:
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM compare_alignment_pairs
                    WHERE run_id=?{alpha_filter}
                    ORDER BY method='exact' DESC, score DESC, headword_key
                    """,
                    tuple(params),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM compare_alignment_pairs
                    WHERE run_id=?{alpha_filter} AND entry_id_a IS NOT NULL AND entry_id_b IS NOT NULL
                    ORDER BY method='exact' DESC, score DESC, headword_key
                    """,
                    tuple(params),
                ).fetchall()
        return rows

    def compare_diff_rows(self, run_id: str, alpha_bucket: str | None = None) -> list[sqlite3.Row]:
        query = """
            SELECT *
            FROM compare_diff_rows
            WHERE run_id=?
        """
        params: list[Any] = [run_id]
        cleaned_bucket = str(alpha_bucket or "").strip().upper()
        if cleaned_bucket:
            query += f" AND {self._alpha_bucket_sql_expr('headword_key')}=?"
            params.append(cleaned_bucket)
        query += " ORDER BY headword_key"
        with connect(self.db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return rows

    def compare_coverage_letter_counts(self, run_id: str) -> dict[str, int]:
        bucket_expr = self._alpha_bucket_sql_expr("headword_key")
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT {bucket_expr} AS alpha_bucket, COUNT(DISTINCT headword_key) AS n
                FROM compare_coverage_items
                WHERE run_id=?
                GROUP BY alpha_bucket
                """,
                (run_id,),
            ).fetchall()
        return {str(row["alpha_bucket"] or "#"): int(row["n"] or 0) for row in rows}

    def compare_alignment_letter_counts(self, run_id: str) -> dict[str, int]:
        bucket_expr = self._alpha_bucket_sql_expr("headword_key")
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT {bucket_expr} AS alpha_bucket, COUNT(*) AS n
                FROM compare_alignment_pairs
                WHERE run_id=?
                GROUP BY alpha_bucket
                """,
                (run_id,),
            ).fetchall()
        return {str(row["alpha_bucket"] or "#"): int(row["n"] or 0) for row in rows}

    def compare_diff_letter_counts(self, run_id: str) -> dict[str, int]:
        bucket_expr = self._alpha_bucket_sql_expr("headword_key")
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT {bucket_expr} AS alpha_bucket, COUNT(*) AS n
                FROM compare_diff_rows
                WHERE run_id=?
                GROUP BY alpha_bucket
                """,
                (run_id,),
            ).fetchall()
        return {str(row["alpha_bucket"] or "#"): int(row["n"] or 0) for row in rows}

    def save_comparison_session(self, config: dict[str, Any]) -> str:
        payload = json.dumps(config, ensure_ascii=False, sort_keys=True)
        session_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        with connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO comparison_sessions(session_id, created_at, config_json) VALUES (?, ?, ?)",
                (session_id, utc_now_iso(), payload),
            )
            conn.commit()
        return session_id

    def upsert_lemma_group(self, lemma_label: str) -> str:
        lemma_group_id = lemma_group_id_for(lemma_label)
        with connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO lemma_groups(lemma_group_id, lemma_label, created_at) VALUES (?, ?, ?)",
                (lemma_group_id, lemma_label, utc_now_iso()),
            )
            conn.commit()
        return lemma_group_id

    def add_lemma_member(
        self,
        lemma_group_id: str,
        dict_id: str,
        entry_id: str,
        score: float,
        status: str,
        notes: str | None = None,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO lemma_members(
                  lemma_group_id, dict_id, entry_id, score, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (lemma_group_id, dict_id, entry_id, score, status, notes),
            )
            conn.commit()

    def top_issues(self, dict_id: str | None = None, limit: int = 20) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            if dict_id:
                rows = conn.execute(
                    """
                    SELECT code, kind, COUNT(*) AS n
                    FROM issues
                    WHERE dict_id=?
                    GROUP BY code, kind
                    ORDER BY n DESC
                    LIMIT ?
                    """,
                    (dict_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT code, kind, COUNT(*) AS n
                    FROM issues
                    GROUP BY code, kind
                    ORDER BY n DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return rows

    def comparison_rows(self, dict_ids: list[str]) -> list[dict[str, Any]]:
        if not dict_ids:
            return []
        placeholders = ",".join(["?"] * len(dict_ids))
        with connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT
                  lg.lemma_group_id,
                  lg.lemma_label,
                  lm.dict_id,
                  e.pron_render,
                  e.form_display,
                  e.form_norm,
                  lm.score
                FROM lemma_groups lg
                JOIN lemma_members lm ON lm.lemma_group_id = lg.lemma_group_id
                JOIN entries e ON e.entry_id = lm.entry_id
                WHERE lm.dict_id IN ({placeholders})
                ORDER BY lg.lemma_label, lm.dict_id
                """,
                tuple(dict_ids),
            ).fetchall()

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            group_id = row["lemma_group_id"]
            grouped.setdefault(
                group_id,
                {"lemma_group_id": group_id, "lemma_label": row["lemma_label"], "values": {}},
            )
            grouped[group_id]["values"][row["dict_id"]] = (
                row["pron_render"] or row["form_display"] or row["form_norm"] or ""
            )
        return list(grouped.values())


def ensure_active_dict(db_path: Path, suggested_name: str = "Corpus") -> str:
    store = SQLiteStore(db_path)
    return store.ensure_active_dict(suggested_name=suggested_name)


def append_jsonl_log(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
