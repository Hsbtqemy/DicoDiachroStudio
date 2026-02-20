from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from ..models import Issue, ParsedEntry, ProfileApplied, ProfileSpec, ProjectPaths, utc_now_iso

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
        for template in template_dir.glob("*.yml"):
            target = paths.rules_dir / template.name
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

        CREATE TABLE IF NOT EXISTS entries (
            entry_id TEXT PRIMARY KEY,
            dict_id TEXT NOT NULL,
            section TEXT,
            syllables INTEGER NOT NULL,
            headword_raw TEXT NOT NULL,
            pos_raw TEXT NOT NULL,
            pron_raw TEXT,
            form_display TEXT,
            form_norm TEXT,
            features_json TEXT,
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

        CREATE INDEX IF NOT EXISTS idx_entries_dict ON entries(dict_id);
        CREATE INDEX IF NOT EXISTS idx_entries_norm ON entries(form_norm);
        CREATE INDEX IF NOT EXISTS idx_issues_dict ON issues(dict_id);
        CREATE INDEX IF NOT EXISTS idx_issues_code ON issues(code);
        CREATE INDEX IF NOT EXISTS idx_members_dict ON lemma_members(dict_id);
        """
    )
    conn.commit()


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
                    profile.version,
                    str(path),
                    sha256,
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def clear_dict_entries(self, dict_id: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute("DELETE FROM entries WHERE dict_id=?", (dict_id,))
            conn.execute("DELETE FROM issues WHERE dict_id=?", (dict_id,))
            conn.execute("DELETE FROM lemma_members WHERE dict_id=?", (dict_id,))
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
                rows.append(
                    (
                        eid,
                        entry.dict_id,
                        entry.section,
                        entry.syllables,
                        entry.headword_raw,
                        entry.pos_raw,
                        entry.pron_raw,
                        applied.form_display if applied else None,
                        applied.form_norm if applied else None,
                        (
                            json.dumps(applied.features, ensure_ascii=False, sort_keys=True)
                            if applied
                            else "{}"
                        ),
                        entry.source_path,
                        entry.line_no,
                        utc_now_iso(),
                    )
                )
            conn.executemany(
                """
                INSERT OR REPLACE INTO entries(
                  entry_id, dict_id, section, syllables, headword_raw, pos_raw, pron_raw,
                  form_display, form_norm, features_json, source_path, line_no, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

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
                    json.dumps(applied.features, ensure_ascii=False, sort_keys=True),
                    entry_id,
                    dict_id,
                )
                for entry_id, applied in applied_by_entry_id.items()
            ]
            conn.executemany(
                """
                UPDATE entries
                SET form_display=?, form_norm=?, features_json=?
                WHERE entry_id=? AND dict_id=?
                """,
                rows,
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
    ) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            if search:
                query = (
                    "SELECT * FROM entries WHERE dict_id=? AND "
                    "(headword_raw LIKE ? OR form_display LIKE ? OR form_norm LIKE ?) "
                    "ORDER BY section, line_no LIMIT ? OFFSET ?"
                )
                needle = f"%{search}%"
                rows = conn.execute(
                    query, (dict_id, needle, needle, needle, limit, offset)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM entries WHERE dict_id=? ORDER BY section, line_no LIMIT ? OFFSET ?",
                    (dict_id, limit, offset),
                ).fetchall()
        return rows

    def count_entries(self, dict_id: str) -> int:
        with connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM entries WHERE dict_id=?", (dict_id,)
            ).fetchone()
        return int(row["n"]) if row else 0

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

    def entries_for_dict(self, dict_id: str) -> list[sqlite3.Row]:
        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM entries WHERE dict_id=? ORDER BY line_no", (dict_id,)
            ).fetchall()
        return rows

    def entry_by_id(self, entry_id: str) -> sqlite3.Row | None:
        with connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM entries WHERE entry_id=?", (entry_id,)).fetchone()
        return row

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
                SELECT lg.lemma_group_id, lg.lemma_label, lm.dict_id, e.form_display, e.form_norm, lm.score
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
                row["form_display"] or row["form_norm"] or ""
            )
        return list(grouped.values())


def append_jsonl_log(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
