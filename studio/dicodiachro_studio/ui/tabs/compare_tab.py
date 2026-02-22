from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSlider,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.compare.workflow import (
    CompareWorkflowError,
    apply_compare_run,
    list_compare_runs,
    load_compare_run_data,
    preview_alignment,
    preview_coverage,
    preview_diff,
)
from dicodiachro.core.exporters.compare_exports import (
    export_compare_alignment,
    export_compare_coverage,
    export_compare_diff,
)

from ...services.jobs import JobThread
from ...services.state import AppState
from ...services.theme import apply_theme_safe_styles


class CompareTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state

        self.preview_payload: dict[str, Any] = {}
        self.current_run_id: str | None = None
        self.current_job: JobThread | None = None

        self.workflow_label = QLabel(
            "Ce que fait cet atelier: compare la couverture, aligne A/B, puis calcule les différences phonologiques."
        )
        self.workflow_label.setWordWrap(True)
        self.next_step_label = QLabel("Étape suivante recommandée: Exporter les résultats.")
        self.next_step_label.setWordWrap(True)
        self.next_step_btn = QPushButton("Aller à l'étape suivante")
        self.next_step_btn.clicked.connect(self.open_export)

        self.corpus_search = QLineEdit()
        self.corpus_search.setPlaceholderText("Rechercher corpus...")
        self.corpus_search.textChanged.connect(self._apply_corpus_filter)

        self.corpus_list = QListWidget()

        self.key_field_combo = QComboBox()
        self.key_field_combo.addItem("headword_norm_effective", "headword_norm_effective")
        self.key_field_combo.addItem("form_norm_effective", "form_norm_effective")
        self.key_field_combo.addItem("headword_effective", "headword_effective")

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Exact", "exact")
        self.mode_combo.addItem("Exact + Fuzzy", "exact+fuzzy")

        self.threshold_slider = QSlider(Qt.Orientation.Horizontal)
        self.threshold_slider.setRange(70, 95)
        self.threshold_slider.setValue(90)
        self.threshold_slider.valueChanged.connect(self._update_threshold_label)
        self.threshold_label = QLabel("90")

        self.strategy_combo = QComboBox()
        self.strategy_combo.addItem("Greedy 1-to-1", "greedy")

        self.preview_btn = QPushButton("Prévisualiser")
        self.preview_btn.clicked.connect(self.preview_run)

        self.apply_btn = QPushButton("Appliquer")
        self.apply_btn.clicked.connect(self.apply_run)

        self.counters_label = QLabel("Union: 0 | Communs: 0 | Uniques A: 0 | Uniques B: 0")

        self.preview_tabs = QTabWidget()

        self.coverage_filter = QComboBox()
        self.coverage_filter.addItem("Tous", "all")
        self.coverage_filter.addItem("Communs à tous", "common_all")
        self.coverage_filter.addItem("Uniquement dans A", "only_a")
        self.coverage_filter.addItem("A pas dans B", "a_not_b")
        self.coverage_filter.addItem("B pas dans A", "b_not_a")
        self.coverage_filter.currentIndexChanged.connect(lambda _: self._render_coverage_table())

        self.coverage_table = QTableWidget(0, 0)
        self.coverage_table.horizontalHeader().setStretchLastSection(True)

        coverage_widget = QWidget()
        coverage_layout = QVBoxLayout()
        coverage_row = QHBoxLayout()
        coverage_row.addWidget(QLabel("Filtre"))
        coverage_row.addWidget(self.coverage_filter)
        coverage_row.addStretch(1)
        coverage_layout.addLayout(coverage_row)
        coverage_layout.addWidget(self.coverage_table)
        coverage_widget.setLayout(coverage_layout)

        self.alignment_show_unmatched = QCheckBox("Afficher non alignés")
        self.alignment_show_unmatched.setChecked(True)
        self.alignment_show_unmatched.toggled.connect(lambda _: self._render_alignment_table())

        self.alignment_table = QTableWidget(0, 8)
        self.alignment_table.setHorizontalHeaderLabels(
            [
                "headword_A",
                "headword_B",
                "score",
                "method",
                "status_A",
                "status_B",
                "diff",
                "reason",
            ]
        )
        self.alignment_table.horizontalHeader().setStretchLastSection(True)

        alignment_widget = QWidget()
        alignment_layout = QVBoxLayout()
        alignment_controls = QHBoxLayout()
        alignment_controls.addWidget(self.alignment_show_unmatched)
        alignment_controls.addStretch(1)
        alignment_layout.addLayout(alignment_controls)
        alignment_layout.addWidget(self.alignment_table)
        alignment_widget.setLayout(alignment_layout)

        self.diff_filter = QComboBox()
        self.diff_filter.addItem("Tous", "all")
        self.diff_filter.addItem("pron_render différents", "pron_render_diff")
        self.diff_filter.addItem("syll_count diff != 0", "syll_count_diff")
        self.diff_filter.addItem("stress_schema diff", "stress_diff")
        self.diff_filter.currentIndexChanged.connect(lambda _: self._render_diff_table())

        self.diff_table = QTableWidget(0, 9)
        self.diff_table.setHorizontalHeaderLabels(
            [
                "headword_key",
                "pron_render_A",
                "pron_render_B",
                "pron_norm_A",
                "pron_norm_B",
                "features_A",
                "features_B",
                "delta",
                "changed",
            ]
        )
        self.diff_table.horizontalHeader().setStretchLastSection(True)

        diff_widget = QWidget()
        diff_layout = QVBoxLayout()
        diff_row = QHBoxLayout()
        diff_row.addWidget(QLabel("Filtre"))
        diff_row.addWidget(self.diff_filter)
        diff_row.addStretch(1)
        diff_layout.addLayout(diff_row)
        diff_layout.addWidget(self.diff_table)
        diff_widget.setLayout(diff_layout)

        self.preview_tabs.addTab(coverage_widget, "Couverture")
        self.preview_tabs.addTab(alignment_widget, "Alignement")
        self.preview_tabs.addTab(diff_widget, "Diff phonologique")

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        self.apply_summary = QTextEdit()
        self.apply_summary.setReadOnly(True)

        self.history_table = QTableWidget(0, 7)
        self.history_table.setHorizontalHeaderLabels(
            ["date", "run_id", "corpus", "mode", "seuil", "hash", "stats"]
        )
        self.history_table.horizontalHeader().setStretchLastSection(True)
        self.history_table.itemSelectionChanged.connect(self._on_history_selected)

        export_coverage_btn = QPushButton("Exporter couverture")
        export_coverage_btn.clicked.connect(self.export_coverage)
        export_alignment_btn = QPushButton("Exporter alignement")
        export_alignment_btn.clicked.connect(self.export_alignment)
        export_diff_btn = QPushButton("Exporter différences")
        export_diff_btn.clicked.connect(self.export_diff)

        refresh_history_btn = QPushButton("Rafraîchir historique")
        refresh_history_btn.clicked.connect(self.refresh_history)

        left = QWidget()
        left_layout = QVBoxLayout()
        left_layout.addWidget(QLabel("Corpus"))
        left_layout.addWidget(self.corpus_search)
        left_layout.addWidget(self.corpus_list)

        options_row_1 = QHBoxLayout()
        options_row_1.addWidget(QLabel("Clé"))
        options_row_1.addWidget(self.key_field_combo)

        options_row_2 = QHBoxLayout()
        options_row_2.addWidget(QLabel("Mode"))
        options_row_2.addWidget(self.mode_combo)

        options_row_3 = QHBoxLayout()
        options_row_3.addWidget(QLabel("Seuil fuzzy"))
        options_row_3.addWidget(self.threshold_slider, 1)
        options_row_3.addWidget(self.threshold_label)

        options_row_4 = QHBoxLayout()
        options_row_4.addWidget(QLabel("Stratégie"))
        options_row_4.addWidget(self.strategy_combo)

        buttons_row = QHBoxLayout()
        buttons_row.addWidget(self.preview_btn)
        buttons_row.addWidget(self.apply_btn)

        left_layout.addLayout(options_row_1)
        left_layout.addLayout(options_row_2)
        left_layout.addLayout(options_row_3)
        left_layout.addLayout(options_row_4)
        left_layout.addLayout(buttons_row)
        left_layout.addStretch(1)
        left.setLayout(left_layout)

        center = QWidget()
        center_layout = QVBoxLayout()
        center_layout.addWidget(self.counters_label)
        center_layout.addWidget(self.preview_tabs)
        center.setLayout(center_layout)

        right = QWidget()
        right_layout = QVBoxLayout()
        right_layout.addWidget(self.progress)
        right_layout.addWidget(QLabel("Résumé application"))
        right_layout.addWidget(self.apply_summary)
        right_layout.addWidget(QLabel("Historique des runs"))
        right_layout.addWidget(self.history_table)
        right_layout.addWidget(refresh_history_btn)
        right_layout.addWidget(export_coverage_btn)
        right_layout.addWidget(export_alignment_btn)
        right_layout.addWidget(export_diff_btn)
        right.setLayout(right_layout)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(center)
        splitter.addWidget(right)
        splitter.setSizes([340, 860, 420])

        root_layout = QVBoxLayout()
        banner = QHBoxLayout()
        banner.addWidget(self.workflow_label, 2)
        banner.addWidget(self.next_step_label, 2)
        banner.addWidget(self.next_step_btn)
        root_layout.addLayout(banner)
        root_layout.addWidget(splitter)
        self.setLayout(root_layout)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.state.dictionary_changed.connect(lambda _: self.refresh())

        apply_theme_safe_styles(self)
        self.refresh()

    def _update_threshold_label(self, value: int) -> None:
        self.threshold_label.setText(str(value))

    def _apply_corpus_filter(self) -> None:
        query = self.corpus_search.text().strip().lower()
        for idx in range(self.corpus_list.count()):
            item = self.corpus_list.item(idx)
            if not item:
                continue
            text = item.text().lower()
            item.setHidden(bool(query) and query not in text)

    def _checked_corpora(self) -> list[str]:
        corpora: list[str] = []
        for idx in range(self.corpus_list.count()):
            item = self.corpus_list.item(idx)
            if not item:
                continue
            if item.checkState() == Qt.CheckState.Checked:
                corpora.append(item.text())
        return corpora

    def _settings(self) -> dict[str, Any]:
        return {
            "key_field": str(self.key_field_combo.currentData() or "headword_norm_effective"),
            "mode": str(self.mode_combo.currentData() or "exact"),
            "fuzzy_threshold": int(self.threshold_slider.value()),
            "algorithm": str(self.strategy_combo.currentData() or "greedy"),
        }

    def refresh(self) -> None:
        checked = set(self._checked_corpora())
        self.corpus_list.clear()
        for row in self.state.list_dictionaries():
            corpus_id = str(row["dict_id"])
            item = QListWidgetItem(corpus_id)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            if corpus_id in checked:
                item.setCheckState(Qt.CheckState.Checked)
            else:
                item.setCheckState(Qt.CheckState.Unchecked)
            self.corpus_list.addItem(item)

        if self.state.active_dict_id:
            self.preselect_corpora([self.state.active_dict_id])

        self._apply_corpus_filter()
        self.refresh_history()
        self._update_next_step_state()

    def preselect_corpora(self, corpus_ids: list[str]) -> None:
        wanted = set(corpus_ids)
        for idx in range(self.corpus_list.count()):
            item = self.corpus_list.item(idx)
            if not item:
                continue
            if item.text() in wanted:
                item.setCheckState(Qt.CheckState.Checked)

    def configure_alignment_options(
        self,
        *,
        corpus_ids: list[str] | None = None,
        mode: str | None = None,
        fuzzy_threshold: int | None = None,
        key_field: str | None = None,
        algorithm: str | None = None,
        open_alignment_tab: bool = False,
    ) -> None:
        if corpus_ids:
            self.preselect_corpora(corpus_ids)

        if key_field:
            key_idx = self.key_field_combo.findData(key_field)
            if key_idx >= 0:
                self.key_field_combo.setCurrentIndex(key_idx)

        if mode:
            mode_idx = self.mode_combo.findData(mode)
            if mode_idx >= 0:
                self.mode_combo.setCurrentIndex(mode_idx)

        if fuzzy_threshold is not None:
            bounded = max(
                self.threshold_slider.minimum(),
                min(self.threshold_slider.maximum(), int(fuzzy_threshold)),
            )
            self.threshold_slider.setValue(bounded)

        if algorithm:
            algo_idx = self.strategy_combo.findData(algorithm)
            if algo_idx >= 0:
                self.strategy_combo.setCurrentIndex(algo_idx)

        if open_alignment_tab:
            self.preview_tabs.setCurrentIndex(1)

    def _validate_selection(self) -> tuple[list[str], str, str] | None:
        corpus_ids = self._checked_corpora()
        if len(corpus_ids) < 2:
            QMessageBox.warning(self, "Comparer", "Sélectionnez au moins 2 corpus.")
            return None
        corpus_a = corpus_ids[0]
        corpus_b = corpus_ids[1]
        return corpus_ids, corpus_a, corpus_b

    def preview_run(self) -> None:
        if not self.state.db_path:
            return

        selection = self._validate_selection()
        if selection is None:
            return
        corpus_ids, corpus_a, corpus_b = selection
        settings = self._settings()

        try:
            coverage = preview_coverage(
                db_path=self.state.db_path,
                corpus_ids=corpus_ids,
                limit=2000,
                filters={"mode": "all"},
                key_field=settings["key_field"],
            )
            alignment = preview_alignment(
                db_path=self.state.db_path,
                corpus_a=corpus_a,
                corpus_b=corpus_b,
                mode=settings["mode"],
                threshold=int(settings["fuzzy_threshold"]),
                limit=2000,
                key_field=settings["key_field"],
                include_unmatched=True,
            )
            diff = preview_diff(
                db_path=self.state.db_path,
                run_settings={
                    "alignment_rows": alignment["rows"],
                    "corpus_a": corpus_a,
                    "corpus_b": corpus_b,
                },
                limit=2000,
                filters={"mode": "all"},
            )
        except CompareWorkflowError as exc:
            QMessageBox.warning(self, "Comparer", str(exc))
            return

        self.preview_payload = {
            "coverage": coverage,
            "alignment": alignment,
            "diff": diff,
            "settings": settings,
            "corpus_ids": corpus_ids,
            "corpus_a": corpus_a,
            "corpus_b": corpus_b,
        }
        self.current_run_id = None
        self.apply_summary.setPlainText("Preview ready (non persisted).")
        self._render_all_tables()

    def apply_run(self) -> None:
        if not self.state.db_path:
            return

        selection = self._validate_selection()
        if selection is None:
            return
        corpus_ids, corpus_a, corpus_b = selection
        settings = self._settings()

        self.progress.show()
        self.current_job = JobThread(
            apply_compare_run,
            self.state.db_path,
            corpus_ids,
            corpus_a,
            corpus_b,
            settings,
        )
        self.current_job.signals.finished.connect(self._on_apply_finished)
        self.current_job.signals.failed.connect(self._on_apply_failed)
        self.current_job.start()

    def _on_apply_finished(self, result: dict[str, Any]) -> None:
        self.progress.hide()
        self.current_run_id = str(result.get("run_id") or "") or None
        self.apply_summary.setPlainText(json.dumps(result, ensure_ascii=False, indent=2))

        if self.current_run_id and self.state.db_path:
            try:
                loaded = load_compare_run_data(self.state.db_path, self.current_run_id)
                self.preview_payload = {
                    "coverage": loaded.get("coverage", {}),
                    "alignment": loaded.get("alignment", {}),
                    "diff": loaded.get("diff", {}),
                    "settings": {
                        "key_field": result.get("key_field", "headword_norm_effective"),
                        "mode": result.get("mode", "exact"),
                        "fuzzy_threshold": result.get("fuzzy_threshold", 90),
                        "algorithm": result.get("algorithm", "greedy"),
                    },
                    "corpus_ids": result.get("corpus_ids", []),
                    "corpus_a": result.get("corpus_a", ""),
                    "corpus_b": result.get("corpus_b", ""),
                }
                self._render_all_tables()
            except CompareWorkflowError:
                pass

        self.refresh_history()
        self.state.notify_data_changed()
        self._update_next_step_state()

    def _on_apply_failed(self, trace: str) -> None:
        self.progress.hide()
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        friendly = lines[-1] if lines else "Erreur de comparaison"
        QMessageBox.critical(self, "Comparer", friendly)

    def _coverage_rows_filtered(self) -> tuple[list[str], list[dict[str, Any]]]:
        coverage = self.preview_payload.get("coverage", {})
        if not isinstance(coverage, dict):
            return [], []

        corpus_ids = [str(corpus_id) for corpus_id in coverage.get("corpus_ids", [])]
        rows = coverage.get("rows", [])
        if not isinstance(rows, list):
            return corpus_ids, []

        mode = str(self.coverage_filter.currentData() or "all")
        if mode == "all":
            return corpus_ids, rows

        filtered: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            presence = row.get("presence", {})
            if not isinstance(presence, dict):
                continue

            if mode == "common_all":
                if all(bool(presence.get(corpus_id, False)) for corpus_id in corpus_ids):
                    filtered.append(row)
                continue

            if len(corpus_ids) < 2:
                filtered.append(row)
                continue

            corpus_a = corpus_ids[0]
            corpus_b = corpus_ids[1]
            in_a = bool(presence.get(corpus_a, False))
            in_b = bool(presence.get(corpus_b, False))

            if mode == "only_a":
                if in_a and not any(
                    bool(presence.get(corpus_id, False))
                    for corpus_id in corpus_ids
                    if corpus_id != corpus_a
                ):
                    filtered.append(row)
                continue

            if mode == "a_not_b":
                if in_a and not in_b:
                    filtered.append(row)
                continue

            if mode == "b_not_a":
                if in_b and not in_a:
                    filtered.append(row)
                continue

            filtered.append(row)

        return corpus_ids, filtered

    def _render_coverage_table(self) -> None:
        corpus_ids, rows = self._coverage_rows_filtered()
        self.coverage_table.setColumnCount(1 + len(corpus_ids))
        self.coverage_table.setHorizontalHeaderLabels(["headword_key", *corpus_ids])
        self.coverage_table.setRowCount(0)

        for row in rows:
            if not isinstance(row, dict):
                continue
            table_row = self.coverage_table.rowCount()
            self.coverage_table.insertRow(table_row)
            self.coverage_table.setItem(
                table_row,
                0,
                QTableWidgetItem(str(row.get("headword_key") or "")),
            )
            presence = row.get("presence", {})
            if not isinstance(presence, dict):
                presence = {}
            for col, corpus_id in enumerate(corpus_ids, start=1):
                marker = "✓" if bool(presence.get(corpus_id, False)) else "—"
                self.coverage_table.setItem(table_row, col, QTableWidgetItem(marker))

    def _render_alignment_table(self) -> None:
        alignment = self.preview_payload.get("alignment", {})
        rows = alignment.get("rows", []) if isinstance(alignment, dict) else []
        if not isinstance(rows, list):
            rows = []

        show_unmatched = self.alignment_show_unmatched.isChecked()
        filtered_rows = [
            row
            for row in rows
            if isinstance(row, dict)
            and (show_unmatched or (row.get("entry_id_a") and row.get("entry_id_b")))
        ]

        self.alignment_table.setRowCount(0)
        for row in filtered_rows:
            table_row = self.alignment_table.rowCount()
            self.alignment_table.insertRow(table_row)

            headword_a = str(row.get("headword_a") or "")
            headword_b = str(row.get("headword_b") or "")
            norm_a = str(row.get("headword_norm_a") or "")
            norm_b = str(row.get("headword_norm_b") or "")
            diff = "diff" if norm_a and norm_b and norm_a != norm_b else ""

            values = [
                headword_a,
                headword_b,
                str(row.get("score") or 0),
                str(row.get("method") or ""),
                str(row.get("status_a") or ""),
                str(row.get("status_b") or ""),
                diff,
                str(row.get("reason") or ""),
            ]
            for col, value in enumerate(values):
                self.alignment_table.setItem(table_row, col, QTableWidgetItem(value))

    def _render_diff_table(self) -> None:
        diff_payload = self.preview_payload.get("diff", {})
        rows = diff_payload.get("rows", []) if isinstance(diff_payload, dict) else []
        if not isinstance(rows, list):
            rows = []

        mode = str(self.diff_filter.currentData() or "all")
        filtered_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            delta = row.get("delta", {})
            if not isinstance(delta, dict):
                delta = {}

            if mode == "pron_render_diff":
                if row.get("pron_render_a") == row.get("pron_render_b"):
                    continue
            elif mode == "syll_count_diff":
                if int(delta.get("syll_count_diff", 0) or 0) == 0:
                    continue
            elif mode == "stress_diff":
                if not bool(delta.get("stress_schema_diff", False)):
                    continue

            filtered_rows.append(row)

        self.diff_table.setRowCount(0)
        for row in filtered_rows:
            table_row = self.diff_table.rowCount()
            self.diff_table.insertRow(table_row)

            features_a = json.dumps(row.get("features_a", {}), ensure_ascii=False, sort_keys=True)
            features_b = json.dumps(row.get("features_b", {}), ensure_ascii=False, sort_keys=True)
            delta_json = json.dumps(row.get("delta", {}), ensure_ascii=False, sort_keys=True)
            changed = "yes" if row.get("pron_render_a") != row.get("pron_render_b") else ""

            values = [
                str(row.get("headword_key") or ""),
                str(row.get("pron_render_a") or ""),
                str(row.get("pron_render_b") or ""),
                str(row.get("pron_norm_a") or ""),
                str(row.get("pron_norm_b") or ""),
                features_a,
                features_b,
                delta_json,
                changed,
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col in {5, 6, 7}:
                    item.setToolTip(value)
                self.diff_table.setItem(table_row, col, item)

    def _render_counters(self) -> None:
        coverage = self.preview_payload.get("coverage", {})
        counts = coverage.get("counts", {}) if isinstance(coverage, dict) else {}
        if not isinstance(counts, dict):
            counts = {}

        self.counters_label.setText(
            "Union: {union} | Communs: {common} | Uniques A: {ua} | Uniques B: {ub}".format(
                union=int(counts.get("union", 0) or 0),
                common=int(counts.get("common_all", 0) or 0),
                ua=int(counts.get("unique_a", 0) or 0),
                ub=int(counts.get("unique_b", 0) or 0),
            )
        )

    def _render_all_tables(self) -> None:
        self._render_counters()
        self._render_coverage_table()
        self._render_alignment_table()
        self._render_diff_table()

    def refresh_history(self) -> None:
        if not self.state.db_path:
            self.history_table.setRowCount(0)
            return

        rows = list_compare_runs(self.state.db_path, limit=30)
        self.history_table.setRowCount(0)
        for row in rows:
            stats = row.get("stats", {})
            alignment_counts = stats.get("alignment", {}) if isinstance(stats, dict) else {}
            stats_text = "exact={exact} fuzzy={fuzzy} unmatched={unmatched}".format(
                exact=int(alignment_counts.get("matched_exact", 0) or 0),
                fuzzy=int(alignment_counts.get("matched_fuzzy", 0) or 0),
                unmatched=int(alignment_counts.get("unmatched", 0) or 0),
            )

            table_row = self.history_table.rowCount()
            self.history_table.insertRow(table_row)

            values = [
                str(row.get("created_at") or ""),
                str(row.get("run_id") or "")[:12],
                ",".join([str(c) for c in row.get("corpus_ids", [])]),
                str(row.get("mode") or ""),
                str(row.get("fuzzy_threshold") or ""),
                str(row.get("settings_sha256") or "")[:12],
                stats_text,
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 1:
                    item.setData(Qt.ItemDataRole.UserRole, str(row.get("run_id") or ""))
                self.history_table.setItem(table_row, col, item)

    def _selected_run_id(self) -> str | None:
        selected = self.history_table.selectedItems()
        if not selected:
            return None
        row_idx = selected[0].row()
        item = self.history_table.item(row_idx, 1)
        if not item:
            return None
        run_id = item.data(Qt.ItemDataRole.UserRole)
        if not run_id:
            return None
        return str(run_id)

    def _on_history_selected(self) -> None:
        run_id = self._selected_run_id()
        if not run_id or not self.state.db_path:
            return

        try:
            payload = load_compare_run_data(self.state.db_path, run_id)
        except CompareWorkflowError:
            return

        run = payload.get("run", {})
        self.current_run_id = run_id
        self.preview_payload = {
            "coverage": payload.get("coverage", {}),
            "alignment": payload.get("alignment", {}),
            "diff": payload.get("diff", {}),
            "settings": {
                "key_field": run.get("key_field", "headword_norm_effective"),
                "mode": run.get("mode", "exact"),
                "fuzzy_threshold": run.get("fuzzy_threshold", 90),
                "algorithm": run.get("algorithm", "greedy"),
            },
            "corpus_ids": run.get("corpus_ids", []),
            "corpus_a": "",
            "corpus_b": "",
        }
        self.apply_summary.setPlainText(json.dumps(run, ensure_ascii=False, indent=2))
        self._render_all_tables()
        self._update_next_step_state()

    def _select_export_path(self, title: str, default_name: str) -> Path | None:
        if not self.state.project_dir:
            return None
        out, _ = QFileDialog.getSaveFileName(
            self,
            title,
            default_name,
            "CSV (*.csv);;XLSX (*.xlsx)",
        )
        if not out:
            return None
        return Path(out)

    def _require_run_id(self) -> str | None:
        if self.current_run_id:
            return self.current_run_id

        run_id = self._selected_run_id()
        if run_id:
            self.current_run_id = run_id
            return run_id

        QMessageBox.warning(self, "Comparer", "Aucun run appliqué sélectionné.")
        return None

    def export_coverage(self) -> None:
        if not self.state.store:
            return
        run_id = self._require_run_id()
        if not run_id:
            return

        target = self._select_export_path("Exporter couverture", "coverage.csv")
        if target is None:
            return

        path = export_compare_coverage(self.state.store, run_id, target)
        QMessageBox.information(self, "Comparer", f"Export coverage: {path}")

    def export_alignment(self) -> None:
        if not self.state.store:
            return
        run_id = self._require_run_id()
        if not run_id:
            return

        target = self._select_export_path("Exporter alignement", "alignment.csv")
        if target is None:
            return

        path = export_compare_alignment(self.state.store, run_id, target)
        QMessageBox.information(self, "Comparer", f"Export alignment: {path}")

    def export_diff(self) -> None:
        if not self.state.store:
            return
        run_id = self._require_run_id()
        if not run_id:
            return

        target = self._select_export_path("Exporter différences", "diff.csv")
        if target is None:
            return

        path = export_compare_diff(self.state.store, run_id, target)
        QMessageBox.information(self, "Comparer", f"Export diff: {path}")

    def _update_next_step_state(self) -> None:
        ready = bool(self.current_run_id or self._selected_run_id())
        if ready:
            self.next_step_label.setText("Étape suivante recommandée: Exporter les résultats.")
            self.next_step_btn.setEnabled(True)
            return
        self.next_step_label.setText("Étape suivante recommandée: Appliquer la comparaison.")
        self.next_step_btn.setEnabled(False)

    def open_export(self) -> None:
        self.state.request_export()
