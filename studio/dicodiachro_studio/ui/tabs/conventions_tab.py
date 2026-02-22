from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.conventions.workflow import apply_convention, preview_convention
from dicodiachro.core.pipeline import PipelineError
from dicodiachro.core.profiles import (
    ProfileValidationError,
    build_profile_from_ui,
    load_profile,
    profile_sha256_from_path,
    profile_to_yaml_text,
)
from dicodiachro.core.storage.sqlite import project_paths
from dicodiachro.core.utils.slug import slugify

from ...services.jobs import JobThread
from ...services.state import AppState
from ...services.theme import apply_theme_safe_styles
from ..dialogs.save_convention_dialog import SaveConventionDialog


class ConventionsTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.current_job: JobThread | None = None
        self.preview_job: JobThread | None = None

        self.preview_payload: dict[str, Any] = {}
        self._syncing_settings = False
        self._preview_generation = 0
        self._preview_sample_ids: list[str] = []
        self._sample_corpus_id: str | None = None
        self._preview_dirty = False
        self._last_preview_time = "-"

        self._preview_debounce = QTimer(self)
        self._preview_debounce.setSingleShot(True)
        self._preview_debounce.setInterval(350)
        self._preview_debounce.timeout.connect(self._trigger_preview_auto)

        self.workflow_label = QLabel(
            "Ce que fait cet atelier: applique des conventions non destructives pour produire NORM/RENDER/FEATURES."
        )
        self.workflow_label.setWordWrap(True)
        self.next_step_label = QLabel("Étape suivante recommandée: Atelier Comparer.")
        self.next_step_label.setWordWrap(True)
        self.next_step_btn = QPushButton("Aller à l'étape suivante")
        self.next_step_btn.clicked.connect(self.open_compare)

        self.status_label = QLabel("Conventions actives pour ce corpus : -")

        self.profile_list = QListWidget()
        self.profile_list.currentItemChanged.connect(self._on_profile_changed)

        self.profile_meta_label = QLabel("Sélectionnez une convention.")
        self.profile_meta_label.setWordWrap(True)

        self.preview_limit = QSpinBox()
        self.preview_limit.setRange(1, 5000)
        self.preview_limit.setValue(200)

        self.diff_only_toggle = QCheckBox("Afficher: changements uniquement")
        self.diff_only_toggle.setChecked(True)
        self.diff_only_toggle.toggled.connect(self._render_preview_rows)

        self.sample_label = QLabel("Entrées prévisualisées: 0 (échantillon figé)")
        self.modified_label = QLabel("Modifiées: 0")
        self.alerts_label = QLabel("Alertes: 0")
        self.preview_time_label = QLabel("Dernière preview: -")
        self.pending_badge = QLabel("")

        self.preview_table = QTableWidget(0, 11)
        self.preview_table.setHorizontalHeaderLabels(
            [
                "headword_raw",
                "headword_effective",
                "headword_norm",
                "pron_raw",
                "pron_effective",
                "pron_norm",
                "pron_render",
                "features",
                "alertes",
                "changed",
                "override",
            ]
        )
        self.preview_table.horizontalHeader().setStretchLastSection(True)

        self.apply_summary = QTextEdit()
        self.apply_summary.setReadOnly(True)

        self.history_table = QTableWidget(0, 7)
        self.history_table.setHorizontalHeaderLabels(
            ["date", "convention_id", "version", "sha256", "entries", "alertes", "status"]
        )
        self.history_table.horizontalHeader().setStretchLastSection(True)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        preview_btn = QPushButton("Prévisualiser")
        preview_btn.clicked.connect(self.preview_selected)
        resample_btn = QPushButton("Nouveau sample")
        resample_btn.clicked.connect(self.resample_preview)
        reset_btn = QPushButton("Réinitialiser")
        reset_btn.clicked.connect(self.reset_settings)

        apply_btn = QPushButton("Appliquer au corpus")
        apply_btn.clicked.connect(self.apply_selected)
        compare_btn = QPushButton("Comparer…")
        compare_btn.clicked.connect(self.open_compare)

        duplicate_btn = QPushButton("Dupliquer")
        duplicate_btn.clicked.connect(self.duplicate_profile)

        import_btn = QPushButton("Importer YAML")
        import_btn.clicked.connect(self.import_yaml)

        export_btn = QPushButton("Exporter YAML")
        export_btn.clicked.connect(self.export_yaml)

        save_new_btn = QPushButton("Enregistrer comme nouvelle convention…")
        save_new_btn.clicked.connect(self.save_as_new_convention)

        quick_group = QWidget()
        quick_layout = QHBoxLayout(quick_group)
        quick_layout.addWidget(QLabel("Conventions courantes"))
        diplomatic_btn = QPushButton("Diplomatique")
        align_btn = QPushButton("Aligner (ſ→s…)")
        publication_btn = QPushButton("Publication (parenthèses)")
        diplomatic_btn.clicked.connect(lambda: self.preview_named_convention("reading_v1"))
        align_btn.clicked.connect(lambda: self.preview_named_convention("alignment_v1"))
        publication_btn.clicked.connect(
            lambda: self.preview_named_convention("analysis_quantity_v1")
        )
        quick_layout.addWidget(diplomatic_btn)
        quick_layout.addWidget(align_btn)
        quick_layout.addWidget(publication_btn)
        quick_layout.addStretch(1)

        self.settings_group = self._build_settings_group()

        left = QWidget()
        left_layout = QVBoxLayout(left)
        actions = QHBoxLayout()
        actions.addWidget(duplicate_btn)
        actions.addWidget(import_btn)
        actions.addWidget(export_btn)
        left_layout.addWidget(self.status_label)
        left_layout.addWidget(self.profile_list)
        left_layout.addLayout(actions)
        left_layout.addWidget(save_new_btn)
        left_layout.addWidget(QLabel("Résumé"))
        left_layout.addWidget(self.profile_meta_label)
        left_layout.addWidget(self.settings_group)

        center = QWidget()
        center_layout = QVBoxLayout(center)
        center_layout.addWidget(quick_group)
        preview_controls = QHBoxLayout()
        preview_controls.addWidget(preview_btn)
        preview_controls.addWidget(resample_btn)
        preview_controls.addWidget(reset_btn)
        preview_controls.addWidget(QLabel("N"))
        preview_controls.addWidget(self.preview_limit)
        preview_controls.addWidget(self.diff_only_toggle)
        preview_controls.addStretch(1)
        center_layout.addLayout(preview_controls)

        stats_row = QHBoxLayout()
        stats_row.addWidget(self.sample_label)
        stats_row.addWidget(self.modified_label)
        stats_row.addWidget(self.alerts_label)
        stats_row.addStretch(1)
        center_layout.addLayout(stats_row)

        info_row = QHBoxLayout()
        info_row.addWidget(self.preview_time_label)
        info_row.addStretch(1)
        info_row.addWidget(self.pending_badge)
        center_layout.addLayout(info_row)

        center_layout.addWidget(self.preview_table)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.addWidget(apply_btn)
        right_layout.addWidget(compare_btn)
        right_layout.addWidget(self.progress)
        right_layout.addWidget(QLabel("Résumé application"))
        right_layout.addWidget(self.apply_summary)
        right_layout.addWidget(QLabel("Historique"))
        right_layout.addWidget(self.history_table)

        self.split = QSplitter(Qt.Orientation.Horizontal)
        self.split.addWidget(left)
        self.split.addWidget(center)
        self.split.addWidget(right)
        self.split.setSizes([420, 860, 460])

        layout = QVBoxLayout()
        banner = QHBoxLayout()
        banner.addWidget(self.workflow_label, 2)
        banner.addWidget(self.next_step_label, 2)
        banner.addWidget(self.next_step_btn)
        layout.addLayout(banner)
        layout.addWidget(self.split)
        self.setLayout(layout)

        self.state.project_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh_history)
        self.state.dictionary_changed.connect(lambda _: self._on_corpus_changed())

        apply_theme_safe_styles(self)
        self.refresh()

    def _build_settings_group(self) -> QGroupBox:
        group = QGroupBox("Réglages")

        self.long_s_to_s_check = QCheckBox("Remplacer ſ → s")
        self.lowercase_check = QCheckBox("Minuscules")
        self.strip_diacritics_check = QCheckBox("Retirer diacritiques")
        self.normalize_spaces_check = QCheckBox("Normaliser espaces/ponctuation")
        self.remove_punctuation_check = QCheckBox("Retirer ponctuation")

        self.stress_prime_radio = QRadioButton("primes ʹ")
        self.stress_acute_radio = QRadioButton("voyelles accentuées")
        self.stress_both_radio = QRadioButton("les deux")
        self.stress_group = QButtonGroup(self)
        self.stress_group.addButton(self.stress_prime_radio)
        self.stress_group.addButton(self.stress_acute_radio)
        self.stress_group.addButton(self.stress_both_radio)
        self.stress_both_radio.setChecked(True)

        self.require_pron_check = QCheckBox("Exiger prononciation")
        self.enforce_stress_check = QCheckBox("Enforcer cohérence stress")

        self.render_mode = QComboBox()
        self.render_mode.addItem("Aucun", "none")
        self.render_mode.addItem("Parenthèses sur voyelle accentuée", "accent")
        self.render_mode.addItem("Parenthèses sur marque prime", "prime")

        for checkbox in [
            self.long_s_to_s_check,
            self.lowercase_check,
            self.strip_diacritics_check,
            self.normalize_spaces_check,
            self.remove_punctuation_check,
            self.require_pron_check,
            self.enforce_stress_check,
        ]:
            checkbox.toggled.connect(self._on_settings_changed)

        for radio in [self.stress_prime_radio, self.stress_acute_radio, self.stress_both_radio]:
            radio.toggled.connect(self._on_settings_changed)

        self.render_mode.currentIndexChanged.connect(self._on_settings_changed)

        layout = QVBoxLayout(group)
        layout.addWidget(QLabel("A) Normalisation"))
        layout.addWidget(self.long_s_to_s_check)
        layout.addWidget(self.lowercase_check)
        layout.addWidget(self.strip_diacritics_check)
        layout.addWidget(self.normalize_spaces_check)
        layout.addWidget(self.remove_punctuation_check)

        layout.addWidget(QLabel("B) Marques de stress"))
        layout.addWidget(self.stress_prime_radio)
        layout.addWidget(self.stress_acute_radio)
        layout.addWidget(self.stress_both_radio)

        layout.addWidget(QLabel("C) QA"))
        layout.addWidget(self.require_pron_check)
        layout.addWidget(self.enforce_stress_check)

        layout.addWidget(QLabel("D) Rendu"))
        layout.addWidget(self.render_mode)

        return group

    def _profile_files(self) -> list[Path]:
        if not self.state.project_dir:
            return []

        rules_dir = project_paths(self.state.project_dir).rules_dir
        paths = set(rules_dir.glob("*.yml")) | set(rules_dir.glob("*.yaml"))
        if self.state.active_dict_id:
            scoped = rules_dir / self.state.active_dict_id
            if scoped.exists():
                paths |= set(scoped.rglob("*.yml"))
                paths |= set(scoped.rglob("*.yaml"))
        return sorted(path.resolve() for path in paths)

    def _selected_profile_path(self) -> Path | None:
        item = self.profile_list.currentItem()
        if not item:
            return None
        value = item.data(Qt.ItemDataRole.UserRole)
        if not value:
            return None
        return Path(str(value))

    def _stress_mode_from_profile(self, profile) -> str:
        qa = profile.qa if isinstance(profile.qa, dict) else {}
        prime = bool(qa.get("require_prime_for_primary_stress", False))
        acute = bool(qa.get("require_acute_for_primary_stress", False))
        if prime and not acute:
            return "prime"
        if acute and not prime:
            return "acute"
        return "both"

    def _render_mode_from_profile(self, profile) -> str:
        render = profile.render if isinstance(profile.render, dict) else {}
        if not bool(render.get("enabled", True)):
            return "none"
        accent = bool(render.get("parenthesize_accented_vowel", False))
        prime = bool(render.get("parenthesize_prime_segment", False))
        if accent and not prime:
            return "accent"
        if prime and not accent:
            return "prime"
        return "prime"

    def _settings_payload_from_profile(self, profile) -> dict[str, object]:
        norm = profile.norm if isinstance(profile.norm, dict) else {}
        qa = profile.qa if isinstance(profile.qa, dict) else {}

        return {
            "long_s_to_s": bool(norm.get("long_s_to_s", False)),
            "lowercase": bool(norm.get("lowercase", True)),
            "strip_diacritics": bool(norm.get("strip_diacritics", False)),
            "normalize_spaces": bool(norm.get("collapse_spaces", True)),
            "remove_punctuation": bool(norm.get("remove_punctuation", False)),
            "stress_mode": self._stress_mode_from_profile(profile),
            "require_pronunciation": bool(qa.get("require_pronunciation", False)),
            "enforce_stress_consistency": bool(qa.get("enforce_stress_consistency", False)),
            "render_mode": self._render_mode_from_profile(profile),
        }

    def _apply_profile_to_settings(self, profile) -> None:
        baseline = self._settings_payload_from_profile(profile)

        self._syncing_settings = True
        self.long_s_to_s_check.setChecked(bool(baseline["long_s_to_s"]))
        self.lowercase_check.setChecked(bool(baseline["lowercase"]))
        self.strip_diacritics_check.setChecked(bool(baseline["strip_diacritics"]))
        self.normalize_spaces_check.setChecked(bool(baseline["normalize_spaces"]))
        self.remove_punctuation_check.setChecked(bool(baseline["remove_punctuation"]))

        stress_mode = str(baseline["stress_mode"])
        if stress_mode == "prime":
            self.stress_prime_radio.setChecked(True)
        elif stress_mode == "acute":
            self.stress_acute_radio.setChecked(True)
        else:
            self.stress_both_radio.setChecked(True)

        self.require_pron_check.setChecked(bool(baseline["require_pronunciation"]))
        self.enforce_stress_check.setChecked(bool(baseline["enforce_stress_consistency"]))

        idx = self.render_mode.findData(str(baseline["render_mode"]))
        self.render_mode.setCurrentIndex(idx if idx >= 0 else 0)
        self._syncing_settings = False

    def _settings_payload(self) -> dict[str, object]:
        if self.stress_prime_radio.isChecked():
            stress_mode = "prime"
        elif self.stress_acute_radio.isChecked():
            stress_mode = "acute"
        else:
            stress_mode = "both"

        return {
            "long_s_to_s": self.long_s_to_s_check.isChecked(),
            "lowercase": self.lowercase_check.isChecked(),
            "strip_diacritics": self.strip_diacritics_check.isChecked(),
            "normalize_spaces": self.normalize_spaces_check.isChecked(),
            "remove_punctuation": self.remove_punctuation_check.isChecked(),
            "stress_mode": stress_mode,
            "require_pronunciation": self.require_pron_check.isChecked(),
            "enforce_stress_consistency": self.enforce_stress_check.isChecked(),
            "render_mode": str(self.render_mode.currentData() or "none"),
        }

    def _on_settings_changed(self) -> None:
        if self._syncing_settings:
            return
        self._queue_auto_preview()

    def _queue_auto_preview(self) -> None:
        if (
            not self.state.project_dir
            or not self._current_corpus()
            or not self._selected_profile_path()
        ):
            return
        self._preview_debounce.start()

    def _trigger_preview_auto(self) -> None:
        self._start_preview(resample=False, manual=False)

    def _effective_profile_path(
        self,
        base_profile_path: Path,
        overrides: dict[str, object] | None = None,
    ) -> Path:
        if not self.state.project_dir:
            raise PipelineError("Projet non ouvert")

        base_profile = load_profile(base_profile_path)
        settings = self._settings_payload()
        if overrides:
            settings.update(overrides)
        effective_profile = build_profile_from_ui(base_profile, settings)
        yaml_text = profile_to_yaml_text(effective_profile)

        generated_dir = project_paths(self.state.project_dir).rules_dir / ".generated"
        generated_dir.mkdir(parents=True, exist_ok=True)
        target = generated_dir / f"{base_profile_path.stem}__ui.yml"
        target.write_text(yaml_text, encoding="utf-8")
        return target

    def _on_profile_changed(self) -> None:
        profile_path = self._selected_profile_path()
        if not profile_path:
            self.profile_meta_label.setText("Sélectionnez une convention.")
            return

        try:
            profile = load_profile(profile_path)
            self._apply_profile_to_settings(profile)
            warning_text = (
                ", ".join(profile.validation_warnings) if profile.validation_warnings else "aucun"
            )
            self.profile_meta_label.setText(
                "\n".join(
                    [
                        f"ID: {profile.profile_id}",
                        f"Version: {profile.version}",
                        f"Hash: {profile_sha256_from_path(profile_path)[:12]}",
                        f"Chemin: {profile_path.name}",
                        f"Avertissements: {warning_text}",
                    ]
                )
            )
        except ProfileValidationError as exc:
            self.profile_meta_label.setText(
                "\n".join(
                    [
                        f"Convention invalide: {profile_path.name}",
                        f"Erreurs: {'; '.join(exc.errors)}",
                    ]
                )
            )
            return

    def _current_corpus(self) -> str | None:
        return self.state.active_dict_id

    def _on_corpus_changed(self) -> None:
        self._preview_sample_ids = []
        self._sample_corpus_id = None
        self.refresh()

    def refresh(self) -> None:
        previous = self._selected_profile_path()

        self.profile_list.clear()
        rules_dir = (
            project_paths(self.state.project_dir).rules_dir if self.state.project_dir else None
        )
        for path in self._profile_files():
            display = path.stem
            if rules_dir:
                try:
                    display = str(path.relative_to(rules_dir))
                except ValueError:
                    display = path.name
            item = QListWidgetItem(display)
            item.setData(Qt.ItemDataRole.UserRole, str(path))
            self.profile_list.addItem(item)

        target_row = 0 if self.profile_list.count() > 0 else -1
        if previous:
            previous_resolved = previous.resolve()
            for idx in range(self.profile_list.count()):
                item = self.profile_list.item(idx)
                if not item:
                    continue
                path = Path(str(item.data(Qt.ItemDataRole.UserRole) or "")).resolve()
                if path == previous_resolved:
                    target_row = idx
                    break

        if target_row >= 0:
            self.profile_list.setCurrentRow(target_row)

        self.refresh_history()
        self._update_next_step_state()

    def _ensure_preview_sample_ids(self, *, resample: bool) -> list[str]:
        if not self.state.store or not self._current_corpus():
            return []

        corpus_id = self._current_corpus() or ""
        if resample or not self._preview_sample_ids or self._sample_corpus_id != corpus_id:
            rows = self.state.store.list_entries(
                dict_id=corpus_id,
                limit=self.preview_limit.value(),
                offset=0,
            )
            self._preview_sample_ids = [str(row["entry_id"]) for row in rows]
            self._sample_corpus_id = corpus_id
        return list(self._preview_sample_ids)

    def _run_preview_job(
        self,
        generation_id: int,
        project_dir: Path,
        corpus_id: str,
        profile_ref: str,
        sample_ids: list[str],
    ) -> dict[str, Any]:
        payload = preview_convention(
            project_dir=project_dir,
            corpus_id=corpus_id,
            profile_ref=profile_ref,
            limit=max(len(sample_ids), 1),
            entry_ids=sample_ids,
        )
        return {"generation_id": generation_id, "payload": payload}

    def _start_preview(self, *, resample: bool, manual: bool) -> None:
        if not self.state.project_dir or not self._current_corpus():
            if manual:
                QMessageBox.warning(self, "Conventions", "Sélectionnez d'abord un corpus actif.")
            return

        profile_path = self._selected_profile_path()
        if not profile_path:
            if manual:
                QMessageBox.warning(self, "Conventions", "Sélectionnez une convention.")
            return

        sample_ids = self._ensure_preview_sample_ids(resample=resample)
        if not sample_ids:
            if manual:
                QMessageBox.information(self, "Conventions", "Aucune entrée à prévisualiser.")
            self.preview_payload = {}
            self._render_preview_rows()
            return

        try:
            effective_profile_path = self._effective_profile_path(profile_path)
        except (PipelineError, ProfileValidationError) as exc:
            if manual:
                QMessageBox.warning(self, "Conventions", str(exc))
            return

        self._preview_generation += 1
        generation_id = self._preview_generation

        if self.preview_job and self.preview_job.isRunning():
            self.preview_job.cancel()

        self.progress.show()
        self.preview_job = JobThread(
            self._run_preview_job,
            generation_id,
            self.state.project_dir,
            self._current_corpus() or "",
            str(effective_profile_path),
            sample_ids,
        )
        self.preview_job.signals.finished.connect(self._on_preview_finished)
        self.preview_job.signals.failed.connect(self._on_preview_failed)
        self.preview_job.start()

    def preview_selected(self) -> None:
        self._start_preview(resample=False, manual=True)

    def resample_preview(self) -> None:
        self._start_preview(resample=True, manual=True)

    def _on_preview_finished(self, result: object) -> None:
        if not isinstance(result, dict):
            return
        generation_id = int(result.get("generation_id", -1))
        if generation_id != self._preview_generation:
            return

        payload = result.get("payload")
        if not isinstance(payload, dict):
            return

        self.progress.hide()
        self.preview_payload = payload
        self._last_preview_time = datetime.now().strftime("%H:%M:%S")
        self._preview_dirty = True

        self._update_preview_stats()
        self._render_preview_rows()

    def _on_preview_failed(self, trace: str) -> None:
        self.progress.hide()
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        friendly = lines[-1] if lines else "Erreur de prévisualisation"
        QMessageBox.warning(self, "Conventions", friendly)

    def _row_changed(self, row: dict[str, Any]) -> bool:
        alerts = row.get("alerts", [])
        has_alerts = bool(alerts) and len(list(alerts)) > 0
        return bool(
            str(row.get("headword_effective") or "") != str(row.get("headword_norm") or "")
            or str(row.get("pron_effective") or "") != str(row.get("pron_norm") or "")
            or str(row.get("pron_norm") or "") != str(row.get("pron_render") or "")
            or has_alerts
        )

    def _update_preview_stats(self) -> None:
        rows = self.preview_payload.get("rows") if isinstance(self.preview_payload, dict) else []
        rows = rows if isinstance(rows, list) else []

        changed_count = 0
        alerts_count = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            if self._row_changed(row):
                changed_count += 1
            alerts = row.get("alerts", [])
            alerts_count += len(alerts) if isinstance(alerts, list) else 0

        self.sample_label.setText(
            f"Entrées prévisualisées: {len(rows)} (échantillon figé: {len(self._preview_sample_ids)})"
        )
        self.modified_label.setText(f"Modifiées: {changed_count}")
        self.alerts_label.setText(f"Alertes: {alerts_count}")
        self.preview_time_label.setText(
            f"Dernière preview: {self._last_preview_time} (non appliquée)"
        )
        self.pending_badge.setText("Modifications non appliquées" if self._preview_dirty else "")

    def _render_preview_rows(self) -> None:
        rows = self.preview_payload.get("rows") if isinstance(self.preview_payload, dict) else None
        if not isinstance(rows, list):
            self.preview_table.setRowCount(0)
            self._update_preview_stats()
            return

        diff_only = self.diff_only_toggle.isChecked()
        self.preview_table.setRowCount(0)

        changed_bg = self.preview_table.palette().alternateBase()

        for row in rows:
            if not isinstance(row, dict):
                continue

            row_changed = self._row_changed(row)
            if diff_only and not row_changed:
                continue

            table_row = self.preview_table.rowCount()
            self.preview_table.insertRow(table_row)

            features_text = str(row.get("features", {}))
            alerts_text = ", ".join([str(x) for x in row.get("alerts", [])])

            values = [
                str(row.get("headword_raw") or ""),
                str(row.get("headword_effective") or ""),
                str(row.get("headword_norm") or ""),
                str(row.get("pron_raw") or ""),
                str(row.get("pron_effective") or ""),
                str(row.get("pron_norm") or ""),
                str(row.get("pron_render") or ""),
                features_text,
                alerts_text,
                "✓" if row_changed else "",
                "oui" if bool(row.get("overridden")) else "",
            ]

            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 7:
                    item.setToolTip(features_text)
                if col == 8 and alerts_text:
                    item.setToolTip(alerts_text)
                if col in {2, 5, 6}:
                    if (
                        (
                            col == 2
                            and str(row.get("headword_effective") or "")
                            != str(row.get("headword_norm") or "")
                        )
                        or (
                            col == 5
                            and str(row.get("pron_effective") or "")
                            != str(row.get("pron_norm") or "")
                        )
                        or (
                            col == 6
                            and str(row.get("pron_norm") or "") != str(row.get("pron_render") or "")
                        )
                    ):
                        item.setBackground(changed_bg)
                if col == 8 and alerts_text:
                    item.setBackground(changed_bg)
                self.preview_table.setItem(table_row, col, item)

        self._update_preview_stats()

    def _on_apply_finished(self, payload: dict[str, object]) -> None:
        self.progress.hide()
        self.state.notify_data_changed()
        self.refresh_history()

        self._preview_dirty = False
        self._update_preview_stats()

        self.apply_summary.setPlainText(
            "\n".join(
                [
                    f"Convention: {payload.get('profile_id', '-')}",
                    f"Version: {payload.get('profile_version', '-')}",
                    f"Entrées mises à jour: {payload.get('entries_updated', 0)}",
                    f"Alertes: {payload.get('alerts_count', 0)}",
                    f"Temps (ms): {payload.get('elapsed_ms', 0)}",
                ]
            )
        )
        QMessageBox.information(self, "Conventions", "Convention appliquée.")

    def _on_apply_failed(self, trace: str) -> None:
        self.progress.hide()
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        friendly = lines[-1] if lines else "Erreur inconnue"
        QMessageBox.critical(self, "Appliquer conventions", friendly)

    def apply_selected(self) -> None:
        if not self.state.project_dir or not self._current_corpus():
            QMessageBox.warning(self, "Conventions", "Sélectionnez d'abord un corpus actif.")
            return

        profile_path = self._selected_profile_path()
        if not profile_path:
            QMessageBox.warning(self, "Conventions", "Sélectionnez une convention.")
            return

        try:
            effective_profile_path = self._effective_profile_path(profile_path)
        except (PipelineError, ProfileValidationError) as exc:
            QMessageBox.warning(self, "Conventions", str(exc))
            return

        self.progress.show()
        self.current_job = JobThread(
            apply_convention,
            self.state.project_dir,
            self._current_corpus() or "",
            str(effective_profile_path),
        )
        self.current_job.signals.finished.connect(self._on_apply_finished)
        self.current_job.signals.failed.connect(self._on_apply_failed)
        self.current_job.start()

    def refresh_history(self) -> None:
        corpus_id = self._current_corpus()
        if not self.state.store or not corpus_id:
            self.history_table.setRowCount(0)
            self.status_label.setText("Conventions actives pour ce corpus : -")
            return

        rows = self.state.store.list_convention_applications(corpus_id, limit=30)
        self.history_table.setRowCount(0)
        for row in rows:
            row_data = dict(row)
            table_row = self.history_table.rowCount()
            self.history_table.insertRow(table_row)
            values = [
                str(row_data.get("created_at") or ""),
                str(row_data.get("profile_id") or ""),
                str(row_data.get("profile_version") or ""),
                str(row_data.get("profile_sha256") or "")[:12],
                str(row_data.get("entries_count") or 0),
                str(row_data.get("issues_count") or 0),
                str(row_data.get("status") or ""),
            ]
            for col, value in enumerate(values):
                self.history_table.setItem(table_row, col, QTableWidgetItem(value))

        if rows:
            latest = dict(rows[0])
            self.status_label.setText(
                "Conventions actives pour ce corpus : {pid} (v{version})".format(
                    pid=latest.get("profile_id", "-"),
                    version=latest.get("profile_version", "-"),
                )
            )
        else:
            self.status_label.setText("Conventions actives pour ce corpus : -")

        self._update_next_step_state()

    def _next_copy_target(self, source: Path) -> Path:
        rules_dir = project_paths(self.state.project_dir).rules_dir  # type: ignore[arg-type]
        stem = f"{source.stem}_copy"
        candidate = rules_dir / f"{stem}.yml"
        idx = 2
        while candidate.exists():
            candidate = rules_dir / f"{stem}_{idx}.yml"
            idx += 1
        return candidate

    def duplicate_profile(self) -> None:
        if not self.state.project_dir:
            return
        source = self._selected_profile_path()
        if not source:
            return
        target = self._next_copy_target(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        self.refresh()

    def import_yaml(self) -> None:
        if not self.state.project_dir:
            return
        src, _ = QFileDialog.getOpenFileName(self, "Importer YAML", "", "YAML (*.yml *.yaml)")
        if not src:
            return
        source = Path(src)
        target_dir = project_paths(self.state.project_dir).rules_dir
        target = target_dir / source.name
        idx = 2
        while target.exists():
            target = target_dir / f"{source.stem}_{idx}{source.suffix}"
            idx += 1
        shutil.copy2(source, target)
        self.refresh()

    def export_yaml(self) -> None:
        source = self._selected_profile_path()
        if not source:
            return
        dst, _ = QFileDialog.getSaveFileName(
            self,
            "Exporter YAML",
            source.name,
            "YAML (*.yml *.yaml)",
        )
        if not dst:
            return
        target = Path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    def reset_settings(self) -> None:
        profile_path = self._selected_profile_path()
        if not profile_path:
            return

        try:
            profile = load_profile(profile_path)
        except ProfileValidationError as exc:
            QMessageBox.warning(self, "Conventions", str(exc))
            return

        current = self._settings_payload()
        baseline = self._settings_payload_from_profile(profile)
        if current != baseline:
            answer = QMessageBox.question(
                self,
                "Réinitialiser",
                "Réinitialiser les réglages à la convention sélectionnée ?",
            )
            if answer != QMessageBox.StandardButton.Yes:
                return

        self._apply_profile_to_settings(profile)
        self._queue_auto_preview()

    def save_as_new_convention(self) -> None:
        if not self.state.project_dir:
            return
        profile_path = self._selected_profile_path()
        corpus_id = self._current_corpus()
        if not profile_path or not corpus_id:
            QMessageBox.warning(
                self, "Conventions", "Sélectionnez d'abord un corpus et une convention."
            )
            return

        dialog = SaveConventionDialog(default_name=f"{profile_path.stem}_custom", parent=self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        profile_name = dialog.convention_name
        profile_slug = slugify(profile_name) or "convention_custom"

        try:
            base_profile = load_profile(profile_path)
            settings = self._settings_payload()
            settings.update(
                {
                    "profile_id": profile_slug,
                    "name": profile_name,
                    "description": dialog.description,
                    "version": 1,
                }
            )
            new_profile = build_profile_from_ui(base_profile, settings)
            yaml_text = profile_to_yaml_text(new_profile)
        except (ProfileValidationError, ValueError) as exc:
            QMessageBox.warning(self, "Conventions", str(exc))
            return

        rules_dir = project_paths(self.state.project_dir).rules_dir
        target_dir = rules_dir / corpus_id / "conventions"
        target_dir.mkdir(parents=True, exist_ok=True)

        target = target_dir / f"{profile_slug}.yml"
        idx = 2
        while target.exists():
            target = target_dir / f"{profile_slug}_{idx}.yml"
            idx += 1

        target.write_text(yaml_text, encoding="utf-8")
        self.refresh()
        self._select_profile_path(target)
        QMessageBox.information(self, "Conventions", f"Convention enregistrée: {target.name}")

    def _select_profile_path(self, target: Path) -> None:
        for idx in range(self.profile_list.count()):
            item = self.profile_list.item(idx)
            if not item:
                continue
            item_path = Path(str(item.data(Qt.ItemDataRole.UserRole) or "")).resolve()
            if item_path == target.resolve():
                self.profile_list.setCurrentRow(idx)
                return

    def open_compare(self) -> None:
        corpus = self._current_corpus()
        if corpus:
            self.state.request_compare([corpus])

    def _update_next_step_state(self) -> None:
        if not self.state.store or not self._current_corpus():
            self.next_step_label.setText(
                "Étape suivante recommandée: sélectionner un corpus actif."
            )
            self.next_step_btn.setEnabled(False)
            return
        entries_count = self.state.store.count_entries(self._current_corpus() or "")
        if entries_count > 0:
            self.next_step_label.setText("Étape suivante recommandée: Atelier Comparer.")
            self.next_step_btn.setEnabled(True)
            return
        self.next_step_label.setText(
            "Étape suivante recommandée: appliquer la convention au corpus."
        )
        self.next_step_btn.setEnabled(False)

    def preview_named_convention(self, profile_name: str) -> None:
        if self.profile_list.count() == 0:
            return
        target = profile_name.strip().lower()
        for idx in range(self.profile_list.count()):
            item = self.profile_list.item(idx)
            if not item:
                continue
            profile_path = Path(str(item.data(Qt.ItemDataRole.UserRole) or ""))
            if profile_path.stem.lower() == target:
                self.profile_list.setCurrentRow(idx)
                self.preview_selected()
                return
        QMessageBox.information(
            self,
            "Conventions",
            f"Convention introuvable: {profile_name}",
        )

    def reset_layout(self) -> None:
        self.split.setSizes([420, 860, 460])

    def closeEvent(self, event: QCloseEvent) -> None:
        self._preview_debounce.stop()
        if self.preview_job and self.preview_job.isRunning():
            self.preview_job.cancel()
            self.preview_job.wait(1500)
        if self.current_job and self.current_job.isRunning():
            self.current_job.cancel()
            self.current_job.wait(1500)
        super().closeEvent(event)
