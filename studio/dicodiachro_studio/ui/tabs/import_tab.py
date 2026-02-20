from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dicodiachro.core.importers.pdf_import import save_pdf_as_text
from dicodiachro.core.importers.text_import import import_text_batch
from dicodiachro.core.importers.url_import import import_from_share_link
from dicodiachro.core.pipeline import register_import_event, run_pipeline
from dicodiachro.core.storage.sqlite import init_project, project_paths

from ...services.jobs import JobThread
from ...services.state import AppState


class ImportTab(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self.state = state
        self.current_job: JobThread | None = None

        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("https://... (share link)")
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.hide()

        import_local_btn = QPushButton("Import local txt/pdf")
        import_url_btn = QPushButton("Import URL")
        run_btn = QPushButton("Run pipeline")
        cancel_btn = QPushButton("Cancel")

        import_local_btn.clicked.connect(self.import_local)
        import_url_btn.clicked.connect(self.import_url)
        run_btn.clicked.connect(self.run_pipeline)
        cancel_btn.clicked.connect(self.cancel_job)

        row = QHBoxLayout()
        row.addWidget(self.url_edit)
        row.addWidget(import_url_btn)

        actions = QHBoxLayout()
        actions.addWidget(import_local_btn)
        actions.addWidget(run_btn)
        actions.addWidget(cancel_btn)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Import local files or ShareDocs URL"))
        layout.addLayout(row)
        layout.addLayout(actions)
        layout.addWidget(self.progress)
        layout.addWidget(self.log)
        self.setLayout(layout)

    def _require_project(self) -> bool:
        if not self.state.project_dir:
            QMessageBox.warning(self, "Project required", "Open a project first.")
            return False
        return True

    def _append(self, text: str) -> None:
        self.log.append(text)

    def _start_job(self, fn, *args):
        if self.current_job and self.current_job.isRunning():
            QMessageBox.information(self, "Busy", "A job is already running.")
            return
        self.current_job = JobThread(fn, *args)
        self.current_job.signals.finished.connect(self._on_job_finished)
        self.current_job.signals.failed.connect(self._on_job_failed)
        self.progress.show()
        self.current_job.start()

    def _on_job_finished(self, result):
        self.progress.hide()
        self._append(f"Done: {result}")
        self.state.notify_data_changed()

    def _on_job_failed(self, error_text: str):
        self.progress.hide()
        self._append(error_text)
        QMessageBox.critical(self, "Job failed", error_text)

    def cancel_job(self):
        if self.current_job and self.current_job.isRunning():
            self.current_job.cancel()
            self.current_job.terminate()
            self.progress.hide()
            self._append("Job cancelled (best effort).")

    def import_local(self) -> None:
        if not self._require_project():
            return
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Import local files",
            str(self.state.project_dir),
            "Text/PDF (*.txt *.pdf)",
        )
        if not files:
            return

        project_dir = self.state.project_dir
        assert project_dir is not None
        init_project(project_dir)
        paths = project_paths(project_dir)

        imported: list[str] = []
        for item in files:
            path = Path(item)
            if path.suffix.lower() == ".pdf":
                txt_path = paths.raw_dir / "imports" / f"{path.stem}.txt"
                save_pdf_as_text(path, txt_path, use_coords=False)
                imported.append(str(txt_path))
            else:
                results = import_text_batch(paths.raw_dir / "imports", path)
                imported.extend(str(p) for p in results)

        register_import_event(
            project_dir,
            {
                "type": "local",
                "files": imported,
            },
        )
        self._append(f"Imported {len(imported)} file(s)")

    def import_url(self) -> None:
        if not self._require_project():
            return
        url = self.url_edit.text().strip()
        if not url:
            return
        project_dir = self.state.project_dir
        assert project_dir is not None
        paths = project_paths(project_dir)

        def _job():
            imported, metadata = import_from_share_link(
                url=url,
                imports_dir=paths.raw_dir / "imports",
                extract_dir=paths.raw_dir / "imports" / "unzipped",
            )
            register_import_event(
                project_dir,
                {
                    "type": "url",
                    **metadata,
                    "imported_files": [str(p) for p in imported],
                },
            )
            return {"imported": len(imported)}

        self._start_job(_job)

    def run_pipeline(self) -> None:
        if not self._require_project():
            return
        if not self.state.active_dict_id:
            QMessageBox.warning(
                self, "Dictionary required", "Select an active dictionary in Project tab."
            )
            return

        project_dir = self.state.project_dir
        dict_id = self.state.active_dict_id
        profile = self.state.active_profile
        assert project_dir is not None

        self._start_job(
            run_pipeline,
            project_dir,
            dict_id,
            profile,
            None,
            True,
        )
