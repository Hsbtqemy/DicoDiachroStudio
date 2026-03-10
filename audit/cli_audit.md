# Audit CLI - DicoDiachro

Mise a jour remediation 2026-03-10: l'ecart `compare --algorithm` est corrige (`greedy|mutual_best` branche en preview/apply).

Source principale: `src/dicodiachro/cli/app.py`
Source de matrice: `audit/cli_command_matrix.csv`

## Surface exposee

- Groupes Typer: `app`, `import_app`, `export_app`, `filter_app`, `parser_app`, `profile_app`, `template_app`, `convention_app`, `compare_app`
- Commandes uniques recensees: 32
- Parametres recenses: 127

## Branchements verifies

- Init/projet: `init_project`, `project_paths`
- Import: `import_text_batch`, `import_csv_batch`, `import_pdf_text`, `import_from_share_link`, `register_import_event`
- Pipeline: `run_pipeline`, `apply_profile_to_entries`, `preview_profile_entries`
- Templates: `preview_template_on_source`, `apply_template_to_corpus`
- Conventions: `preview_convention`, `apply_convention`
- Compare: `preview_coverage`, `preview_alignment`, `preview_diff`, `apply_compare_run`, `list_compare_runs`, `load_compare_run_data`
- Export: `export_entries_csv/jsonl`, `export_comparison_xlsx/docx`, `export_compare_*`

## Ecart fonctionnel detecte

- Audit initial: `compare apply --algorithm` etait expose mais non branche.
- Statut actuel: corrige (strategie `greedy|mutual_best` appliquee dans `preview_alignment` et `apply_compare_run`).

## Conclusion

CLI globalement fonctionnelle et correctement branchee. L'ecart `--algorithm` est clos.
