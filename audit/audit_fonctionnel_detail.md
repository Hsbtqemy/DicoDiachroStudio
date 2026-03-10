# Audit fonctionnel detaille - DicoDiachroStudio

Date: 2026-03-10

## Mise a jour remediation (2026-03-10)

- F1 (P0, Curation filtres apres pagination): corrige.
- F2 (P1, Compare `algorithm` non effectif): corrige.
- F3 (P1, onglets non exposes): corrige (onglets montes dans `MainWindow`).
- F4 (P1, Export path picker): corrige (chemin utilisateur respecte).
- F5 (P2, Templates `Diff view`): corrige (rerender immediat).
- F6 (P2, feedback `Reset layout`): corrige.
- F7 (P2, details Curation sur selection): corrige.
- Verification post-remediation: `python -m pytest -q` -> 103 passed, 0 failed.
- Note: les sections suivantes conservent le snapshot de l'audit initial; se referer a cette section pour le statut courant.

## 1) Methode

1. Inventaire statique AST de tous les widgets/actions/signaux dans `studio/dicodiachro_studio/ui`.
2. Verification des branchements des controles jusqu'aux appels core (`dicodiachro.core.*`) et `AppState`.
3. Verification CLI via lecture de `src/dicodiachro/cli/app.py` + execution `python -m dicodiachro --help`.
4. Validation technique par tests: `python -m pytest -q`.

## 2) Couverture analysee

### UI recensee

- Main window: `studio/dicodiachro_studio/ui/main_window.py`
- Onglets montes: Import, Atelier gabarits, Curation, Conventions, Comparer, Export
- Onglets non montes: AlignTab, ProjectTab, ProfilesTab
- Dialogues: create/add/save + dialogues atelier templates

### CLI recensee

- Commandes racine + sous-commandes `import`, `export`, `filter`, `parser`, `profile`, `template`, `convention`, `compare`

## 3) Resultats inventaire

Source brute: `audit/functional_inventory.csv`

- Total controles interactifs detectes: 213
- Controles accessibles: 194
- Controles non exposes (present mais inaccessibles): 19

Repartition notable (classes accessibles):

- ImportTab: 36
- TemplatesTab: 36
- EntriesTab: 30
- ConventionsTab: 31
- CompareTab: 23
- ExportTab: 6
- MainWindow menus/actions: 10

Controles presents mais non exposes:

- `AlignTab` (5) `studio/dicodiachro_studio/ui/tabs/align_tab.py:21`
- `ProjectTab` (4) `studio/dicodiachro_studio/ui/tabs/project_tab.py:21`
- `ProfilesTab` (10) `studio/dicodiachro_studio/ui/tabs/profiles_tab.py:37`

Cause: ces classes existent mais ne sont pas ajoutees dans le `QTabWidget` principal (`studio/dicodiachro_studio/ui/main_window.py:41-46`).

## 4) Verification branchement reel (present vs reellement branche)

### 4.1 Confirme reellement branche

- Import -> importers + pipeline (`import_text_batch`, `import_csv_batch`, `import_pdf_text`, `import_from_share_link`, `run_pipeline`) dans `studio/dicodiachro_studio/ui/tabs/import_tab.py`.
- Atelier templates -> preview/apply + overrides (`preview_template_on_source`, `apply_template_to_corpus`, `upsert_override_record`) dans `studio/dicodiachro_studio/ui/tabs/templates_tab.py`.
- Curation -> operations d'override/edition/split/merge/delete/restore (`dicodiachro.core.overrides`) dans `studio/dicodiachro_studio/ui/tabs/entries_tab.py`.
- Conventions -> preview/apply (`preview_convention`, `apply_convention`) dans `studio/dicodiachro_studio/ui/tabs/conventions_tab.py`.
- Compare -> preview/apply/export (`preview_*`, `apply_compare_run`, `export_compare_*`) dans `studio/dicodiachro_studio/ui/tabs/compare_tab.py`.
- Export -> exporters CSV/JSONL/XLSX/DOCX + session JSON dans `studio/dicodiachro_studio/ui/tabs/export_tab.py`.

### 4.2 Present mais pas reellement branche / pas expose

1. **Onglets non montes**
- Classes presentes, signaux connectes localement, mais jamais accessibles depuis l'UI principale.
- Evidence: `studio/dicodiachro_studio/ui/main_window.py:41-46` + classes `align_tab.py:21`, `project_tab.py:21`, `profiles_tab.py:37`.

2. **Option Compare `algorithm` (UI + CLI) non fonctionnelle**
- UI expose une strategie (`studio/dicodiachro_studio/ui/tabs/compare_tab.py:94-95`, `:410`).
- CLI expose `--algorithm` (`src/dicodiachro/cli/app.py:817`).
- Le workflow recupere `algorithm` (`src/dicodiachro/core/compare/workflow.py:699`) mais n'applique aucune branche algorithmique dans l'alignement (appel unique `preview_alignment` sans algo, `:708`).
- `algorithm` est seulement persiste dans les metadonnees (`:730`, `:765`).

## 5) Findings priorises

## P0 - corriger

### F1. Curation: filtrage applique apres pagination (risque de faux-negatifs visuels)

- Evidence:
  - chargement page brute: `studio/dicodiachro_studio/ui/tabs/entries_tab.py:478`
  - filtres status/flags appliques ensuite en memoire: `:495`, `:499`, `:501`
- Impact:
  - la page courante peut sembler vide alors que des lignes correspondantes existent plus loin.
  - resultat fonctionnel non fiable pour curation (selection/edit) selon offset.
- Action:
  - pousser status/manual/flags dans la requete SQL de `list_entries` avant `LIMIT/OFFSET`.

## P1 - completer

### F2. Compare `algorithm` est un faux parametre (present mais non effectif)

- Evidence: `compare_tab.py:94-95`, `:410`; `cli/app.py:817`; `compare/workflow.py:699`, `:708`, `:730`.
- Impact: l'utilisateur choisit un algo qui ne change pas le resultat.
- Action: implementer le branchement reel (ex: greedy vs autre) ou retirer le parametre.

### F3. Trois onglets complets non exposes dans l'UI principale

- Evidence: `main_window.py:41-46` n'ajoute que 6 tabs (Import/Templates/Entries/Conventions/Compare/Export).
- Onglets hors parcours: `AlignTab`, `ProjectTab`, `ProfilesTab`.
- Impact: fonctionnalites et controles presents dans le code mais inaccessibles en prod.
- Action: decision produit explicite (monter ou deprecier/supprimer).

### F4. Export tab ignore le dossier choisi dans le file picker

- Evidence:
  - selection chemin utilisateur: `studio/dicodiachro_studio/ui/tabs/export_tab.py:77`, `:90`, `:105`, `:121`, `:137`
  - ecriture forcee dans `data/derived` avec seul nom de fichier: `:83`, `:98`, `:114`, `:130`, `:149`
- Impact: comportement different de l'intention implicite du selecteur de chemin.
- Action: utiliser le chemin complet choisi (ou expliciter UX "export toujours dans le projet").

## P2 - polish

### F5. Templates `Diff view` non reactif tant que preview n'est pas relancee

- Evidence: checkbox `studio/dicodiachro_studio/ui/tabs/templates_tab.py:308`; usage seulement dans `_render_preview` `:781`.
- Impact: decalage UX (etat du toggle visible mais table non rafraichie immediatement).
- Action: connecter `diff_only.toggled` a un rerender local de la preview deja chargee.

### F6. Action globale "Reset layout" partiellement applicable

- Evidence: action globale `studio/dicodiachro_studio/ui/main_window.py:80`, logique `:136`.
- Seuls `EntriesTab` et `ConventionsTab` implementent `reset_layout` (`entries_tab.py:1224`, `conventions_tab.py:1019`).
- Impact: no-op silencieux sur les autres onglets.
- Action: feedback utilisateur ou implementation uniforme.

### F7. Panneau details Curation maj sur clic souris uniquement

- Evidence: `studio/dicodiachro_studio/ui/tabs/entries_tab.py:88` (`table.clicked.connect(self.on_row_selected)`).
- Impact: navigation clavier peut laisser un panneau details stale.
- Action: connecter aussi au signal de changement de selection/courant.

## 6) Audit CLI (parametrage + branchement)

Source brute: `audit/cli_command_matrix.csv`

- Commandes detectees: 32 (incluant groupes/sous-groupes)
- Parametres recenses: 126
- Branchement global: commandes connectees aux fonctions core attendues (`run_pipeline`, `preview_*`, `apply_*`, importers/exporters, storage).
- Ecart principal confirme: `compare apply --algorithm` expose mais non effectif (voir F2).

## 7) Verification execution

- Tests: `python -m pytest -q` -> `92 passed`.
- CLI aide OK: `python -m dicodiachro --help`, `python -m dicodiachro import --help`.
- Note: `python -m dicodiachro_studio --help` n'est pas une CLI; cela lance l'app GUI (pas de mode help dedie).

## 8) Conclusion

Le socle est globalement branche et operationnel sur les flux principaux (Import -> Templates -> Curation -> Conventions -> Compare -> Export). Les ecarts les plus importants sont de type coherence fonctionnelle et exposition produit:

1. Un bug de logique de filtration/pagination en Curation (P0).
2. Un faux parametre algorithmique en Compare (P1, present mais non reellement branche).
3. Des modules UI complets presents mais non exposes (P1).
