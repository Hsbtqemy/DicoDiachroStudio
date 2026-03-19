# Backlog : Champ « page » par entrée (PDF + visionneuse)

Backlog exécutable pour ajouter le numéro de page (PDF) par entrée, sans fichier par page. Ordre des tâches respecté pour les dépendances.

---

## Contexte technique (rappel)

- **Import PDF** : produit un seul `.txt` (toutes pages concaténées) dans `data/raw/imports/`.
- **Pipeline** : découvre les `.txt` dans `raw_dir`, appelle `parse_lines(lines, ...)` puis `store.insert_entries(entries, ...)`.
- **Modèle** : `ParsedEntry` + table `entries` avec `source_path`, `line_no` ; pas de `page` aujourd’hui.
- **Stratégie** : garder un `.txt` par PDF + **fichier sidecar** `.line_pages` (un entier par ligne = numéro de page). Le pipeline lit le sidecar s’il existe et remplit `page` sur chaque entrée.

---

## 1. Modèle et schéma

### 1.1 Ajouter `page` au modèle `ParsedEntry`

- **Fichier** : `src/dicodiachro/core/models.py`
- **Action** : Ajouter `page: int | None = None` dans le dataclass `ParsedEntry`.
- **Critère** : Les appels existants qui construisent `ParsedEntry` sans `page` restent valides (valeur par défaut).

### 1.2 Ajouter la colonne `page` en base

- **Fichier** : `src/dicodiachro/core/storage/sqlite.py`
- **Actions** :
  1. Dans `create_schema`, ajouter `page INTEGER` dans la liste des colonnes de `CREATE TABLE IF NOT EXISTS entries` (après `line_no`, avant `created_at`).
  2. Dans `_ensure_entries_columns`, ajouter `"page": "INTEGER"` dans `expected_columns` pour que les bases existantes reçoivent la colonne par migration.
- **Critère** : Nouvelle base et base existante ont une colonne `page` (NULL autorisé).

### 1.3 Insérer et lire `page` dans le store

- **Fichier** : `src/dicodiachro/core/storage/sqlite.py`
- **Actions** :
  1. Dans `insert_entries` : ajouter `entry.page` (ou `getattr(entry, "page", None)`) dans le tuple des valeurs et dans la liste des colonnes de l’`INSERT` (ex. après `line_no`). Ajuster le nombre de placeholders (37 → 38).
  2. Dans `insert_entry` (création manuelle) : ajouter le paramètre optionnel `page: int | None = None`, l’inclure dans l’`INSERT` et dans le payload si nécessaire pour cohérence. Vérifier la liste des colonnes utilisées.
  3. S’assurer que `list_entries` et tout `SELECT * FROM entries` renvoient bien `page` (déjà le cas si `SELECT *`).
- **Critère** : Une entrée insérée avec `page=3` est lue avec `row["page"] == 3` ; les entrées sans page ont `row["page"]` à `None`.

---

## 2. Extraction PDF et sidecar

### 2.1 Retourner les numéros de page par ligne depuis l’extracteur PDF

- **Fichier** : `src/dicodiachro/core/importers/pdf_text_import.py`
- **Actions** :
  1. Modifier `extract_pdf_text_lines` pour retourner non plus `tuple[list[str], int, int]` mais par ex. `tuple[list[str], list[int], int, int]` : `(lines, line_pages, pages_total, pages_with_text)` où `line_pages[i]` est le numéro de page (1-based) de `lines[i]`.
  2. Dans la boucle `for page in pdf.pages`, avant d’étendre `all_lines`, enregistrer le numéro de page courant (1-based) pour chaque ligne de `page_lines` et l’ajouter à une liste `all_line_pages`.
- **Critère** : `len(line_pages) == len(lines)` ; chaque ligne a le bon numéro de page.

### 2.2 Écrire le fichier sidecar `.line_pages` à l’import PDF

- **Fichier** : `src/dicodiachro/core/importers/pdf_text_import.py`
- **Actions** :
  1. Modifier `import_pdf_text` pour utiliser le nouveau retour de `extract_pdf_text_lines` (avec `line_pages`).
  2. Après avoir écrit `target_txt`, écrire un fichier `target_txt.parent / (target_txt.name + ".line_pages")` (ou convention claire du type `{stem}.txt.line_pages`) contenant un entier par ligne (un par ligne du .txt), au format texte (un nombre par ligne), dans le même ordre que les lignes du .txt.
  3. Ne pas modifier la signature publique de `import_pdf_text` (retour `PDFTextImportResult` inchangé) sauf si on décide d’y exposer les infos de pages (optionnel).
- **Critère** : Après import d’un PDF de 5 pages, le .txt a N lignes et le .line_pages a N lignes d’entiers entre 1 et 5.

---

## 3. Parsing et pipeline

### 3.1 Accepter `line_pages` dans `parse_lines`

- **Fichier** : `src/dicodiachro/core/parsing.py`
- **Actions** :
  1. Ajouter un paramètre optionnel `line_pages: list[int] | None = None` à `parse_lines`. Si fourni, exiger `len(line_pages) == len(lines)` (ou adapter si `lines` est un itérable : construire une liste pour l’indexation).
  2. Lors de la construction de chaque `ParsedEntry` (deux branches : preset et `ENTRY_RE`), définir `page=line_pages[line_no - 1] if line_pages is not None and 0 <= line_no - 1 < len(line_pages) else None` (ou équivalent selon la structure exacte).
- **Critère** : Appel avec `line_pages=[1,1,2,2]` et 4 lignes parsées produit des entrées avec `page` 1, 1, 2, 2 ; sans `line_pages`, `page` reste `None`.

### 3.2 Charger le sidecar dans le pipeline et le passer à `parse_lines`

- **Fichier** : `src/dicodiachro/core/pipeline.py`
- **Actions** :
  1. Dans `_parse_source`, après avoir lu `lines` depuis `path`, vérifier si un fichier sidecar existe (ex. `Path(str(path) + ".line_pages")` ou `path.with_suffix(path.suffix + ".line_pages")` selon la convention choisie en 2.2).
  2. Si le fichier existe, le lire : une ligne = un entier (page), ordre identique aux lignes du .txt. Construire `line_pages: list[int]` de même longueur que `lines` (tronquer ou compléter avec 0/1 si nécessaire pour éviter les index out of range).
  3. Appeler `parse_lines(..., line_pages=line_pages)` ou `line_pages=None` si pas de sidecar.
- **Critère** : Lancer le pipeline sur un .txt produit par l’import PDF (avec .line_pages) remplit `page` sur les entrées ; sur un .txt sans sidecar, `page` reste `None`.

---

## 4. Autres chemins qui créent des entrées

### 4.1 Rebuild / apply_profile / conventions

- **Fichiers** : `src/dicodiachro/core/pipeline.py` (ex. `apply_profile_to_entries`), `src/dicodiachro/core/overrides.py` (merge, split, etc.), et tout endroit qui lit des lignes depuis la BDD et recrée des `ParsedEntry` ou insère des lignes.
- **Actions** :
  1. Lorsqu’on reconstruit un `ParsedEntry` à partir d’une ligne BDD (ex. dans `apply_profile_to_entries`), ajouter `page=int(row["page"]) if row.get("page") is not None else None` (ou équivalent) pour ne pas perdre l’attribut.
  2. Dans `merge_entries` / `split_entry` / tout code qui crée ou met à jour des entrées, décider de la règle pour `page` (ex. conserver la page de l’entrée « principale » en merge, dupliquer en split) et l’appliquer. Si le schéma n’expose pas encore `page` dans ces chemins, au minimum s’assurer qu’aucun INSERT/UPDATE ne supprime la colonne (les nouveaux champs sont en général ajoutés, pas supprimés).
- **Critère** : Après apply_profile ou après merge/split, les entrées concernées ont toujours un `page` cohérent (ou NULL si non défini).

### 4.2 Insert manuel (Add Entry) et overrides

- **Fichier** : `src/dicodiachro/core/storage/sqlite.py` (`insert_entry`)
- **Action** : Déjà couvert en 1.3 : paramètre optionnel `page` et colonne dans l’INSERT. Les entrées manuelles ont typiquement `page=None`.
- **Critère** : Création d’une entrée à la main ne provoque pas d’erreur et laisse `page` à NULL.

---

## 5. UI – Onglet Entrées

### 5.1 Afficher la colonne « page »

- **Fichier** : `studio/dicodiachro_studio/ui/tabs/entries_tab.py`
- **Actions** :
  1. Ajouter une colonne « page » au modèle de la table (ex. après « section » ou « headword »). Incrémenter le nombre de colonnes du modèle (9 → 10) et ajouter l’en-tête « page » dans `setHorizontalHeaderLabels`.
  2. Dans la boucle de remplissage du modèle (dans `refresh`), pour chaque ligne, ajouter la valeur `str(row.get("page") or "")` dans le tuple `values` au bon index, et créer l’item correspondant.
  3. Mettre à jour les constantes de colonnes (ex. `COL_PAGE`) et `NOMENCLATURE_COLUMNS` si on souhaite inclure ou exclure « page » en Vue Nomenclature (recommandation : inclure « page » dans la vue nomenclature).
- **Critère** : La colonne page s’affiche ; les entrées issues d’un PDF importé après les changements affichent leur numéro de page.

### 5.2 Filtre par page(s)

- **Fichier** : `src/dicodiachro/core/storage/sqlite.py` et `studio/.../entries_tab.py`
- **Actions** :
  1. Dans `list_entries`, ajouter des paramètres optionnels `page_min: int | None = None` et `page_max: int | None = None` (ou `page: int | None = None` pour une seule page). Si fournis, ajouter des conditions `AND page BETWEEN ? AND ?` (ou `AND page = ?`) et les paramètres correspondants.
  2. Dans l’onglet Entrées, ajouter un contrôle (ex. deux spinbox « Page de » / « Page à », ou une seule « Page ») et un bouton ou filtre automatique. Lorsque l’utilisateur définit une plage, appeler `list_entries(..., page_min=..., page_max=...)` (ou équivalent).
- **Critère** : En filtrant par « page 3 », seules les entrées de la page 3 s’affichent.

---

## 6. Export et détail

### 6.1 Exporter la colonne `page`

- **Fichiers** : `src/dicodiachro/core/exporters/csv_jsonl.py`, et tout export CSV/JSONL qui liste les champs d’entrée.
- **Action** : Ajouter `"page"` dans la liste des champs exportés et remplir la valeur depuis `row.get("page")`.
- **Critère** : L’export contient une colonne `page` avec les valeurs ou vide.

### 6.2 Détail d’entrée (JSON)

- **Fichier** : `studio/.../entries_tab.py` (panneau détail / snapshot).
- **Action** : S’assurer que le snapshot ou le JSON affiché inclut `page` (souvent déjà le cas si le détail est construit depuis la ligne BDD complète).
- **Critère** : En sélectionnant une entrée, le détail affiche `"page": 3` si défini.

---

## 7. Tests

### 7.1 Tests unitaires

- **Fichiers** : `tests/test_pdf_text_import.py`, `tests/test_parsing.py`, tests du store si présents.
- **Actions** :
  1. `test_pdf_text_import` : vérifier qu’après import, le fichier `.line_pages` existe et que les entiers correspondent aux pages.
  2. `parse_lines` : test avec `line_pages` et sans ; vérifier la présence de `page` sur les `ParsedEntry`.
  3. Store : test d’insertion avec `page` et lecture.
- **Critère** : Les tests existants passent ; les nouveaux tests couvrent page / sidecar / pipeline.

### 7.2 Test de bout en bout (optionnel)

- **Action** : Import d’un PDF multi-pages → lancement du pipeline → vérification en base et dans l’UI que les entrées ont la colonne page remplie et que le filtre par page fonctionne.
- **Critère** : Scénario complet sans régression.

---

## Ordre d’exécution recommandé

| Étape | Tâches | Dépendances |
|-------|--------|-------------|
| 1 | 1.1 Modèle ParsedEntry | - |
| 2 | 1.2 Schéma + 1.3 Store (insert/read) | 1.1 |
| 3 | 2.1 Extracteur retourne line_pages | - |
| 4 | 2.2 Écrire .line_pages à l’import PDF | 2.1 |
| 5 | 3.1 parse_lines accepte line_pages | 1.1 |
| 6 | 3.2 Pipeline charge sidecar et appelle parse_lines avec line_pages | 2.2, 3.1 |
| 7 | 4.1 et 4.2 Rebuild / merge / split / insert manuel | 1.3 |
| 8 | 5.1 Colonne page dans l’onglet Entrées | 1.3 |
| 9 | 5.2 Filtre par page (list_entries + UI) | 1.3, 5.1 |
| 10 | 6.1 Export, 6.2 Détail | 1.3 |
| 11 | 7.1 et 7.2 Tests | Toutes |

---

## Fichiers impactés (résumé)

| Fichier | Changements |
|---------|-------------|
| `src/dicodiachro/core/models.py` | `ParsedEntry.page` |
| `src/dicodiachro/core/storage/sqlite.py` | Schéma, migration, insert_entries, insert_entry, list_entries (filtre) |
| `src/dicodiachro/core/importers/pdf_text_import.py` | extract_pdf_text_lines (line_pages), import_pdf_text (écriture sidecar) |
| `src/dicodiachro/core/parsing.py` | parse_lines(line_pages=...) et passage à ParsedEntry |
| `src/dicodiachro/core/pipeline.py` | _parse_source : lecture .line_pages, appel parse_lines avec line_pages |
| `src/dicodiachro/core/pipeline.py` (apply_profile_to_entries) | Reconstruction ParsedEntry avec page depuis row |
| `src/dicodiachro/core/overrides.py` | Règles page pour merge/split si nécessaire |
| `src/dicodiachro/core/exporters/csv_jsonl.py` | Champ page à l’export |
| `studio/.../ui/tabs/entries_tab.py` | Colonne page, constantes, filtre page, détail |
| `tests/test_pdf_text_import.py` | Sidecar et line_pages |
| `tests/test_parsing.py` | parse_lines avec line_pages |

---

## Convention du fichier sidecar

- **Nom** : `{chemin_du_fichier_txt}.line_pages` (ex. `mon_doc-abc123.txt.line_pages`).
- **Format** : une ligne par ligne du .txt ; chaque ligne contient un entier (numéro de page 1-based). Même nombre de lignes que le .txt.
- **Encodage** : UTF-8 (ou ASCII suffisant pour des entiers).

Ce document peut servir de référence unique pour l’implémentation et le suivi (cases à cocher, PR, etc.).
