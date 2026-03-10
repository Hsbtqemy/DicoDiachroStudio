# Resume executif - Audit fonctionnel DicoDiachroStudio

Date audit: 2026-03-10
Perimetre: GUI Studio (PySide6) + CLI Typer + branchements vers `dicodiachro.core`.
Note: la phase d'audit initiale a ete produite sans modification applicative. Cette mise a jour inclut la remediation P0/P1/P2.

## Mise a jour remediation (2026-03-10)

- P0 (Curation filtre/pagination): corrige.
- P1.1 (Compare `algorithm` present mais non branche): corrige.
- P1.2 (onglets non exposes): corrige (onglets montes dans `MainWindow`).
- P1.3 (Export path picker): corrige (chemin complet respecte).
- P2.1 (Templates `Diff view`): corrige (rerender immediat sans relancer preview).
- P2.2 (feedback `Reset layout`): corrige.
- P2.3 (details Curation sur selection): corrige.
- Restent ouverts: aucun ecart priorise issu de l'audit initial.
- Verification post-remediation: `python -m pytest -q` -> 103 passed, 0 failed.

## Resultat global

- Controleurs interactifs recenses: 213
- Controles accessibles dans l'app actuelle: 213
- Controles presents mais non exposes: 0
- Tests executes: `python -m pytest -q` -> 103 passed, 0 failed

## Points majeurs

- Tous les ecarts P0/P1/P2 identifies dans l'audit initial sont corriges.

## Plan d'action priorise

### P0 - corriger

1. Curation: deplacer les filtres `status/manual/flags` au niveau SQL avant `LIMIT/OFFSET`.
2. Ajouter un test de non-regression: filtre + pagination doivent retourner les memes ids qu'un filtrage global complet.

### P1 - completer

1. Compare: termine (branche `algorithm` implementee: `greedy` et `mutual_best`, UI+CLI+workflow).
2. Onglets non exposes: termine (montes explicitement dans `MainWindow`).
3. Export tab: termine (le chemin complet choisi est maintenant respecte).

### P2 - polish

1. Templates tab: termine (`Diff view` reactif immediatement).
2. MainWindow: termine (feedback utilisateur si `Reset layout` indisponible).
3. Entries tab: termine (maj details sur changement de selection).

## Artefacts

- `audit/functional_inventory.csv` (inventaire exhaustif des controles UI)
- `audit/cli_command_matrix.csv` (matrice commandes CLI/parametres/branchements)
- `audit/audit_fonctionnel_detail.md` (preuves, references code, classement complet)
