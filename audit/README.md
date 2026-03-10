# Audit folder

Contenu produit pendant l'audit fonctionnel (sans modification des fichiers applicatifs):

- `summary_executif.md`: synthese management + plan P0/P1/P2
- `audit_fonctionnel_detail.md`: audit detaille, preuves, references code
- `functional_inventory.csv`: inventaire brut de 213 controles UI
- `functional_inventory_classified.csv`: inventaire classe par statut fonctionnel
- `cli_command_matrix.csv`: matrice commandes/parametres/branchements CLI
- `cli_audit.md`: synthese CLI + ecart principal

Verifications executees:

- `python -m pytest -q` -> 92 passed
- `python -m dicodiachro --help`
- `python -m dicodiachro import --help`
