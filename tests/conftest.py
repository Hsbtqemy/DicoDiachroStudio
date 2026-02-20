import os

# Pytest importe conftest.py très tôt → avant les tests
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
