from pathlib import Path
import sys

PARSER_PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]

for path in (str(REPO_ROOT), str(PARSER_PROJECT_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
