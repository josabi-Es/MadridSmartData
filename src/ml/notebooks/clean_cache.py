"""Delete local caches and executed-notebook copies. Never touches
`data/gold/ml_runs/*.parquet` (the per-model comparison rows `promote_winner`
reads) -- only `ml_runs/executed/`, which is a pure inspection artifact.

Usage: python -m src.ml.notebooks.clean_cache
"""

import shutil
from pathlib import Path

TARGETS = [".pytest_cache", "data/gold/ml_runs/executed"]


def main() -> None:
    for target in TARGETS:
        shutil.rmtree(target, ignore_errors=True)
    for cache_dir in Path(".").rglob("__pycache__"):
        shutil.rmtree(cache_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
