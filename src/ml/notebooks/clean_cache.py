"""Delete local caches. Never touches anything under `data/`.

Usage: python -m src.ml.notebooks.clean_cache
"""

import shutil
from pathlib import Path

TARGETS = [".pytest_cache"]


def main() -> None:
    for target in TARGETS:
        shutil.rmtree(target, ignore_errors=True)
    for cache_dir in Path(".").rglob("__pycache__"):
        shutil.rmtree(cache_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
