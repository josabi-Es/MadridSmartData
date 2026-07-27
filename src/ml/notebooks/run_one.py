"""Papermill CLI wrapper -- one notebook, one (model, variable, ano) run.

Usage: python -m src.ml.notebooks.run_one --model random_forest \
    --variable NO2 --ano 2024
"""

import argparse
from pathlib import Path

import papermill as pm

NOTEBOOKS_DIR = Path(__file__).parent
MODEL_NAMES = ["naive", "decision_tree", "random_forest", "xgboost", "mlp"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=MODEL_NAMES)
    parser.add_argument("--variable", required=True)
    parser.add_argument("--ano", type=int, required=True)
    parser.add_argument("--executed-dir", default="data/gold/ml_runs/executed")
    args = parser.parse_args()

    nb_in = NOTEBOOKS_DIR / f"{args.model}.ipynb"
    executed_dir = Path(args.executed_dir)
    executed_dir.mkdir(parents=True, exist_ok=True)
    nb_out = executed_dir / f"{args.model}_{args.variable}_{args.ano}.ipynb"

    pm.execute_notebook(
        str(nb_in),
        str(nb_out),
        parameters={"variable": args.variable, "ano": args.ano},
        kernel_name="python3",
    )


if __name__ == "__main__":
    main()
