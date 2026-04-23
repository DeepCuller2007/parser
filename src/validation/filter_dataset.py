import argparse
import json
import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import pandera.pandas as pa

from listings_schema import photo_listings_schema, raw_listings_schema


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data/raw/listings.json"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data/processed/listings_valid.json"
DEFAULT_ERRORS_PATH = PROJECT_ROOT / "data/processed/listings_validation_errors.csv"
REMOVED_COLUMNS = {"price_per_m2_rub", "living_area_m2", "kitchen_area_m2"}


def _resolve_project_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _load_listings(path: str | Path) -> pd.DataFrame:
    path = _resolve_project_path(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        raise ValueError("Dataset JSON must contain a list of listing objects.")

    df = pd.DataFrame(payload)
    return df.drop(columns=[col for col in REMOVED_COLUMNS if col in df.columns])


def _save_listings(path: str | Path, df: pd.DataFrame) -> None:
    path = _resolve_project_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    records = df.where(pd.notna(df), None).to_dict(orient="records")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def _save_errors(path: str | Path, failure_cases: pd.DataFrame) -> None:
    path = _resolve_project_path(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    failure_cases.to_csv(path, index=False, encoding="utf-8")


def _filter_valid_rows(df: pd.DataFrame, schema: pa.DataFrameSchema) -> Tuple[pd.DataFrame, pd.DataFrame]:
    candidate_df = df.copy()
    all_failure_cases = []
    dropped_indexes = set()

    while True:
        try:
            valid_df = schema.validate(candidate_df, lazy=True)
            if not all_failure_cases:
                return valid_df, pd.DataFrame()
            return valid_df, pd.concat(all_failure_cases, ignore_index=True)
        except pa.errors.SchemaErrors as e:
            failure_cases = e.failure_cases.copy()
            all_failure_cases.append(failure_cases)

            failed_indexes = {
                index
                for index in failure_cases.get("index", pd.Series(dtype=object)).dropna().unique()
                if index in candidate_df.index
            }
            new_failed_indexes = failed_indexes - dropped_indexes

            if not new_failed_indexes:
                unresolved_errors = pd.concat(all_failure_cases, ignore_index=True)
                return candidate_df.iloc[0:0].copy(), unresolved_errors

            dropped_indexes.update(new_failed_indexes)
            candidate_df = candidate_df.drop(index=list(new_failed_indexes)).copy()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and filter parsed CIAN listings.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to raw listings JSON.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to write valid listings JSON.")
    parser.add_argument("--errors", default=DEFAULT_ERRORS_PATH, help="Path to write validation errors CSV.")
    parser.add_argument(
        "--require-photos",
        action="store_true",
        help="Require at least one MinIO image path for each listing.",
    )
    args = parser.parse_args()
    input_path = _resolve_project_path(args.input)
    output_path = _resolve_project_path(args.output)
    errors_path = _resolve_project_path(args.errors)

    schema = photo_listings_schema if args.require_photos else raw_listings_schema
    print(f"[INFO] Project root: {PROJECT_ROOT}")
    print(f"[INFO] Input dataset: {input_path}")
    print(f"[INFO] Output dataset: {output_path}")
    print(f"[INFO] Errors report: {errors_path}")

    raw_df = _load_listings(input_path)
    valid_df, failure_cases = _filter_valid_rows(raw_df, schema)

    _save_listings(output_path, valid_df)
    if not failure_cases.empty:
        _save_errors(errors_path, failure_cases)

    print(f"[INFO] Input rows: {len(raw_df)}")
    print(f"[INFO] Valid rows: {len(valid_df)}")
    print(f"[INFO] Dropped rows: {len(raw_df) - len(valid_df)}")
    print(f"[INFO] Valid dataset written to: {output_path}")
    if not failure_cases.empty:
        print(f"[INFO] Validation errors written to: {errors_path}")
        if "index" in failure_cases.columns and failure_cases["index"].isna().any():
            print("[WARN] Some validation errors were dataset-level and did not point to a single row.")


if __name__ == "__main__":
    main()
