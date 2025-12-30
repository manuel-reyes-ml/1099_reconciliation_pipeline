"""
generate_sample_data.py

Seeded generator for synthetic Matrix and Relius sample inputs.

This script writes four Excel files into data/sample/, using raw headers that
match the column maps in src/config.py so load_data.py validation passes.
The outputs are deterministic given a seed and include both valid and edge-case
rows to exercise normalization and validation logic.
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd

from src.config import (
    INHERITED_PLAN_IDS,
    MATRIX_COLUMN_MAP,
    RELIUS_COLUMN_MAP,
    RELIUS_DEMO_COLUMN_MAP,
    RELIUS_ROTH_BASIS_COLUMN_MAP,
    SAMPLE_DIR,
)


DEFAULT_SEED = 20250214


def _amount(rng: random.Random, low: float, high: float) -> float:
    value = rng.uniform(low, high)
    return round(value, 2)


def _transaction_id(rng: random.Random) -> int:
    base = rng.randint(1_000_000, 9_999_999)
    return int(f"{base}0")


def _build_base_transactions(rng: random.Random) -> list[dict[str, object]]:
    inherited_plan_ids = sorted(INHERITED_PLAN_IDS)
    inherited_primary = inherited_plan_ids[-1] if inherited_plan_ids else "300004PLAT"
    inherited_secondary = inherited_plan_ids[0] if inherited_plan_ids else "300004MBD"

    base = [
        {
            "plan_id": inherited_primary,
            "ssn": "111223333",
            "first_name": "Ava",
            "last_name": "Nguyen",
            "state": "CA",
            "gross_amt": _amount(rng, 12000, 18000),
            "exported_date": "2024-01-15",
            "txn_date": "2024-01-17",
            "tax_year": 2024,
            "dist_name": "Rollover",
            "dist_code_1": "7",
            "tax_code_1": "7",
            "tax_code_2": "G",
            "tax_form": "1099-R",
            "dist_type": "Rollover",
            "txn_method": "ACH",
            "roth_initial_contribution_year": None,
        },
        {
            "plan_id": "400001ABC",
            "ssn": "222334444",
            "first_name": "Liam",
            "last_name": "Patel",
            "state": "TX",
            "gross_amt": _amount(rng, 6000, 9000),
            "exported_date": "2024-02-01",
            "txn_date": "2024-02-05",
            "tax_year": 2024,
            "dist_name": "Cash Distribution",
            "dist_code_1": "1",
            "tax_code_1": "1",
            "tax_code_2": None,
            "tax_form": "1099-R",
            "dist_type": "Cash",
            "txn_method": "Wire",
            "roth_initial_contribution_year": None,
        },
        {
            "plan_id": "300005R",
            "ssn": "333445555",
            "first_name": "Mia",
            "last_name": "Chen",
            "state": "WA",
            "gross_amt": _amount(rng, 10000, 14000),
            "exported_date": "2024-03-05",
            "txn_date": "2024-03-07",
            "tax_year": 2024,
            "dist_name": "Roth Distribution",
            "dist_code_1": "B",
            "tax_code_1": "B",
            "tax_code_2": "G",
            "tax_form": "1099-R",
            "dist_type": "Roth",
            "txn_method": "ACH",
            "roth_initial_contribution_year": 2016,
        },
        {
            "plan_id": inherited_secondary,
            "ssn": "444556666",
            "first_name": "Noah",
            "last_name": "Garcia",
            "state": "FL",
            "gross_amt": _amount(rng, 4000, 7000),
            "exported_date": "2024-04-10",
            "txn_date": "2024-04-12",
            "tax_year": None,
            "dist_name": "Rollover",
            "dist_code_1": "7",
            "tax_code_1": "7",
            "tax_code_2": None,
            "tax_form": "1099-R",
            "dist_type": "Rollover",
            "txn_method": "ACH",
            "roth_initial_contribution_year": None,
        },
    ]

    for row in base:
        row["transaction_id"] = _transaction_id(rng)

    return base


def _build_relius_transactions(base: list[dict[str, object]], rng: random.Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for row in base:
        rows.append(
            {
                "PLANID_1": row["plan_id"],
                "SSNUM_1": row["ssn"],
                "FIRSTNAM": row["first_name"],
                "LASTNAM": row["last_name"],
                "STATEADDR": row["state"],
                "GROSSDISTRAMT": row["gross_amt"],
                "EXPORTEDDATE": row["exported_date"],
                "DISTR1CD": row["dist_code_1"],
                "TAXYR": row["tax_year"],
                "DISTRNAM": row["dist_name"],
            }
        )

    rows.extend(
        [
            {
                "PLANID_1": "400001ABC",
                "SSNUM_1": "222334444",
                "FIRSTNAM": "Liam",
                "LASTNAM": "Patel",
                "STATEADDR": "TX",
                "GROSSDISTRAMT": _amount(rng, 800, 1400),
                "EXPORTEDDATE": "2099-01-01",
                "DISTR1CD": "7",
                "TAXYR": 2024,
                "DISTRNAM": "Cash Distribution",
            },
            {
                "PLANID_1": base[0]["plan_id"],
                "SSNUM_1": "111223333",
                "FIRSTNAM": "Ava",
                "LASTNAM": "Nguyen",
                "STATEADDR": "CA",
                "GROSSDISTRAMT": -250.00,
                "EXPORTEDDATE": "2024-05-05",
                "DISTR1CD": "ZZ",
                "TAXYR": 2024,
                "DISTRNAM": "Rollover",
            },
        ]
    )

    return pd.DataFrame(rows, columns=list(RELIUS_COLUMN_MAP.keys()))


def _build_matrix_export(base: list[dict[str, object]], rng: random.Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    matrix_accounts = ["07C00442", "07D00442", "07E00442", "07F00442"]

    for idx, row in enumerate(base):
        rows.append(
            {
                "Matrix Account": matrix_accounts[idx % len(matrix_accounts)],
                "Client Account": row["plan_id"],
                "Participant SSN": row["ssn"],
                "Participant Name": f"{row['first_name']} {row['last_name']}",
                "Participant State": row["state"],
                "Gross Amount": row["gross_amt"],
                "Transaction Date": row["txn_date"],
                "Transaction Type": row["txn_method"],
                "Tax Code": row["tax_code_1"],
                "Tax Code 2": row["tax_code_2"],
                "Tax Form": row["tax_form"],
                "Distribution Type": row["dist_type"],
                "Transaction Id": row["transaction_id"],
                "Fed Taxable Amount": row["gross_amt"],
                "Roth Initial Contribution Year": row["roth_initial_contribution_year"],
            }
        )

    duplicate_txn = base[2].copy()
    rows.append(
        {
            "Matrix Account": "07G00442",
            "Client Account": duplicate_txn["plan_id"],
            "Participant SSN": duplicate_txn["ssn"],
            "Participant Name": f"{duplicate_txn['first_name']} {duplicate_txn['last_name']}",
            "Participant State": duplicate_txn["state"],
            "Gross Amount": _amount(rng, 3000, 6000),
            "Transaction Date": "2024-03-08",
            "Transaction Type": "ACH",
            "Tax Code": "B",
            "Tax Code 2": None,
            "Tax Form": "1099-R",
            "Distribution Type": "Roth",
            "Transaction Id": duplicate_txn["transaction_id"],
            "Fed Taxable Amount": 0.00,
            "Roth Initial Contribution Year": None,
        }
    )

    rows.extend(
        [
            {
                "Matrix Account": "07H00442",
                "Client Account": "400001ABC",
                "Participant SSN": "222334444",
                "Participant Name": "Liam Patel",
                "Participant State": "TX",
                "Gross Amount": _amount(rng, 7000, 9000),
                "Transaction Date": "2099-01-01",
                "Transaction Type": "ACH",
                "Tax Code": "7",
                "Tax Code 2": None,
                "Tax Form": "1099-R",
                "Distribution Type": "Cash",
                "Transaction Id": _transaction_id(rng),
                "Fed Taxable Amount": _amount(rng, 7000, 9000),
                "Roth Initial Contribution Year": None,
            },
            {
                "Matrix Account": "07J00442",
                "Client Account": "400001ABC",
                "Participant SSN": "222334444",
                "Participant Name": "Liam Patel",
                "Participant State": "TX",
                "Gross Amount": 3000.00,
                "Transaction Date": "2024-02-20",
                "Transaction Type": "Wire",
                "Tax Code": "ZZ",
                "Tax Code 2": None,
                "Tax Form": "1099-R",
                "Distribution Type": "Cash",
                "Transaction Id": _transaction_id(rng),
                "Fed Taxable Amount": 9000.00,
                "Roth Initial Contribution Year": None,
            },
            {
                "Matrix Account": "07B00442",
                "Client Account": base[0]["plan_id"],
                "Participant SSN": "123456789",
                "Participant Name": "Edge Case",
                "Participant State": "CA",
                "Gross Amount": _amount(rng, 2000, 4000),
                "Transaction Date": "2024-01-10",
                "Transaction Type": "Account Transfer",
                "Tax Code": "7",
                "Tax Code 2": "G",
                "Tax Form": "1099-R",
                "Distribution Type": "Rollover",
                "Transaction Id": _transaction_id(rng),
                "Fed Taxable Amount": _amount(rng, 2000, 4000),
                "Roth Initial Contribution Year": None,
            },
        ]
    )

    return pd.DataFrame(rows, columns=list(MATRIX_COLUMN_MAP.keys()))


def _build_relius_demo(rng: random.Random) -> pd.DataFrame:
    rows = [
        {
            "PLANID": "300004PLAT",
            "SSNUM": "111223333",
            "FIRSTNAM": "Ava",
            "LASTNAM": "Nguyen",
            "BIRTHDATE": "1970-05-10",
            "TERM_DATE": "2020-12-31",
        },
        {
            "PLANID": "400001ABC",
            "SSNUM": "222334444",
            "FIRSTNAM": "Liam",
            "LASTNAM": "Patel",
            "BIRTHDATE": "1962-11-03",
            "TERM_DATE": None,
        },
        {
            "PLANID": "300005R",
            "SSNUM": "333445555",
            "FIRSTNAM": "Mia",
            "LASTNAM": "Chen",
            "BIRTHDATE": "1985-07-19",
            "TERM_DATE": "2023-06-30",
        },
        {
            "PLANID": "300004MBD",
            "SSNUM": "444556666",
            "FIRSTNAM": "Noah",
            "LASTNAM": "Garcia",
            "BIRTHDATE": None,
            "TERM_DATE": "2021-05-01",
        },
        {
            "PLANID": "300005R",
            "SSNUM": "555667777",
            "FIRSTNAM": "Zoe",
            "LASTNAM": "Lopez",
            "BIRTHDATE": "not-a-date",
            "TERM_DATE": "2022-01-15",
        },
    ]

    return pd.DataFrame(rows, columns=list(RELIUS_DEMO_COLUMN_MAP.keys()))


def _build_relius_roth_basis(rng: random.Random) -> pd.DataFrame:
    rows = [
        {
            "PLANID": "300005R",
            "SSNUM": "333445555",
            "FIRSTNAM": "Mia",
            "LASTNAM": "Chen",
            "FIRSTTAXYEARROTH": 2016,
            "Total": _amount(rng, 10000, 14000),
        },
        {
            "PLANID": "300005R",
            "SSNUM": "555667777",
            "FIRSTNAM": "Zoe",
            "LASTNAM": "Lopez",
            "FIRSTTAXYEARROTH": None,
            "Total": _amount(rng, 5000, 8000),
        },
        {
            "PLANID": "300005R",
            "SSNUM": "666778888",
            "FIRSTNAM": "Evan",
            "LASTNAM": "Stone",
            "FIRSTTAXYEARROTH": 1800,
            "Total": -100.00,
        },
    ]

    return pd.DataFrame(rows, columns=list(RELIUS_ROTH_BASIS_COLUMN_MAP.keys()))


def generate_sample_data(output_dir: Path = SAMPLE_DIR, seed: int = DEFAULT_SEED) -> dict[str, Path]:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_transactions = _build_base_transactions(rng)
    relius_df = _build_relius_transactions(base_transactions, rng)
    matrix_df = _build_matrix_export(base_transactions, rng)
    relius_demo_df = _build_relius_demo(rng)
    relius_roth_basis_df = _build_relius_roth_basis(rng)

    outputs = {
        "relius": output_dir / "relius_sample.xlsx",
        "matrix": output_dir / "matrix_sample.xlsx",
        "relius_demo": output_dir / "relius_demo_sample.xlsx",
        "relius_roth_basis": output_dir / "relius_roth_basis_sample.xlsx",
    }

    relius_df.to_excel(outputs["relius"], index=False)
    matrix_df.to_excel(outputs["matrix"], index=False)
    relius_demo_df.to_excel(outputs["relius_demo"], index=False)
    relius_roth_basis_df.to_excel(outputs["relius_roth_basis"], index=False)

    return outputs


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate seeded synthetic sample data for Matrix/Relius inputs."
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic RNG seed")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=SAMPLE_DIR,
        help="Destination directory for sample Excel files",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    outputs = generate_sample_data(output_dir=args.output_dir, seed=args.seed)
    for label, path in outputs.items():
        print(f"Wrote {label} sample to: {path}")


if __name__ == "__main__":
    main()
