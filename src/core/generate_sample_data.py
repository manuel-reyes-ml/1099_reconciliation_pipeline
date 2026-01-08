"""
generate_sample_data.py

Seeded generator for synthetic Matrix and Relius sample inputs.

This script writes four Excel files into data/sample/, using raw headers that
match the column maps in src/core/config.py so load_data.py validation passes.
The outputs are deterministic given a seed and include both valid and edge-case
rows to exercise normalization and validation logic.
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from .config import (
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


def _build_base_transactions(rng: random.Random, faker: Faker) -> list[dict[str, object]]:
    inherited_plan_ids = sorted(INHERITED_PLAN_IDS)
    inherited_primary = inherited_plan_ids[-1] if inherited_plan_ids else "300004PLAT"
    inherited_secondary = inherited_plan_ids[0] if inherited_plan_ids else "300004MBD"

    plan_ids = sorted({*INHERITED_PLAN_IDS, "400001ABC", "300005R"})
    roth_plan_ids = ["300005R"]
    non_roth_plan_ids = [plan_id for plan_id in plan_ids if plan_id not in roth_plan_ids]

    distribution_options = [
        {
            "dist_name": "Rollover",
            "dist_code_1": "7",
            "tax_code_1": "7",
            "tax_code_2": "G",
            "dist_type": "Rollover",
            "gross_low": 12000,
            "gross_high": 18000,
        },
        {
            "dist_name": "Cash Distribution",
            "dist_code_1": "1",
            "tax_code_1": "1",
            "tax_code_2": None,
            "dist_type": "Cash",
            "gross_low": 4000,
            "gross_high": 9000,
        },
        {
            "dist_name": "Roth Distribution",
            "dist_code_1": "B",
            "tax_code_1": "B",
            "tax_code_2": "G",
            "dist_type": "Roth",
            "gross_low": 9000,
            "gross_high": 15000,
        },
    ]

    txn_methods = ["ACH", "Wire", "Check", "Account Transfer"]
    reserved_ssns = {"111223333", "222334444", "333445555", "444556666", "555667777", "666778888"}
    used_ssns = set(reserved_ssns)

    base: list[dict[str, object]] = []
    for _ in range(100):
        dist = rng.choice(distribution_options)
        if dist["dist_type"] == "Roth":
            plan_id = rng.choice(roth_plan_ids)
            roth_year = rng.randint(2005, 2020)
        else:
            plan_id = rng.choice(non_roth_plan_ids)
            roth_year = None

        ssn = f"{rng.randint(100000000, 999999999)}"
        while ssn in used_ssns:
            ssn = f"{rng.randint(100000000, 999999999)}"
        used_ssns.add(ssn)

        exported_date = faker.date_between_dates(
            date_start=date(2024, 1, 1),
            date_end=date(2024, 12, 15),
        )
        txn_date = exported_date + timedelta(days=rng.randint(0, 10))

        base.append(
            {
                "plan_id": plan_id,
                "ssn": ssn,
                "first_name": faker.first_name(),
                "last_name": faker.last_name(),
                "state": faker.state_abbr(),
                "gross_amt": _amount(rng, dist["gross_low"], dist["gross_high"]),
                "exported_date": exported_date.isoformat(),
                "txn_date": txn_date.isoformat(),
                "tax_year": exported_date.year,
                "dist_name": dist["dist_name"],
                "dist_code_1": dist["dist_code_1"],
                "tax_code_1": dist["tax_code_1"],
                "tax_code_2": dist["tax_code_2"],
                "tax_form": "1099-R",
                "dist_type": dist["dist_type"],
                "txn_method": rng.choice(txn_methods),
                "roth_initial_contribution_year": roth_year,
            }
        )

    base.extend(
        [
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
            "plan_id": "300005R",
            "ssn": "555667777",
            "first_name": "Zoe",
            "last_name": "Lopez",
            "state": "OR",
            "gross_amt": _amount(rng, 9000, 13000),
            "exported_date": "2024-05-10",
            "txn_date": "2024-05-12",
            "tax_year": 2024,
            "dist_name": "Roth Distribution",
            "dist_code_1": "B",
            "tax_code_1": "B",
            "tax_code_2": "G",
            "tax_form": "1099-R",
            "dist_type": "Roth",
            "txn_method": "Wire",
            "roth_initial_contribution_year": None,
        },
        {
            "plan_id": "300005R",
            "ssn": "666778888",
            "first_name": "Evan",
            "last_name": "Stone",
            "state": "CO",
            "gross_amt": _amount(rng, 7000, 11000),
            "exported_date": "2024-06-15",
            "txn_date": "2024-06-17",
            "tax_year": 2024,
            "dist_name": "Roth Distribution",
            "dist_code_1": "B",
            "tax_code_1": "B",
            "tax_code_2": "G",
            "tax_form": "1099-R",
            "dist_type": "Roth",
            "txn_method": "Check",
            "roth_initial_contribution_year": 2012,
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
    )

    for row in base:
        row["transaction_id"] = _transaction_id(rng)

    return base


def _build_relius_transactions(
    base: list[dict[str, object]],
    rng: random.Random,
    faker: Faker,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    ava_row = next((row for row in base if row["ssn"] == "111223333"), base[0])

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
                "PLANID_1": ava_row["plan_id"],
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


def _build_matrix_export(
    base: list[dict[str, object]],
    rng: random.Random,
    faker: Faker,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    matrix_accounts = ["07C00442", "07D00442", "07E00442", "07F00442"]
    ava_row = next((row for row in base if row["ssn"] == "111223333"), base[0])

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

    duplicate_txn = next((row for row in base if row["ssn"] == "333445555"), base[2]).copy()
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
                "Client Account": ava_row["plan_id"],
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


def _build_relius_demo(
    base: list[dict[str, object]],
    rng: random.Random,
    faker: Faker,
) -> pd.DataFrame:
    edge_by_ssn = {
        "111223333": {
            "FIRSTNAM": "Ava",
            "LASTNAM": "Nguyen",
            "BIRTHDATE": "1970-05-10",
            "TERM_DATE": "2020-12-31",
        },
        "222334444": {
            "FIRSTNAM": "Liam",
            "LASTNAM": "Patel",
            "BIRTHDATE": "1962-11-03",
            "TERM_DATE": None,
        },
        "333445555": {
            "FIRSTNAM": "Mia",
            "LASTNAM": "Chen",
            "BIRTHDATE": "1985-07-19",
            "TERM_DATE": "2023-06-30",
        },
        "444556666": {
            "FIRSTNAM": "Noah",
            "LASTNAM": "Garcia",
            "BIRTHDATE": None,
            "TERM_DATE": "2021-05-01",
        },
    }

    rows: list[dict[str, object]] = []
    index_by_key: dict[tuple[str, str], int] = {}

    for row in base:
        key = (row["plan_id"], row["ssn"])
        if key in index_by_key:
            continue

        edge = edge_by_ssn.get(row["ssn"])
        if edge:
            demo_row = {
                "PLANID": row["plan_id"],
                "SSNUM": row["ssn"],
                **edge,
            }
        else:
            birth_date = faker.date_between_dates(
                date_start=date(1940, 1, 1),
                date_end=date(2005, 12, 31),
            )
            term_date = None
            if rng.random() < 0.75:
                term_date = faker.date_between_dates(
                    date_start=date(2010, 1, 1),
                    date_end=date(2024, 12, 31),
                )

            demo_row = {
                "PLANID": row["plan_id"],
                "SSNUM": row["ssn"],
                "FIRSTNAM": row["first_name"],
                "LASTNAM": row["last_name"],
                "BIRTHDATE": birth_date.isoformat(),
                "TERM_DATE": term_date.isoformat() if term_date else None,
            }

        index_by_key[key] = len(rows)
        rows.append(demo_row)

    invalid_dob_row = {
        "PLANID": "300005R",
        "SSNUM": "555667777",
        "FIRSTNAM": "Zoe",
        "LASTNAM": "Lopez",
        "BIRTHDATE": "not-a-date",
        "TERM_DATE": "2022-01-15",
    }
    invalid_key = (invalid_dob_row["PLANID"], invalid_dob_row["SSNUM"])
    existing_idx = index_by_key.get(invalid_key)
    if existing_idx is None:
        rows.append(invalid_dob_row)
    else:
        rows[existing_idx] = invalid_dob_row

    return pd.DataFrame(rows, columns=list(RELIUS_DEMO_COLUMN_MAP.keys()))


def _build_relius_roth_basis(
    base: list[dict[str, object]],
    rng: random.Random,
    faker: Faker,
) -> pd.DataFrame:
    edge_by_ssn = {
        "333445555": {
            "FIRSTNAM": "Mia",
            "LASTNAM": "Chen",
            "FIRSTTAXYEARROTH": 2016,
            "Total": _amount(rng, 10000, 14000),
        },
        "555667777": {
            "FIRSTNAM": "Zoe",
            "LASTNAM": "Lopez",
            "FIRSTTAXYEARROTH": None,
            "Total": _amount(rng, 5000, 8000),
        },
        "666778888": {
            "FIRSTNAM": "Evan",
            "LASTNAM": "Stone",
            "FIRSTTAXYEARROTH": 1800,
            "Total": -100.00,
        },
    }

    rows: list[dict[str, object]] = []
    index_by_key: dict[tuple[str, str], int] = {}

    for row in base:
        if row["dist_type"] != "Roth":
            continue

        key = (row["plan_id"], row["ssn"])
        if key in index_by_key:
            continue

        edge = edge_by_ssn.get(row["ssn"])
        if edge:
            basis_row = {
                "PLANID": row["plan_id"],
                "SSNUM": row["ssn"],
                **edge,
            }
        else:
            basis_row = {
                "PLANID": row["plan_id"],
                "SSNUM": row["ssn"],
                "FIRSTNAM": row["first_name"],
                "LASTNAM": row["last_name"],
                "FIRSTTAXYEARROTH": row["roth_initial_contribution_year"],
                "Total": _amount(rng, 3000, 18000),
            }

        index_by_key[key] = len(rows)
        rows.append(basis_row)

    extra_rows = [
        {
            "PLANID": "300005R",
            "SSNUM": ssn,
            **edge_by_ssn[ssn],
        }
        for ssn in ("555667777", "666778888")
    ]

    for row in extra_rows:
        key = (row["PLANID"], row["SSNUM"])
        existing_idx = index_by_key.get(key)
        if existing_idx is None:
            rows.append(row)
        else:
            rows[existing_idx] = row

    return pd.DataFrame(rows, columns=list(RELIUS_ROTH_BASIS_COLUMN_MAP.keys()))


def _join_coverage_ratio(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    key_cols: list[str],
) -> float:
    if left_df.empty:
        return 0.0

    right_keys = right_df[key_cols].drop_duplicates()
    merged = left_df.merge(right_keys, on=key_cols, how="left", indicator=True)
    return float((merged["_merge"] == "both").mean())


def _validate_sample_joins(
    matrix_df: pd.DataFrame,
    relius_demo_df: pd.DataFrame,
    relius_roth_basis_df: pd.DataFrame,
    min_ratio: float = 0.5,
) -> None:
    matrix_keys = matrix_df.rename(columns=MATRIX_COLUMN_MAP)[["plan_id", "ssn", "dist_type"]]
    demo_keys = relius_demo_df.rename(columns=RELIUS_DEMO_COLUMN_MAP)[["plan_id", "ssn"]]
    basis_keys = relius_roth_basis_df.rename(columns=RELIUS_ROTH_BASIS_COLUMN_MAP)[
        ["plan_id", "ssn"]
    ]

    required_matrix_edges = [
        {"plan_id": "300005R", "ssn": "555667777", "dist_type": "Roth"},
        {"plan_id": "300005R", "ssn": "666778888", "dist_type": "Roth"},
    ]
    missing_edges = []
    for edge in required_matrix_edges:
        mask = (
            (matrix_keys["plan_id"] == edge["plan_id"])
            & (matrix_keys["ssn"] == edge["ssn"])
            & (matrix_keys["dist_type"] == edge["dist_type"])
        )
        if not mask.any():
            missing_edges.append(f"{edge['plan_id']}:{edge['ssn']}")
    if missing_edges:
        missing_str = ", ".join(missing_edges)
        raise ValueError(f"Expected edge-case Matrix rows for {missing_str}.")

    demo_ratio = _join_coverage_ratio(matrix_keys[["plan_id", "ssn"]], demo_keys, ["plan_id", "ssn"])
    if demo_ratio <= min_ratio:
        raise ValueError(
            "Expected majority of Matrix rows to match Relius demo rows; "
            f"got {demo_ratio:.1%}."
        )

    roth_matrix = matrix_keys[matrix_keys["dist_type"] == "Roth"]
    if not roth_matrix.empty:
        basis_ratio = _join_coverage_ratio(
            roth_matrix[["plan_id", "ssn"]],
            basis_keys,
            ["plan_id", "ssn"],
        )
        if basis_ratio <= min_ratio:
            raise ValueError(
                "Expected majority of Roth Matrix rows to match Relius Roth basis rows; "
                f"got {basis_ratio:.1%}."
            )


def generate_sample_data(output_dir: Path = SAMPLE_DIR, seed: int = DEFAULT_SEED) -> dict[str, Path]:
    rng = random.Random(seed)
    faker = Faker()
    faker.seed_instance(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_transactions = _build_base_transactions(rng, faker)
    relius_df = _build_relius_transactions(base_transactions, rng, faker)
    matrix_df = _build_matrix_export(base_transactions, rng, faker)
    relius_demo_df = _build_relius_demo(base_transactions, rng, faker)
    relius_roth_basis_df = _build_relius_roth_basis(base_transactions, rng, faker)
    _validate_sample_joins(matrix_df, relius_demo_df, relius_roth_basis_df)

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
