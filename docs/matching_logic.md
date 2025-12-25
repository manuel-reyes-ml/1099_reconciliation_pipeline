# 🧩 Matching Logic — 1099 Reconciliation Pipeline

This document describes the **end-to-end matching and correction logic** used to reconcile retirement plan distribution activity between **Relius** and **Matrix**, and to generate **Matrix-ready 1099-R correction files**.

It covers:

- **🔁 Engine A (Reconciliation):** Relius ↔ Matrix matching for inherited-plan workflows
- **🎂 Engine B (Age-based):** Matrix tax-code analysis using Relius demographics (DOB / term date)
- **🧹 Cleaning assumptions:** canonical schema produced by `clean_relius.py` and `clean_matrix.py`
- **📤 Correction outputs:** how `build_correction_file.py` consumes engine results

> **Note:** Field names in synthetic sample files may differ slightly (snake_case).
> This document describes the **canonical** fields produced by the pipeline.

---

## 📌 Quick Reference

**Jump to:**
- [0. Data Flow Overview](#0-data-flow-overview) — How files move through the pipeline
- [1. Canonical Fields Used](#1-canonical-fields-used) — Minimum fields required per engine
- [2. Cleaning & Normalization Rules](#2-cleaning--normalization-rules) — SSN / dates / tax codes
- [3. Engine A — Relius ↔ Matrix Reconciliation](#3-engine-a--relius--matrix-reconciliation) — Match keys, date window, inherited rules
- [4. Engine B — Age-Based Tax Code Engine](#4-engine-b--age-based-tax-code-engine) — DOB/term-based logic + Roth handling
- [5. Match Status Taxonomy](#5-match-status-taxonomy) — Definitions used across engines
- [6. Correction File Contract](#6-correction-file-contract) — Required columns to write Matrix template
- [7. Validation & QA Checklist](#7-validation--qa-checklist) — Recommended checks before delivery
- [8. Edge Cases & Failure Modes](#8-edge-cases--failure-modes) — Duplicates, missing DOB, multi-digit codes
- [9. Privacy Notes](#9-privacy-notes) — Synthetic data policy

---

## 🎯 Most Critical Rules (Top 6)

For quick orientation, these are the most impactful rules:

1. **🔑 Matching keys (Engine A):** `plan_id + ssn + gross_amt` form the candidate match set.
2. **📅 Asymmetric date window (Engine A):** `txn_date` must be **on/after** `exported_date` and **≤ exported_date + MAX_DELAY_DAYS`.
3. **🧾 Inherited plans (Engine A):** inherited coding rules override “normal” codes (typically **4** and/or **G** with **4** depending on distribution type).
4. **🎂 Age rule (Engine B):** age at distribution **≥ 59.5 → code 7** (non-Roth).
5. **👔 Termination rule (Engine B):** if <59.5 and term date exists, **55+ at term → code 2**, otherwise **code 1**.
6. **🅱️ Roth rule (Engine B):** Roth plans require **Tax Code 1 = B** and **Tax Code 2 = (1/2/7)** based on age logic.

---

## 🟢🟡🔴 Color coding in this document

- 🔴 **Critical** — can cause incorrect 1099-R output or missed corrections
- 🟡 **Important** — affects match rate / reduces noise / improves accuracy
- 🟢 **Reference** — supporting detail for implementation or review

---

## 0. Data Flow Overview

### 0.1 High-level pipeline

┌───────────────────────────┐ ┌───────────────────────────┐
│ Relius Export (.xlsx) │ │ Matrix Export (.xlsx) │
│ (distributions / trans) │ │ (disbursements / 1099) │
└───────────────┬───────────┘ └───────────────┬───────────┘
│ │
▼ ▼
clean_relius.py clean_matrix.py
│ │
└────────────── Engine A (Reconcile) ───────┘
│
▼
match_transactions.py
(reconcile_relius_matrix)
│
▼
build_correction_file.py
(Matrix correction template output)

┌──────────────────────────────────────┐
│ Relius Participant Master (.xlsx) │
│ (DOB / term date by plan + SSN) │
└──────────────────┬───────────────────┘
▼
clean_relius_demo.py
│
▼
age_taxcode_analysis.py
(Engine B: Age rules)
│
▼
build_correction_file.py
(Matrix correction template output)

### 0.2 Why two engines?

- **Engine A** solves: “Do these two systems agree on the same transaction?”
- **Engine B** solves: “Given DOB/term, does Matrix have the correct tax coding?”

They are intentionally independent so you can run:
- Inherited-only reconciliation (Engine A), or
- Age-based code auditing (Engine B), or
- Both, generating separate correction outputs.

---

## 1. Canonical Fields Used

### 1.1 Engine A — minimum required fields

**Relius canonical fields**
- `plan_id`
- `ssn`
- `gross_amt`
- `exported_date`
- `trans_id_relius` (recommended)
- `dist_name` / `dist_category_relius` (recommended for rollover/cash logic)

**Matrix canonical fields**
- `plan_id`
- `ssn`
- `gross_amt`
- `txn_date`
- `transaction_id` (recommended)
- `tax_code_1` / `tax_code_2` (required to compare + correct)
- `matrix_account` and `participant_name` (required for correction output template)

### 1.2 Engine B — minimum required fields

**Matrix canonical fields**
- `plan_id`
- `ssn`
- `txn_date`
- `transaction_id`
- `tax_code_1` / `tax_code_2`
- `matrix_account`, `participant_name`

**Relius demographics canonical fields**
- `plan_id`
- `ssn`
- `dob`
- `term_date` (optional but improves accuracy)
- `first_name`, `last_name` (optional)

---

## 2. Cleaning & Normalization Rules

### 2.1 SSN normalization (🔴 Critical)

All modules normalize to a 9-digit string:
- Strip non-digits
- Handle Excel numeric artifacts
- Truncate to 9 if longer
- `zfill(9)` only when fewer than 9 digits
- Invalid or missing → `NA`

**Rule intent:** SSN must be stable across systems to prevent false mismatches.

### 2.2 Date normalization (🔴 Critical)
- Matrix `txn_date` becomes `date`
- Relius `exported_date` becomes `date`
- Relius demographics `dob` / `term_date` become `date`

### 2.3 Amount normalization (🟡 Important)
- `gross_amt` is coerced to numeric
- Downstream matching assumes the **cleaned** value is comparable across systems

> **Implementation note:** If you later add rounding tolerances, apply them in the engine (not in cleaning).

### 2.4 Tax code normalization (🔴 Critical)
Tax codes may appear as:
- `7 - Normal Distribution`
- `G - Rollover`
- `11 - ...`

Canonical normalization extracts **1–2 leading characters**:
- `7`, `G`, `H`, `11` supported
- stored in `tax_code_1` and `tax_code_2`

---

## 3. Engine A — Relius ↔ Matrix Reconciliation

### 3.1 Purpose
Engine A reconciles transaction records between systems to detect:
- **Matches** (within constraints)
- **Timing outliers** (date outside expected window)
- **Unmatched** items
- **Match needs correction** based on inherited-plan coding rules

### 3.2 Candidate matching keys (🔴 Critical)

Candidate matches are generated using:

- `plan_id`
- `ssn`
- `gross_amt`

These are defined in `config.MATCH_KEYS`.

**Why date is not a strict join key:** operational timing can shift; date is applied as a tolerance constraint to classify candidate matches.

### 3.3 Asymmetric date tolerance window (🔴 Critical)

Operational assumption:
- Relius export happens first, then Matrix executes the disbursement later.

Rule (asymmetric):
- `txn_date >= exported_date`
- `txn_date <= exported_date + MAX_DELAY_DAYS`

Where:
- `MAX_DELAY_DAYS` is defined in `config.py` (commonly 10 days)

**Classification:**
- in-window → eligible for “match/perfect/correction”
- out-of-window → `date_out_range`

### 3.4 Duplicate handling and one-to-many behavior (🟡 Important)

When multiple Matrix rows share the same match keys (same plan/SSN/amount), the engine can produce repeated candidate pairs.

Recommended safeguard (implementation-dependent):
- Prefer uniqueness by including stable identifiers for review:
  - Matrix `transaction_id`
  - Relius `trans_id_relius`
- Apply deterministic selection if needed:
  - “closest txn_date within window”
  - “first match by transaction_id”
  - or require unique keys before joining

> **QA check:** Validate that no Matrix `transaction_id` appears more than once in the **final correction output**.

### 3.5 Inherited-plan correction rules (🔴 Critical)

Inherited plan IDs are defined in `config.INHERITED_PLAN_IDS`.

For inherited plans, expected codes are driven by distribution type:
- Cash distributions (Relius dist category indicates cash/RMD/ACH) → expected **code 4**
- Rollover distributions (Relius indicates rollover) → expected **two-code pattern** (e.g., `G` + `4` or `4` + `G`, per your configured convention)

Engine A compares Matrix current codes vs expected:
- If aligned → `perfect_match`
- If not aligned → `match_needs_correction` + populate:
  - `suggested_tax_code_1`
  - `suggested_tax_code_2` (if applicable)
  - `action = UPDATE_1099`
  - `correction_reason`

---

## 4. Engine B — Age-Based Tax Code Engine

### 4.1 Purpose
Engine B generates corrections **without matching by amount**. It answers:

> “Based on DOB and termination data, should this Matrix transaction’s tax coding be updated?”

Join keys:
- `plan_id + ssn` to attach demographics to each Matrix row.

### 4.2 Exclusions (🔴 Critical)

Engine B must **not** override tax codes driven by distribution type or inherited logic.

Excluded from Engine B processing:
- `tax_code_1` in `{G, H}` (rollovers; driven by distribution type)
- `plan_id` in `INHERITED_PLAN_IDS` (handled by Engine A)

Excluded rows are labeled:
- `match_status = excluded_from_age_engine_rollover_or_inherited`

### 4.3 Age computation (🔴 Critical)

Engine B computes:
- `age_at_distribution` from `dob` and Matrix `txn_date`
- `age_at_termination` from `dob` and `term_date` (if available)

### 4.4 Non-Roth rules (Tax Code 1 = 7/2/1)

For non-Roth plans:

1) If `age_at_distribution >= 59.5` → **Tax Code 1 = 7**

2) If `< 59.5`:
- If `term_date` exists:
  - if `age_at_termination >= 55` → **Tax Code 1 = 2**
  - else → **Tax Code 1 = 1**
- If `term_date` missing (fallback for 2025):
  - if `age_at_distribution >= 55` → **Tax Code 1 = 2**
  - else → **Tax Code 1 = 1**

### 4.5 Roth plan detection (🟡 Important)

Roth plans are identified by:
- `plan_id` starts with `300005` **OR**
- `plan_id` ends with `R`

This logic is implemented in `_is_roth_plan_id()`.

### 4.6 Roth rules (Tax Code 1 = B, Tax Code 2 = 7/2/1) (🔴 Critical)

For Roth plans:
- **Tax Code 1 must be `B`**
- **Tax Code 2** is derived using the same age logic:

Examples:
- Roth age 62 → `B / 7`
- Roth age 57, term age 56 → `B / 2`
- Roth age 52, term age 49 → `B / 1`

### 4.7 Age engine comparison behavior (🔴 Critical)

The engine compares expected vs current codes:

- **Non-Roth:** compare `tax_code_1` only
- **Roth:** compare both `tax_code_1` and `tax_code_2`

Output:
- `perfect_match` when codes already match expected
- `match_needs_correction` when codes differ
  - `suggested_tax_code_1` and `suggested_tax_code_2` populated
  - `action = UPDATE_1099`
  - `correction_reason` populated

---

## 5. Match Status Taxonomy

The following status values appear across engine outputs:

| Status | Meaning | Typical Next Step |
|---|---|---|
| `perfect_match` | Codes already correct (given the engine’s rules) | No action |
| `match_needs_correction` | Eligible match/row with incorrect coding | Export to correction file |
| `date_out_range` | Candidate match but txn_date outside allowed delay window | Investigate timing / wrong pairing |
| `unmatched_relius` | Relius row has no Matrix candidate | Investigate missing disbursement |
| `excluded_from_age_engine_rollover_or_inherited` | Age engine intentionally skipped row | Handled elsewhere / ignore |
| `age_rule_insufficient_data` | Missing DOB (or needed fields) | Review demographics data |

---

## 6. Correction File Contract

`build_correction_file.py` expects engines to provide:

### 6.1 Required fields (minimum)
- `match_status` (must be `match_needs_correction` to export)
- `suggested_tax_code_1` (required)
- `transaction_id` (Matrix)
- `txn_date` (Matrix)
- `ssn`
- `participant_name`
- `matrix_account`

### 6.2 Optional but recommended
- `suggested_tax_code_2` (Roth or two-code cases)
- `tax_code_1`, `tax_code_2` (current)
- `plan_id`
- `action`, `correction_reason`

---

## 7. Validation & QA Checklist

Before delivering a correction file:

### 7.1 Engine A validation
- ✅ Confirm `MAX_DELAY_DAYS` reflects operational reality (e.g., 10)
- ✅ Spot-check a sample of `date_out_range` rows
- ✅ Verify no duplicate Matrix `transaction_id` in final correction output
- ✅ Verify inherited plans only apply inherited tax code rules

### 7.2 Engine B validation
- ✅ Confirm exclusions: no `G` / `H` tax_code_1 rows are corrected
- ✅ Confirm inherited plans excluded from age output
- ✅ Confirm Roth output uses `B` in code1 and age code in code2
- ✅ Confirm DOB join uses `(plan_id, ssn)` correctly

---

## 8. Edge Cases & Failure Modes

- **Duplicate candidate matches (🟡):** same plan/SSN/amount repeats across weeks → can create repeated pairing candidates.
- **Missing DOB/term (🟡):** age engine falls back or marks insufficient data.
- **Multi-digit tax codes (🔴):** ensure normalization preserves `11`, not just first character.
- **SSN formatting artifacts (🔴):** Excel numeric formatting may add decimals or truncate; always treat as string + digit clean.
- **Plan naming variants (🟡):** Roth detection uses pattern rules; adjust if additional variants appear.

---

## 9. Privacy Notes

This repository should contain **synthetic or masked** data only.

Never commit:
- real SSNs
- DOBs
- termination dates
- internal plan exports
- participant names/addresses from production

Production runs should occur only in secure, access-controlled environments.
