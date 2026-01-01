# 🧩 Matching Logic — 1099 Reconciliation Pipeline

This document describes the **end-to-end matching and correction logic** used to reconcile retirement plan distribution activity between **Relius** and **Matrix**, and to generate **Matrix-ready 1099-R correction files**.

It covers:

- **🔁 Engine A (Reconciliation):** Relius ↔ Matrix matching for inherited-plan workflows
- **🎂 Engine B (Age-based):** Matrix tax-code analysis using Relius demographics (DOB / term date)
- **🧾 Engine C (Roth taxable):** Roth taxable amount and Roth tax-code logic
- **🧹 Cleaning assumptions:** canonical schema produced by `clean_relius.py` and `clean_matrix.py`
- **📤 Correction outputs:** how `build_correction_file.py` consumes engine results

> **Note:** Field names in synthetic sample files may differ slightly (snake_case).
> This document describes the **canonical** fields produced by the pipeline.
> To regenerate synthetic inputs from the repo root, run `python -m src.generate_sample_data` or `notebooks/07_generate_sample_data.ipynb`.

---

## 📌 Quick Reference

**Jump to:**
- [0. Data Flow Overview](#0-data-flow-overview) — How files move through the pipeline
- [1. Canonical Fields Used](#1-canonical-fields-used) — Minimum fields required per engine
- [2. Cleaning & Normalization Rules](#2-cleaning--normalization-rules) — SSN / dates / tax codes
- [3. Engine A — Relius ↔ Matrix Reconciliation](#3-engine-a--relius--matrix-reconciliation) — Match keys, date window, inherited rules
- [4. Engine B — Age-Based Tax Code Engine](#4-engine-b--age-based-tax-code-engine) — DOB/term-based logic (non-Roth)
- [5. Engine C — Roth Taxable Engine](#5-engine-c--roth-taxable-engine) — Roth taxable, basis, and tax-code rules
- [6. Match Status Taxonomy](#6-match-status-taxonomy) — Definitions used across engines
- [7. Correction File Contract](#7-correction-file-contract) — Required columns to write Matrix template
- [8. Validation & QA Checklist](#8-validation--qa-checklist) — Recommended checks before delivery
- [9. Edge Cases & Failure Modes](#9-edge-cases--failure-modes) — Duplicates, missing DOB, multi-digit codes
- [10. Privacy Notes](#10-privacy-notes) — Synthetic data policy

---

## 🎯 Most Critical Rules (Top 6)

For quick orientation, these are the most impactful rules:

1. **🔑 Matching keys (Engine A):** `plan_id + ssn + gross_amt` form the candidate match set.
2. **📅 Asymmetric date window (Engine A):** `txn_date` must be **on/after** `exported_date` and **≤ exported_date + MATCHING_CONFIG.max_date_lag_days`.
3. **🧾 Inherited plans (Engine A):** inherited coding rules override “normal” codes (typically **4** and/or **G** with **4** depending on distribution type).
4. **🎂 Age rule (Engine B):** age at distribution **≥ 59.5 → code 7** (non-Roth).
5. **👔 Termination rule (Engine B):** if <59.5 and term date exists, **55+ at term → code 2**, otherwise **code 1**.
6. **🅱️ Roth rule (Engine C):** Roth plans use taxable/basis logic and enforce Roth tax codes (B* for non-rollover, H for rollovers).

---

## 🟢🟡🔴 Color coding in this document

- 🔴 **Critical** — can cause incorrect 1099-R output or missed corrections
- 🟡 **Important** — affects match rate / reduces noise / improves accuracy
- 🟢 **Reference** — supporting detail for implementation or review

---

## 0. Data Flow Overview

### 0.1 High-level pipeline

┌───────────────────────────┐ ┌───────────────────────────┐
│ Relius Export (.xlsx)     │ │ Matrix Export (.xlsx)     │
│ (distributions / trans)   │ │ (disbursements / 1099)    │
└───────────────┬───────────┘ └───────────────┬───────────┘
                │                             │
                ▼                             ▼
          clean_relius.py               clean_matrix.py
                │                             │
                └──────────── Engine A (Reconcile) ───────┘
                                  │
                                  ▼
                        match_transactions.py
                        (reconcile_relius_matrix)
                                  │
                                  ▼
                         build_correction_file.py
                         (Matrix correction output)

┌──────────────────────────────────────┐
│ Relius Participant Master (.xlsx)    │
│ (DOB / term date by plan + SSN)      │
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

┌──────────────────────────────────────┐
│ Relius Roth Basis (.xlsx)            │
│ (first_roth_tax_year, roth_basis_amt)│
└──────────────────┬───────────────────┘
                   ▼
          clean_relius_roth_basis.py
                   │
                   ▼
        roth_taxable_analysis.py
        (Engine C: Roth taxable)
                   │
                   ▼
        build_correction_file.py

### 0.2 Why three engines?

- **Engine A** solves: “Do these two systems agree on the same transaction?”
- **Engine B** solves: “Given DOB/term, does Matrix have the correct non-Roth tax coding?”
- **Engine C** solves: “For Roth plans, are taxable amounts, start year, and tax codes correct?”

They are intentionally independent so you can run:
- Inherited-only reconciliation (Engine A), or
- Age-based code auditing (Engine B), or
- Roth taxable analysis (Engine C), or
- All three, generating separate correction outputs.

---

## 1. Canonical Fields Used

### 1.1 Engine A — minimum required fields

**Relius canonical fields**
- `plan_id`
- `ssn`
- `gross_amt`
- `exported_date`
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

### 1.3 Engine C — minimum required fields

**Matrix canonical fields**
- `plan_id`
- `ssn`
- `txn_date`
- `transaction_id`
- `gross_amt`
- `fed_taxable_amt`
- `tax_code_1` / `tax_code_2`
- `roth_initial_contribution_year`
- `matrix_account`, `participant_name`

**Relius demographics canonical fields**
- `plan_id`
- `ssn`
- `dob`
- `term_date` (optional)

**Relius Roth basis canonical fields**
- `plan_id`
- `ssn`
- `first_roth_tax_year`
- `roth_basis_amt`

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

### 2.5 Plan ID normalization (🟡 Important)
Plan IDs are stripped and normalized for consistent matching and Roth plan
identification (case-insensitive prefixes/suffixes).

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

These are the join keys inside `reconcile_relius_matrix`.

**Why date is not a strict join key:** operational timing can shift; date is applied as a tolerance constraint to classify candidate matches.

### 3.3 Asymmetric date tolerance window (🔴 Critical)

Operational assumption:
- Relius export happens first, then Matrix executes the disbursement later.

Rule (asymmetric):
- `txn_date >= exported_date`
- `txn_date <= exported_date + MATCHING_CONFIG.max_date_lag_days`

Where:
- `MATCHING_CONFIG.max_date_lag_days` is defined in `config.py`

**Classification:**
- in-window → eligible for “match/perfect/correction”
- out-of-window → `date_out_of_range`

### 3.4 Duplicate handling and one-to-many behavior (🟡 Important)

When multiple Matrix rows share the same match keys (same plan/SSN/amount), the engine can produce repeated candidate pairs.

Recommended safeguard (implementation-dependent):
- Prefer uniqueness by including stable identifiers for review:
  - Matrix `transaction_id`
- Apply deterministic selection if needed:
  - “closest txn_date within window”
  - “first match by transaction_id”
  - or require unique keys before joining

> **QA check:** Validate that no Matrix `transaction_id` appears more than once in the **final correction output**.

### 3.5 Inherited-plan correction rules (🔴 Critical)

Inherited plan IDs are defined in `config.INHERITED_PLAN_IDS`.

For inherited plans, expected codes are driven by distribution type:
- Cash-like distributions (Relius dist category is not rollover/partial_rollover) → **tax_code_1 = 4**, **tax_code_2 = (blank)**
- Rollover distributions (`dist_category_relius` in `rollover` or `partial_rollover`) → **tax_code_1 = 4**, **tax_code_2 = G**

`dist_category_relius` is derived from Relius `dist_name` in `clean_relius.py`.

Engine A compares Matrix current codes vs expected:
- If aligned → `match_no_action`
- If not aligned → `match_needs_correction` + populate:
  - `suggested_tax_code_1`
  - `suggested_tax_code_2` (if applicable)
  - `action = UPDATE_1099`
  - `correction_reason`

---

## 4. Engine B — Age-Based Tax Code Engine

### 4.1 Purpose
Engine B generates **non-Roth** tax-code corrections **without matching by amount**. It answers:

> “Based on DOB and termination data, should this Matrix transaction’s tax coding be updated?”

Join keys:
- `plan_id + ssn` to attach demographics to each Matrix row.

### 4.2 Exclusions (🔴 Critical)

Engine B must **not** override tax codes driven by distribution type or inherited logic.

Excluded from Engine B processing:
- Roth plans (handled by Engine C)
- `tax_code_1` in `AGE_TAXCODE_CONFIG.excluded_codes` (rollovers and other excluded codes)
- `plan_id` in `INHERITED_PLAN_IDS` (handled by Engine A)

Excluded rows are labeled:
- `match_status = excluded_from_age_engine_rollover_or_inherited`

Note: Roth rows are filtered out entirely before exclusion flags are set.

### 4.3 Age computation (🔴 Critical)

Engine B computes:
- `age_at_distribution` from `dob` and Matrix `txn_date`
- `age_at_termination` from `dob` and `term_date` (if available)

Age thresholds are applied using year-end attainment logic from `AGE_TAXCODE_CONFIG`.

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

Thresholds come from `AGE_TAXCODE_CONFIG` and are evaluated by year-end
attainment.

### 4.5 Roth plan exclusion (🟡 Important)

Roth plans are identified by configured `roth_plan_prefixes` and
`roth_plan_suffixes` (defaults: prefix `300005` or suffix `R`) via
`normalizers._is_roth_plan`.

Roth rows are excluded from Engine B and handled by Engine C.

### 4.6 Age engine comparison behavior (🔴 Critical)

The engine compares expected vs current codes:

- **Non-Roth:** compare `tax_code_1` only

Output:
- `match_no_action` when codes already match expected
- `match_needs_correction` when codes differ
  - `suggested_tax_code_1` populated (`suggested_tax_code_2` remains `NA`)
  - `action = UPDATE_1099`
  - `correction_reason` populated

---

## 5. Engine C — Roth Taxable Engine

### 5.1 Purpose
Engine C evaluates **Roth plans only** and produces taxable amount, Roth start
year, and Roth tax-code corrections in a single output. It reuses the same
correction file builder as Engines A/B.

### 5.2 Inputs and joins (🔴 Critical)
Engine C starts from Matrix rows and attaches:
- Relius demo data (`dob`, `term_date`) on `plan_id + ssn`
- Relius Roth basis data (`first_roth_tax_year`, `roth_basis_amt`) on `plan_id + ssn`

Matrix fields used include:
`plan_id`, `ssn`, `txn_date`, `transaction_id`, `gross_amt`, `fed_taxable_amt`,
`tax_code_1`, `tax_code_2`, `roth_initial_contribution_year`.

### 5.3 Roth plan identification (🔴 Critical)
Roth plans are identified by config-driven prefixes/suffixes:
- `ROTH_TAXABLE_CONFIG.roth_plan_prefixes` (default: `300005`)
- `ROTH_TAXABLE_CONFIG.roth_plan_suffixes` (default: `R`)

Matching is case-insensitive after trimming whitespace.

### 5.4 Exclusions and rollovers (🔴 Critical)
- **Inherited plans are excluded** (`INHERITED_PLAN_IDS`).
- **Rollovers are NOT excluded.** Roth rollovers still receive taxable and year checks.
- Certain tax codes are excluded from Engine C entirely
  (`ROTH_TAXCODE_CONFIG.excluded_codes_taxcode` like `11`, `13`, `15`, etc.)
  and are labeled with `match_status = excluded_from_age_engine_rollover_or_inherited`.

### 5.5 Taxable and basis rules (C1/C2/C3)

**C1 — Basis coverage (taxable to 0):**
- If `roth_basis_amt >= gross_2025_total` for the configured
  `ROTH_TAXABLE_CONFIG.basis_coverage_year`, then `suggested_taxable_amt = 0`.

**C2 — Qualified Roth (taxable to 0):**
- If participant attained qualified age in the transaction year **and**
  `txn_year - start_roth_year >= qualified_years_since_first`, then
  `suggested_taxable_amt = 0`.
- `start_roth_year` is derived from `first_roth_tax_year` or
  `roth_initial_contribution_year` (first valid value).

**C3 — Roth initial year mismatch / missing year:**
- If `first_roth_tax_year` is valid and differs from Matrix
  `roth_initial_contribution_year`, set `suggested_first_roth_tax_year`.
- If `first_roth_tax_year` is missing/invalid, flag **INVESTIGATE**
  (no suggested year).

**Review rule (15% proximity):**
- If `fed_taxable_amt > 0` and `gross_amt <= fed_taxable_amt * 1.15`,
  flag **INVESTIGATE** (used when no higher-priority correction applied).

### 5.6 Roth tax-code rules (🔴 Critical)

**Non-rollover Roth distributions:**
- `tax_code_1` must be **B**
- `tax_code_2` is derived from age rules (same as Engine B):
  - attained 59.5 in txn year → `7`
  - else if `term_date` exists:
    - attained 55 in term year → `2`
    - else → `1`
  - else (no term_date):
    - attained 55 in txn year → `2`
    - else → `1`

**Roth rollovers:**
- Rollovers are normalized to **H**:
  - `B + G`, `G + (blank)`, `(blank) + G` → `H`
  - `G + 4` → `H + 4`
- Rollovers are **not** excluded from taxable/basis logic.
- Age-based B* expectations do **not** apply to rollover-coded rows.
- Rows already coded as `H` or `B + 4` are treated as tax-code locked and skip
  age-based expectations.

**Death code normalization:**
- `4 + (blank)` and `(blank) + 4` are normalized to `B + 4`.

### 5.7 Engine C outputs

Engine C emits:
- `suggested_taxable_amt`
- `suggested_first_roth_tax_year`
- `suggested_tax_code_1`, `suggested_tax_code_2`, `new_tax_code`
- `action` (may include `UPDATE_1099` and/or `INVESTIGATE`)
- `match_status` (`match_no_action`, `match_needs_correction`, `match_needs_review`)
- `correction_reason` (newline-bulleted reason tokens)

---

## 6. Match Status Taxonomy

The following status values appear across engine outputs:

| Status | Meaning | Typical Next Step |
|---|---|---|
| `match_no_action` | Codes already correct (Engine A/B/C) | No action |
| `match_needs_correction` | Row needs correction (A/B/C) | Export to correction file |
| `match_needs_review` | Engine C flagged review-only item | Investigate |
| `date_out_of_range` | Candidate match but txn_date outside allowed window | Investigate timing |
| `unmatched_relius` | Relius row has no Matrix candidate | Investigate missing disbursement |
| `unmatched_matrix` | Matrix row has no Relius candidate | Investigate missing export |
| `excluded_from_age_engine_rollover_or_inherited` | Excluded due to tax-code list or rollover/inherited logic | Handled elsewhere / ignore |
| `age_rule_insufficient_data` | Missing DOB (or needed fields) | Review demographics data |

---

## 7. Correction File Contract

`build_correction_file.py` expects engines to provide:

### 7.1 Required fields (minimum)
- `match_status` (must be `match_needs_correction` to export)
- `suggested_tax_code_1` (required)
- `transaction_id` (Matrix)
- `txn_date` (Matrix)
- `ssn`
- `participant_name` or `full_name`
- `matrix_account`

### 7.2 Optional but recommended
- `suggested_tax_code_2` (Roth or two-code cases)
- `tax_code_1`, `tax_code_2` (current)
- `suggested_taxable_amt`
- `suggested_first_roth_tax_year`
- `plan_id`
- `action` (may contain multiple lines), `correction_reason`

Default output location for `write_correction_file` follows `USE_SAMPLE_DATA_DEFAULT`: `reports/samples/` in sample mode and `reports/outputs/` in production mode. An explicit `output_path` overrides these defaults.

---

## 8. Validation & QA Checklist

Before delivering a correction file:

### 8.1 Engine A validation
- ✅ Confirm `MATCHING_CONFIG.max_date_lag_days` reflects operational reality
- ✅ Spot-check a sample of `date_out_of_range` rows
- ✅ Verify no duplicate Matrix `transaction_id` in final correction output
- ✅ Verify inherited plans only apply inherited tax code rules

### 8.2 Engine B validation
- ✅ Confirm exclusions: no `AGE_TAXCODE_CONFIG.excluded_codes` rows are corrected
- ✅ Confirm inherited plans excluded from age output
- ✅ Confirm Roth plans are excluded (handled by Engine C)
- ✅ Confirm DOB join uses `(plan_id, ssn)` correctly

### 8.3 Engine C validation
- ✅ Confirm Roth plan patterns align with `ROTH_TAXABLE_CONFIG`
- ✅ Confirm inherited plans are excluded, rollovers are not
- ✅ Confirm rollover normalization to `H` (and `H + 4` when death code present)
- ✅ Confirm taxable updates follow C1/C2 rules and year mismatch triggers
- ✅ Confirm missing basis years are flagged as `INVESTIGATE`
- ✅ Confirm `suggested_taxable_amt` and `suggested_first_roth_tax_year` are present when applicable

---

## 9. Edge Cases & Failure Modes

- **Duplicate candidate matches (🟡):** same plan/SSN/amount repeats across weeks → can create repeated pairing candidates.
- **Missing DOB/term (🟡):** age engine falls back or marks insufficient data.
- **Multi-digit tax codes (🔴):** ensure normalization preserves `11`, not just first character.
- **SSN formatting artifacts (🔴):** Excel numeric formatting may add decimals or truncate; always treat as string + digit clean.
- **Plan naming variants (🟡):** Roth detection uses pattern rules; adjust if additional variants appear.
- **Roth rollovers (🔴):** rollovers are not excluded; they should normalize to `H` and still receive taxable/basis checks.
- **Missing/invalid Roth basis year (🟡):** Engine C flags `INVESTIGATE` rather than suggesting a year update.
- **Multi-action outputs (🟡):** Engine C may emit multi-line `action` values; correction builder handles line-splitting.

---

## 10. Privacy Notes

This repository should contain **synthetic or masked** data only.

Never commit:
- real SSNs
- DOBs
- termination dates
- internal plan exports
- participant names/addresses from production

Production runs should occur only in secure, access-controlled environments.
