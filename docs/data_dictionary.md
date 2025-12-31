# 📚 Data Dictionary – 1099 Reconciliation Pipeline

This document describes the key fields used in the reconciliation process.

It covers:

- **Relius distributions** (transaction data)
- **Relius demographics** (DOB/termination)
- **Relius Roth basis** (first year/basis)
- **Matrix export** (disbursement / 1099 data)
- **Derived fields** used during cleaning and matching
- **Data quality issues** and validation rules

> Note: Column names in synthetic sample files may slightly differ from raw exports.  
> This dictionary reflects the canonical fields used by the pipeline (`src/config.py`).

---

## Quick Reference

**Jump to:**
- [Data Flow Overview](#0-data-flow-overview) – Visual schema and relationships
- [Field Criticality](#05-field-importance--criticality) – Which fields matter most
- [Relius Export Fields](#1-relius-export--transaction-data) – Historical transaction data
- [Matrix Export Fields](#2-matrix-export--disbursement--1099-data) – Disbursement/1099 data
- [Relius Demo Fields](#25-relius-demo-export--participant-data) – DOB/term data
- [Relius Roth Basis Fields](#26-relius-roth-basis-export) – Roth basis data
- [Derived Fields](#3-derived--cleaned-fields) – Calculated during pipeline
- [Data Quality Issues](#35-common-data-quality-issues) – Common problems and solutions
- [Validation Rules](#36-field-validation-rules) – Automated checks
- [Correction File](#4-correction-file-fields) – Final output for operations
- [Synthetic Data Notes](#5-notes-on-synthetic-data) – Privacy approach

---

### Most Critical Fields (Top 5)

For quick orientation, these are the most important fields:

1. **plan_id** – Primary plan identifier used in matching
2. **ssn** – Primary participant key, must be valid 9-digit format
3. **gross_amt** – Core reconciliation field used in matching
4. **exported_date / txn_date** – Timing window for Engine A matching
5. **tax_code_1 / tax_code_2** – Determines tax treatment and corrections

**Color coding in this document:**
- 🔴 **Critical** – Errors cause incorrect 1099-R
- 🟡 **Important** – Significant but lower impact
- 🟢 **Reference** – Informational/investigative use

---

## 0. Data Flow Overview

### High-Level Data Model
```
┌─────────────────────────────────────────────────────────────┐
│                 RELIUS DISTRIBUTIONS                        │
│  (plan_id, ssn, gross_amt, exported_date, dist_code_1)       │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                      MATRIX EXPORT                          │
│  (plan_id, ssn, gross_amt, txn_date, tax_code_1/2, fed_taxable_amt) │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │  Engine A match keys:
                     │  • plan_id + ssn + gross_amt
                     │  • date lag window via MATCHING_CONFIG
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 MATCHED OUTPUT (ENGINE A)                   │
│  (match_status, suggested_tax_code_*, action, reason)        │
└─────────────────────────────────────────────────────────────┘

Additional inputs for Engines B/C:
┌──────────────────────────┐   ┌─────────────────────────────┐
│ RELIUS DEMO (DOB/term)   │   │ RELIUS ROTH BASIS            │
└──────────────────────────┘   └─────────────────────────────┘
```

### Field Relationships

**Primary Keys (Engine A):**
- `plan_id` – Plan identifier (normalized)
- `ssn` – Participant identifier (cleaned to 9 digits)
- `gross_amt` – Transaction amount
- `exported_date` / `txn_date` – Timing window

**Critical for Matching:**
- plan_id + ssn + gross_amt must align across systems
- Date lag window enforced via `MATCHING_CONFIG.max_date_lag_days`

**Critical for 1099 Accuracy:**
- `gross_amt` – IRS reporting requirement
- `fed_taxable_amt` – Taxable amount in Matrix
- `tax_code_1` / `tax_code_2` – Determines tax treatment
- `first_roth_tax_year` / `roth_initial_contribution_year` – Roth qualification inputs

---

## 0.5 Field Importance & Criticality

Understanding which fields have the highest impact on reconciliation:

| Field | System | Criticality | Impact if Wrong | Used For |
|-------|--------|-------------|-----------------|----------|
| **plan_id** | Both | 🔴 CRITICAL | Wrong plan matching | Primary matching key |
| **ssn** | Both | 🔴 CRITICAL | Wrong participant match | Primary matching key |
| **gross_amt** | Both | 🔴 CRITICAL | Incorrect tax reporting | Matching and reconciliation |
| **tax_code_1** | Matrix | 🔴 CRITICAL | Wrong tax treatment | Tax classification |
| **tax_code_2** | Matrix | 🟡 HIGH | Secondary code impacts | Roth/inherited logic |
| **fed_taxable_amt** | Matrix | 🟡 HIGH | Taxable amount errors | Roth taxable checks |
| **exported_date** | Relius | 🟡 MEDIUM | Timing mismatch | Engine A date lag |
| **txn_date** | Matrix | 🟡 MEDIUM | Timing mismatch | Engine A date lag |
| **first_roth_tax_year** | Relius Roth basis | 🟡 HIGH | Roth qualification errors | Engine C start year |
| **roth_initial_contribution_year** | Matrix | 🟡 HIGH | Roth year mismatch | Engine C corrections |
| **transaction_id** | Matrix | 🟢 REFERENCE | Investigation and output | Correction file key |
| **matrix_account** | Matrix | 🟢 REFERENCE | Operations output field | Correction file key |

### Criticality Definitions

- **🔴 CRITICAL:** Errors cause incorrect 1099-R, direct participant tax impact, compliance risk
- **🟡 HIGH:** Significant impact on accuracy, requires prompt attention and correction
- **🟡 MEDIUM:** Important for reconciliation accuracy but lower immediate compliance risk
- **🟢 LOW:** Nice to have, minimal impact if mismatched, informational value
- **🟢 REFERENCE:** Informational only, used for investigation and tracing

### Priority in Mismatch Detection

When classifying discrepancies, the pipeline prioritizes based on engine output:

1. **Engine A corrections** (inherited-plan tax code updates)
2. **Engine B corrections** (age-based non-Roth tax codes)
3. **Engine C corrections** (Roth taxable amount and Roth start year updates)
4. **Review-only items** (INVESTIGATE) and date out-of-range flags

### Match Tolerance Guidelines

| Field | Tolerance | Rationale |
|-------|-----------|-----------|
| plan_id | Exact match required | Primary matching key |
| ssn | Exact match required | Primary participant key |
| gross_amt | Exact match required | Matching key in Engine A |
| exported_date/txn_date | Config-driven lag window | `MATCHING_CONFIG.max_date_lag_days` (default 10) |
| tax_code_1/2 | Exact match required | Tax treatment must be identical |

---

## 1. Relius Export – Transaction Data

**File examples:**  
`data/sample/relius_sample.xlsx`

| Column Name     | Type      | Example      | Description                                                  | Notes |
|----------------|-----------|--------------|--------------------------------------------------------------|-------|
| `plan_id`      | string    | `300004PLAT` | Plan identifier (normalized)                                 | 🔴 Matching key |
| `ssn`          | string    | `123456789`  | Participant SSN                                              | 🔴 Matching key |
| `first_name`   | string    | `Ava`        | Participant first name                                       | Used for full_name |
| `last_name`    | string    | `Nguyen`     | Participant last name                                        | Used for full_name |
| `state`        | string    | `CA`         | Participant state                                            | Optional |
| `gross_amt`    | float     | `15000.00`   | Gross distribution amount                                    | 🔴 Matching key |
| `exported_date`| date      | `2024-01-15` | Relius export date                                           | Used for date lag |
| `tax_year`     | int       | `2024`       | Tax year                                                     | Optional |
| `dist_code_1`  | string    | `7`          | Relius distribution code (when present)                      | Validation only |
| `dist_name`    | string    | `Rollover`   | Distribution description                                     | Used for dist_category_relius |

---

## 2. Matrix Export – Disbursement / 1099 Data

**File examples:**  
`data/sample/matrix_sample.xlsx`

| Column Name                     | Type   | Example      | Description                                            | Notes |
|---------------------------------|--------|--------------|--------------------------------------------------------|-------|
| `matrix_account`                | string | `07B00442`   | Matrix account identifier                              | Output key |
| `plan_id`                       | string | `300004PLAT` | Plan identifier                                        | 🔴 Matching key |
| `ssn`                           | string | `123456789`  | Participant SSN                                        | 🔴 Matching key |
| `participant_name`              | string | `Ava Nguyen` | Participant display name                               | Optional |
| `state`                         | string | `CA`         | Participant state                                      | Optional |
| `gross_amt`                     | float  | `15000.00`   | Gross disbursement amount                              | 🔴 Matching key |
| `fed_taxable_amt`               | float  | `15000.00`   | Taxable amount reported in Matrix                      | Engine C input |
| `txn_date`                      | date   | `2024-01-17` | Matrix transaction date                                | Date lag |
| `txn_method`                    | string | `ACH`        | Transaction method/type                                | Optional |
| `tax_code_1`                    | string | `7`          | Primary 1099-R tax code                                | 🔴 Correction logic |
| `tax_code_2`                    | string | `G`          | Secondary 1099-R tax code                              | Engine A/C |
| `tax_form`                      | string | `1099-R`     | Tax form identifier                                    | Optional |
| `dist_type`                     | string | `Rollover`   | Distribution type                                      | Optional |
| `roth_initial_contribution_year` | int   | `2016`       | Roth start year (Matrix)                               | Engine C |
| `transaction_id`                | string | `44324568`   | Matrix transaction ID                                  | Output key |

---

## 2.5 Relius Demo Export – Participant Data

**File examples:**  
`data/sample/relius_demo_sample.xlsx`

| Column Name | Type | Example | Description | Notes |
|-------------|------|---------|-------------|-------|
| `plan_id`   | string | `300004PLAT` | Plan identifier | Join key |
| `ssn`       | string | `123456789` | Participant SSN | Join key |
| `dob`       | date   | `1970-05-10` | Date of birth | Engine B/C |
| `term_date` | date   | `2020-12-31` | Termination date | Engine B/C |
| `first_name`| string | `Ava` | Participant first name | Optional |
| `last_name` | string | `Nguyen` | Participant last name | Optional |

---

## 2.6 Relius Roth Basis Export

**File examples:**  
`data/sample/relius_roth_basis_sample.xlsx`

| Column Name | Type | Example | Description | Notes |
|-------------|------|---------|-------------|-------|
| `plan_id` | string | `300005ABC` | Plan identifier | Join key |
| `ssn` | string | `123456789` | Participant SSN | Join key |
| `first_roth_tax_year` | int | `2016` | First Roth tax year | Engine C |
| `roth_basis_amt` | float | `12000.00` | Roth basis amount | Engine C |

---

## 3. Derived / Cleaned Fields

During the cleaning and matching steps, several **derived fields** are created. These may not exist in the raw Excel files but are used by the pipeline.

### 3.1 SSN-Related Fields

| Field Name   | Type | Example | Description |
|--------------|------|---------|-------------|
| `ssn`        | string | `123456789` | Normalized SSN (non-digits stripped, zero-padded) |
| `ssn_valid`  | bool | `True` | Validation flag from `validate_ssn_series` |

> In the public repo, synthetic data uses non-real SSNs.

---

### 3.2 Date-Related Fields

| Field Name | Type | Example | Description |
|------------|------|---------|-------------|
| `exported_date` | date | `2024-01-15` | Relius export date (Engine A) |
| `txn_date` | date | `2024-01-17` | Matrix transaction date |
| `date_lag_days` | int | `2` | `txn_date - exported_date` |
| `date_within_tolerance` | bool | `True` | Lag within `MATCHING_CONFIG.max_date_lag_days` |

---

### 3.3 Amount-Related Fields

| Field Name | Type | Example | Description |
|------------|------|---------|-------------|
| `gross_amt` | float | `15000.00` | Gross distribution amount |
| `fed_taxable_amt` | float | `15000.00` | Matrix taxable amount |
| `roth_basis_amt` | float | `12000.00` | Roth basis amount from Relius |
| `suggested_taxable_amt` | float | `0.00` | Engine C suggested taxable amount |

---

### 3.4 Matching & Classification Fields

After running Engine A/B/C, the pipeline introduces fields to describe match status and corrections.

| Field Name | Type | Example | Description |
|------------|------|---------|-------------|
| `match_status` | string | `match_needs_correction` | Engine status label |
| `dist_category_relius` | string | `rollover` | Relius distribution category |
| `full_name` | string | `Ava Nguyen` | Derived from Relius first/last name |
| `expected_tax_code_1` | string | `4` | Expected primary tax code |
| `expected_tax_code_2` | string | `G` | Expected secondary tax code |
| `suggested_tax_code_1` | string | `4` | Suggested primary tax code |
| `suggested_tax_code_2` | string | `G` | Suggested secondary tax code |
| `new_tax_code` | string | `4G` | Combined suggested tax code |
| `suggested_first_roth_tax_year` | int | `2016` | Engine C suggested Roth start year |
| `action` | string | `UPDATE_1099` | Recommended action |
| `correction_reason` | string | `- roth_initial_year_mismatch` | Audit-friendly reason token(s) |

---

## 3.5 Common Data Quality Issues

Real-world data challenges encountered and how the pipeline handles them:

### SSN Issues
- **Format variations** (hyphens, spaces) are normalized to 9 digits.
- **Invalid SSNs** are flagged via `ssn_valid` and surfaced in `validation_issues`.

### Amount Issues
- Amounts are coerced to numeric with `to_numeric_series`.
- `amount_valid` flags missing or illogical values (e.g., taxable > gross).

### Date Issues
- Dates are coerced with `to_date_series` and validated with `validate_dates_series`.
- Invalid or out-of-range dates are flagged for review.

### Tax Code Issues
- Tax codes are normalized to 1-2 characters with `normalize_tax_code_series`.
- Invalid codes are flagged via `code_1099r_valid`.
- Valid codes include `1`, `2`, `4`, `7`, `8`, `B`, `G`, `H`, `L`, `P`, `Q`.

### Duplicate Issues
- Each cleaned dataset is deduplicated using configured match keys.
- Duplicate handling is conservative and logged for review in notebooks.

---

## 3.6 Field Validation Rules

Automated checks performed during data cleaning:

### SSN Validation

**Must pass ALL checks:**
```python
# Validation rules:
1. Length == 9 digits (after cleaning/padding)
2. Not all zeros (000000000)
3. Not all nines (999999999)
4. Not sequential (012345678, 123456789)
5. First 3 digits not 000, 666, or 900-999 (invalid area numbers per SSA)
```

**Python implementation:**
```python
def validate_ssn(ssn_clean):
    """Validate cleaned 9-digit SSN"""
    if len(ssn_clean) != 9:
        return False
    if ssn_clean in ['000000000', '999999999', '123456789']:
        return False
    area = ssn_clean[:3]
    if area in ['000', '666'] or (area[0] == '9'):
        return False
    return True
```

**Result:** `ssn_valid` field set to True/False

---

### Amount Validation

**Must pass:**
```python
# Validation rules:
1. Numeric type (after cleaning)
2. gross_amt >= 0 unless explicitly marked as correction
3. fed_taxable_amt <= gross_amt (logical constraint)
4. fed_taxable_amt >= 0
5. gross_amt not absurdly large (e.g., > $10M per distribution)
```

**Result:** Invalid amounts flagged for review via `amount_valid`.

---

### Date Validation

**Must pass:**
```python
# Validation rules:
1. Valid date format (parseable)
2. Year between 1990-2050 (reasonable range)
3. exported_date <= Today (cannot be in the future)
4. txn_date <= Today + 30 days (allow near-future postings)
5. txn_date >= exported_date - 30 days (pre-export postings are unusual)
```

**Result:** Invalid dates flagged via `date_valid`.

---

### Tax Code Validation

**Must be:**
```python
# Validation rules:
1. One of the valid IRS codes: 1, 2, 4, 7, 8, B, G, H, L, P, Q
2. 1-2 character alphanumeric (post-normalization)
3. Not null/empty for distributions
```

**Result:** Invalid codes flagged via `code_1099r_valid`.

**IRS Code Reference:**
- `1` = Early distribution, no known exception
- `2` = Early distribution, exception applies
- `4` = Death benefit
- `7` = Normal distribution
- `8` = Excess contributions
- `B` = Distribution from Roth sources
- `G` = Rollover from pre-tax distribution (not taxable)
- `H` = Rollover from Roth distribution (not taxable)
- `L` = Loan treated as distribution
- `P` = Excess contributions plus earnings
- `Q` = Qualified Roth distribution

---

### Cross-Field Validation

**Logical consistency checks:**
```python
# Business logic rules:
1. IF tax_code_1 == 'G' THEN fed_taxable_amt should be <= 10% of gross
2. IF fed_taxable_amt > gross_amt * 1.5 THEN flag as invalid
3. IF tax_code_1 == '1' AND age >= 59.5 (when provided) THEN flag
```

**Result:** Flags illogical combinations for manual investigation via `validation_issues`.

---

### Validation Summary

Validation flags are produced during cleaning so downstream engines can decide
whether to proceed or require review. Invalid rows are not automatically
dropped unless required by match keys.

---

### Runtime Validation Flags

Cleaners emit the following columns to surface validation outcomes without
dropping records:

- `ssn_valid`: boolean flag for SSN validation.
- `amount_valid`: boolean flag for gross/taxable amount checks.
- `date_valid`: boolean flag for distribution/payment date logic.
- `code_1099r_valid`: boolean flag for IRS code validation.
- `validation_issues`: list of issue tokens for failed validations and
  cross-field logic. Current tokens include:
  - `ssn_invalid`
  - `amount_invalid`
  - `date_invalid`
  - `code_1099r_invalid`
  - `cross_code_g_taxable_over_10pct`
  - `cross_taxable_exceeds_gross_150pct`
  - `cross_code1_age_over_59_5`

---

## 4. Correction File Fields

The final correction Excel file (e.g. `reports/samples/correction_file_YYYYMMDD_HHMMSS.xlsx`) includes a curated set of fields designed for the **operations team**.

| Column Name | Type | Example | Description |
|-------------|------|---------|-------------|
| `Transaction Id` | string | `44324568` | Matrix transaction identifier |
| `Transaction Date` | date | `2024-01-17` | Matrix transaction date |
| `Participant SSN` | string | `123456789` | Normalized SSN |
| `Participant Name` | string | `Ava Nguyen` | Name used for review |
| `Matrix Account` | string | `07B00442` | Matrix account identifier |
| `Current Tax Code 1` | string | `7` | Matrix tax code 1 |
| `Current Tax Code 2` | string | `G` | Matrix tax code 2 |
| `New Tax Code` | string | `4G` | Suggested combined tax code |
| `New Taxable Amount` | float | `0.00` | Suggested taxable amount |
| `New First Year contrib` | int | `2016` | Suggested Roth start year |
| `Reason` | string | `- roth_initial_year_mismatch` | Correction reason token(s) |
| `Action` | string | `UPDATE_1099` | Recommended action |

### Action Codes

| Action Code | When Used | Operations Response |
|------------|-----------|-------------------|
| `UPDATE_1099` | Correction required | Update Matrix values |
| `INVESTIGATE` | Missing or ambiguous data | Manual review |

### Priority Levels

Priority levels are managed by operations based on match_status and action.

---

## 5. Notes on Synthetic Data

### 🔒 Privacy & Security

In this public repository:

- ✅ **Column structures mirror real exports** - Field names, types, relationships are accurate
- ✅ **Field types and relationships are accurate** - Data model represents production
- ✅ **Representative patterns** - Synthetic data covers common scenarios
- ❌ **Values are 100% synthetic** - Generated with a deterministic Python generator (`python -m src.generate_sample_data` from the repo root)
- ❌ **SSNs, IDs, plan numbers, amounts are NOT real** - No real participant data
- ❌ **No real participant data** appears anywhere in this repository
- ✅ **Regeneratable inputs** - Run `python -m src.generate_sample_data` from the repo root or `notebooks/07_generate_sample_data.ipynb` to refresh `data/sample/`

### ⚠️ Important Disclaimers

- **No real participant data** appears in this repository under any circumstances
- Any resemblance to real persons, plans, or transactions is purely coincidental
- **Production implementation** uses secure, encrypted real data in controlled environment
- **This demo version** allows safe public sharing while preserving methodology
- Synthetic SSNs are non-real and used for demonstration only
- Synthetic amounts are generated within realistic ranges for testing

### ✅ What This Allows

This synthetic data approach enables:

- ✅ **Safe demonstration** of pipeline capabilities without compliance risk
- ✅ **Public portfolio** sharing without violating participant privacy
- ✅ **Reproducible examples** - anyone can clone and run the code
- ✅ **Privacy-first mindset** - essential skill for financial data work
- ✅ **Educational value** - learn reconciliation concepts without real data exposure
- ✅ **Interview readiness** - show understanding of data privacy requirements

### 🎓 For Learning & Portfolio

The synthetic data maintains authenticity by preserving:

- ✅ **Realistic field formats** - SSN patterns, date formats, amount ranges
- ✅ **Common data quality issues** - Format variations, nulls, duplicates included
- ✅ **Typical distribution patterns** - Includes common matches and edge cases
- ✅ **Edge cases** - Negative amounts, rollovers, early distributions represented
- ✅ **System differences** - Relius vs Matrix timing/format variations
- ✅ **Reconciliation challenges** - Rounding differences, date mismatches, code issues

### 🔐 Production Data Handling

In the real implementation (not in this repo):

- **Data encryption** - All SSNs and sensitive fields encrypted at rest
- **Access control** - Role-based permissions, audit logging
- **Secure storage** - Data never leaves approved secure environment
- **Compliance** - GDPR, SOC 2, financial services regulations followed
- **Data retention** - Automatic purging per policy
- **Incident response** - Procedures for data breach scenarios

**This provides authentic learning while protecting privacy - a critical skill for data professionals in finance, healthcare, and other sensitive domains.**

---

## Appendix: Field Count Summary

| Category | Notes |
|----------|-------|
| **Relius Distributions** | Core matching fields plus distribution metadata |
| **Relius Demo** | DOB and termination dates for age-based rules |
| **Relius Roth Basis** | Roth start year and basis totals |
| **Matrix Export** | Transaction identifiers, tax codes, taxable amounts |
| **Derived Fields** | Match status, suggested codes, actions, reasons |
| **Correction File** | Matrix-ready output with suggested updates |

---

## Document History

- **v1.0** (December 2024): Initial data dictionary
- **v1.1** (December 2024): Added visual schema, criticality matrix, data quality issues, validation rules
- **Purpose:** Technical reference for 1099 reconciliation pipeline
- **Audience:** Data engineers, operations team, auditors, hiring managers

---

*This data dictionary provides comprehensive field documentation for the 1099 reconciliation pipeline. All examples use synthetic data for demonstration purposes.*
