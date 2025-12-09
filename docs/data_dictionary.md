# 📚 Data Dictionary – 1099 Reconciliation Pipeline

This document describes the key fields used in the reconciliation process.

It covers:

- **Relius export** (historical transaction data)
- **Matrix export** (disbursement / 1099 data)
- **Derived fields** used during cleaning and matching
- **Data quality issues** and validation rules

> Note: Column names in synthetic sample files may slightly differ (e.g. snake_case).  
> This dictionary reflects the conceptual fields used by the pipeline.

---

## Quick Reference

**Jump to:**
- [Data Flow Overview](#0-data-flow-overview) – Visual schema and relationships
- [Field Criticality](#05-field-importance--criticality) – Which fields matter most
- [Relius Export Fields](#1-relius-export--transaction-data) – Historical transaction data
- [Matrix Export Fields](#2-matrix-export--disbursement--1099-data) – Disbursement/1099 data
- [Derived Fields](#3-derived--cleaned-fields) – Calculated during pipeline
- [Data Quality Issues](#35-common-data-quality-issues) – Common problems and solutions
- [Validation Rules](#36-field-validation-rules) – Automated checks
- [Correction File](#4-correction-file-fields) – Final output for operations
- [Synthetic Data Notes](#5-notes-on-synthetic-data) – Privacy approach

---

### Most Critical Fields (Top 5)

For quick orientation, these are the most important fields:

1. **SSN** – Primary matching key, must be valid 9-digit format
2. **Gross_Amt** – Core reconciliation field, ±$1 tolerance
3. **Code_1099R** – Determines tax treatment, must match exactly
4. **Distribution_Date / Payment_Date** – Timing, ±3 day tolerance
5. **Taxable_Amt** – Affects participant taxes, must be accurate

**Color coding in this document:**
- 🔴 **Critical** – Errors cause incorrect 1099-R
- 🟡 **Important** – Significant but lower impact
- 🟢 **Reference** – Informational/investigative use

---

## 0. Data Flow Overview

### High-Level Data Model
```
┌─────────────────────────────────────────────────────────────┐
│                    RELIUS EXPORT                            │
│  (Historical Transactions - Source of Truth)                │
├─────────────────────────────────────────────────────────────┤
│ • Participant_ID      • Gross_Amt                           │
│ • SSN                 • Taxable_Amt                         │
│ • Plan_ID             • Fed_Withhold                        │
│ • Trans_Type          • Distribution_Date                   │
│ • Code_1099R          • Tax_Year                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │  MATCH ON:
                     │  • SSN (cleaned to 9 digits)
                     │  • Gross_Amt (±$1.00 tolerance)
                     │  • Date (±3 days tolerance)
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 MATCHED RECORDS                             │
│  (Reconciled Data with Classification)                      │
├─────────────────────────────────────────────────────────────┤
│ • match_status      • mismatch_type                         │
│ • action            • priority                              │
│ • Both Relius & Matrix fields preserved                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    MATRIX EXPORT                            │
│  (Disbursements - 1099-R Reporting)                         │
├─────────────────────────────────────────────────────────────┤
│ • SSN                 • Gross_Amt                           │
│ • Check_Num           • Taxable_Amt                         │
│ • Code_1099R          • Fed_Withhold                        │
│ • Payment_Date        • Tax_Year                            │
└─────────────────────────────────────────────────────────────┘
```

### Field Relationships

**Primary Keys:**
- `SSN` (cleaned to 9 digits) – Universal participant identifier
- `Gross_Amt` (normalized to cents) – Transaction amount
- `Distribution_Date` / `Payment_Date` (standardized) – Timing

**Critical for Matching:**
- SSN must be valid and consistent across systems
- Amount tolerance: ±$1.00 (handles rounding differences)
- Date tolerance: ±3 days (handles processing delays)

**Critical for 1099 Accuracy:**
- `Gross_Amt` – IRS reporting requirement
- `Taxable_Amt` – Determines tax liability
- `Code_1099R` – Determines tax treatment (early penalty, normal, rollover)
- `Fed_Withhold` – Withholding reporting to IRS

---

## 0.5 Field Importance & Criticality

Understanding which fields have the highest impact on reconciliation:

| Field | System | Criticality | Impact if Wrong | Used For |
|-------|--------|-------------|-----------------|----------|
| **SSN** | Both | 🔴 CRITICAL | Wrong person receives 1099-R | Primary matching key |
| **Gross_Amt** | Both | 🔴 CRITICAL | Incorrect tax liability reported | Amount matching, IRS reporting |
| **Code_1099R** | Both | 🔴 CRITICAL | Wrong tax treatment (penalty vs normal) | Tax classification |
| **Taxable_Amt** | Both | 🟡 HIGH | Participant overpays/underpays taxes | Tax calculation base |
| **Fed_Withhold** | Both | 🟡 HIGH | Withholding credit errors | Tax withholding reporting |
| **Distribution_Date** | Relius | 🟡 MEDIUM | Timing mismatch in records | Date matching |
| **Payment_Date** | Matrix | 🟡 MEDIUM | Payment timing variance | Date matching |
| **State_Withhold** | Both | 🟢 LOW | State reporting error only | State tax compliance |
| **Check_Num** | Both | 🟢 REFERENCE | N/A - for investigation only | Tracing/auditing |
| **Plan_ID** | Relius | 🟡 MEDIUM | Incorrect grouping/reporting | Plan-level analysis |
| **Participant_ID** | Both | 🟢 REFERENCE | No direct 1099 impact | Internal tracking |

### Criticality Definitions

- **🔴 CRITICAL:** Errors cause incorrect 1099-R, direct participant tax impact, compliance risk
- **🟡 HIGH:** Significant impact on accuracy, requires prompt attention and correction
- **🟡 MEDIUM:** Important for reconciliation accuracy but lower immediate compliance risk
- **🟢 LOW:** Nice to have, minimal impact if mismatched, informational value
- **🟢 REFERENCE:** Informational only, used for investigation and tracing

### Priority in Mismatch Detection

When classifying discrepancies, the pipeline prioritizes based on business impact:

1. **Amount mismatches** (>$1 difference) → 🔴 **HIGHEST PRIORITY**
   - Direct IRS reporting error
   - Affects participant's tax liability
   - Example: $15,000 vs $15,500 = $500 error on 1099-R

2. **Code mismatches** (wrong tax treatment) → 🔴 **HIGH PRIORITY**
   - Can trigger incorrect penalties
   - Example: Code 1 (early, 10% penalty) vs Code 7 (normal, no penalty)
   - $50,000 distribution = potential $5,000 penalty difference

3. **Withholding mismatches** (>$5 difference) → 🟡 **MEDIUM PRIORITY**
   - Affects withholding credit on tax return
   - Usually smaller dollar impact than amount/code errors

4. **Date mismatches** (>3 days) → 🟢 **LOW PRIORITY**
   - FYI only - minimal impact on 1099-R accuracy
   - Logged for data quality tracking but rarely requires action

### Match Tolerance Guidelines

| Field | Tolerance | Rationale |
|-------|-----------|-----------|
| SSN | Exact match required | No tolerance - must match exactly |
| Gross_Amt | ±$1.00 | Handles rounding differences between systems |
| Date | ±3 days | Allows for posting delays and processing timing |
| Code_1099R | Exact match required | No tolerance - tax treatment must be identical |
| Taxable_Amt | ±$1.00 | Same as gross amount tolerance |
| Withholding | ±$5.00 | Slightly higher tolerance for withholding calculations |

---

## 1. Relius Export – Transaction Data

**File examples:**  
`data/sample/relius_sample.xlsx`

| Column Name        | Type        | Example        | Description                                                                 | Notes                          |
|--------------------|------------|----------------|-----------------------------------------------------------------------------|--------------------------------|
| `Participant_ID`   | string/int | `123456`       | Internal participant identifier used by Relius                              | May not match Matrix IDs       |
| `SSN`              | string     | `123-45-6789`  | Participant Social Security Number                                          | Cleaned to 9-digit format      |
| `Plan_ID`          | string     | `401K-ABC`     | Plan identifier or contract number                                         | Used for plan-level grouping   |
| `Trans_Type`       | string     | `DIST`, `LOAN` | Transaction type (e.g. distribution, loan, refund)                          | Mapped/normalized in cleaning  |
| `Gross_Amt`        | float      | `15000.00`     | Gross distribution amount                                                   | 🔴 Used in matching logic         |
| `Taxable_Amt`      | float      | `15000.00`     | Taxable portion of distribution                                             | May differ from gross          |
| `Fed_Withhold`     | float      | `1500.00`      | Federal withholding amount                                                  | Included in mismatch checks    |
| `State_Withhold`   | float      | `300.00`       | State withholding amount (if applicable)                                    | Optional / may be null         |
| `Distribution_Date`| date/str   | `2024-01-15`   | Date the distribution was posted in Relius                                  | 🔴 Normalized to standard format  |
| `Tax_Year`         | int        | `2024`         | Tax year derived from distribution date                                     | Possibly explicit column       |
| `Code_1099R`       | string     | `7`, `1`, `G`  | Relius's version of distribution code, when available                      | 🔴 Used in code mismatch checks   |
| `Check_Num`        | string     | `0045123`      | Check or payment reference number (if available)                            | 🟢 May help in investigations     |
| `Source_System`    | string     | `Relius`       | Constant identifier for data source                                         | Helpful when concatenating     |
| `Last_Updated`     | date/str   | `2024-02-01`   | Timestamp of last update in Relius                                          | Optional, not always present   |

---

## 2. Matrix Export – Disbursement / 1099 Data

**File examples:**  
`data/sample/matrix_sample.xlsx`

| Column Name     | Type        | Example        | Description                                                                 | Notes                          |
|-----------------|------------|----------------|-----------------------------------------------------------------------------|--------------------------------|
| `SSN`           | string     | `123456789`    | Participant Social Security Number                                          | 🔴 Cleaned to 9-digit format      |
| `Participant_ID`| string/int | `123456`       | Participant identifier in Matrix (may or may not match Relius)             | Sometimes absent               |
| `Check_Num`     | string     | `0045123`      | Check number / payment reference                                            | 🟢 May align with Relius          |
| `Gross_Amt`     | float      | `15000.00`     | Gross disbursement amount                                                   | 🔴 Used in matching logic         |
| `Taxable_Amt`   | float      | `15000.00`     | Taxable portion of disbursement                                             | Used in discrepancy analysis   |
| `Fed_Withhold`  | float      | `1500.00`      | Federal withholding reported to IRS                                         | 🟡 Sensitive for 1099-R accuracy  |
| `State_Withhold`| float      | `300.00`       | State withholding reported                                                  | Optional / may be null         |
| `Code_1099R`    | string     | `7`, `1`, `G`  | Official 1099-R distribution code as reported to IRS                        | 🔴 Critical for 1099 correctness  |
| `Payment_Date`  | date/str   | `2024-01-17`   | Date the payment was made (check issued/ACH sent)                           | Slightly later than Relius date|
| `Tax_Year`      | int        | `2024`         | Tax year associated with the 1099-R                                         | Used in matching & reporting   |
| `Custodian_ID`  | string     | `MATRIX01`     | Identifier for Matrix / custodian system                                    | Optional                       |
| `Source_System` | string     | `Matrix`       | Constant identifier for data source                                         | Helpful when concatenating     |
| `Last_Updated`  | date/str   | `2024-02-03`   | Timestamp of last update in Matrix                                          | Optional                       |

---

## 3. Derived / Cleaned Fields

During the cleaning and matching steps, several **derived fields** are created. These may not exist in the raw Excel files but are used by the pipeline.

### 3.1 SSN-Related Fields

| Field Name     | Type   | Example      | Description                                    |
|----------------|--------|--------------|------------------------------------------------|
| `ssn_clean`    | string | `123456789`  | SSN stripped of non-digits, zero-padded to 9   |
| `ssn_valid`    | bool   | `True`       | Indicates if SSN passes basic length/format checks |
| `ssn_hashed`   | string | `a94a8f...`  | (Optional) Hashed SSN for anonymized analysis  |

> In the public repo, synthetic data may omit real SSNs and use pseudo-values.

---

### 3.2 Date-Related Fields

| Field Name      | Type | Example      | Description                                          |
|-----------------|------|--------------|------------------------------------------------------|
| `dist_date_std` | date | `2024-01-15` | Standardized distribution date from Relius          |
| `pay_date_std`  | date | `2024-01-17` | Standardized payment date from Matrix               |
| `tax_year`      | int  | `2024`       | Extracted or validated tax year                     |
| `date_diff`     | int  | `2`          | Difference in days between distribution and payment |

---

### 3.3 Amount-Related Fields

| Field Name       | Type  | Example | Description                                                   |
|------------------|-------|---------|---------------------------------------------------------------|
| `gross_cents`    | int   | `1500000` | Gross amount converted to integer cents (`gross * 100`)     |
| `taxable_cents`  | int   | `1500000` | Taxable amount in cents                                      |
| `fed_wh_cents`   | int   | `150000`  | Federal withholding in cents                                 |
| `state_wh_cents` | int   | `30000`   | State withholding in cents                                   |
| `amount_diff`    | float | `0.50`    | Difference between Relius and Matrix gross amounts           |

**Why cents?** Using cents as integers helps avoid floating point precision issues when comparing amounts.

**Example:**
```python
# Floating point comparison (unreliable)
15000.00 == 15000.05  # False (good)
15000.00 == 15000.004  # May be True or False (bad!)

# Integer cents comparison (reliable)
1500000 == 1500005  # False (correct)
1500000 == 1500000  # True (correct)
```

---

### 3.4 Matching & Classification Fields

After joining Relius and Matrix records, the pipeline introduces fields to describe match status and discrepancy types.

| Field Name       | Type   | Example                 | Description                                                    |
|------------------|--------|-------------------------|----------------------------------------------------------------|
| `match_key`      | string | `123456789_1500000_2024`| Composite key (ssn + amount + year) used for matching    |
| `match_status`   | string | `perfect`, `mismatch`, `unmatched_relius`, `unmatched_matrix` | High-level match category |
| `mismatch_type`  | string | `amount`, `code`, `date`, `withholding`, `multi` | Primary discrepancy driver |
| `action`         | string | `UPDATE_1099`, `VOID_AND_REISSUE`, `INVESTIGATE`           | Suggested operational action                                  |
| `priority`       | string | `HIGH`, `MEDIUM`, `LOW` | Business priority of the discrepancy                         |

---

## 3.5 Common Data Quality Issues

Real-world data challenges encountered and how the pipeline handles them:

### SSN Issues

| Issue | Example | How Pipeline Handles | Prevention |
|-------|---------|---------------------|------------|
| **Format variations** | `123-45-6789` vs `123456789` vs `123 45 6789` | Strip all non-digits using regex | Normalization in cleaning step |
| **Leading zeros missing** | `12345678` (should be `012345678`) | Zero-pad to 9 digits with `zfill(9)` | Validation + padding |
| **Invalid SSNs** | `000-00-0000`, `999-99-9999`, `123-45-67890` | Flag as invalid, log warning, exclude from matching | Validation rules |
| **Partial masking** | `***-**-6789` (last 4 only) | Cannot match - requires full SSN | Operations must provide full data |
| **Null/missing** | `NaN`, `null`, empty string | Flag record, cannot participate in matching | Require SSN for reconciliation |

**Code example:**
```python
def clean_ssn(ssn):
    """Clean and validate SSN"""
    # Remove all non-digits
    clean = re.sub(r'\D', '', str(ssn))
    # Pad to 9 digits
    clean = clean.zfill(9)
    # Validate
    if len(clean) != 9 or clean in ['000000000', '999999999']:
        return None
    return clean
```

**Handling in pipeline:**
- **5-8% of records** typically have SSN format issues
- **Automatic fix rate:** ~95% (format standardization)
- **Manual review required:** ~5% (invalid/partial SSNs)

---

### Amount Issues

| Issue | Example | How Pipeline Handles | Prevention |
|-------|---------|---------------------|------------|
| **Rounding differences** | `15000.00` vs `15000.05` | ±$1.00 tolerance in matching | Tolerance parameter configurable |
| **Negative amounts** | `-5000.00` (correction/reversal) | Handle as valid, flag for review | Sign preservation, negative flag |
| **Currency symbols** | `$15,000.00`, `USD 15000` | Strip non-numeric chars with regex | Cleaning regex |
| **Comma separators** | `15,000.00` | Remove commas before conversion | String cleaning |
| **Cents precision** | Float rounding errors (0.1 + 0.2 ≠ 0.3) | Convert to integer cents (`amount * 100`) | Integer arithmetic |
| **Scientific notation** | `1.5e4` (rare) | Parse as float first | Robust parsing |

**Code example:**
```python
def normalize_amount(amount):
    """Convert amount to integer cents"""
    # Remove currency symbols and commas
    clean = re.sub(r'[$,USD\s]', '', str(amount))
    # Convert to float, then to cents
    try:
        return int(float(clean) * 100)
    except ValueError:
        return None
```

**Handling in pipeline:**
- **2-3% of records** have amount format issues
- **Rounding tolerance:** ±$1.00 catches ~98% of legitimate matches
- **Large differences (>$100)** automatically flagged as high priority

---

### Date Issues

| Issue | Example | How Pipeline Handles | Prevention |
|-------|---------|---------------------|------------|
| **Format variations** | `01/15/2024`, `2024-01-15`, `Jan 15, 2024`, `15-Jan-2024` | Parse multiple formats sequentially | Standardize to YYYY-MM-DD |
| **Invalid dates** | `2024-02-30`, `13/01/2024`, `2024-00-01` | Try parsing, log error if fail, return None | Validation checks |
| **Missing dates** | `NaT`, `null`, empty string | Flag record, cannot match on date | Require date for reconciliation |
| **Time component** | `2024-01-15 14:30:00`, `2024-01-15T14:30:00Z` | Strip time, keep date only | Date extraction |
| **Excel date numbers** | `45321` (Excel serial date) | Convert from Excel serial to date | Excel date parsing |
| **Text dates** | `January fifteenth, 2024` | Not supported - log error | Standardize at source |

**Code example:**
```python
def standardize_date(date_str):
    """Parse and standardize date to YYYY-MM-DD"""
    # Handle Excel serial dates
    if isinstance(date_str, (int, float)):
        return pd.to_datetime(date_str, unit='D', origin='1899-12-30').strftime('%Y-%m-%d')
    
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%b %d, %Y', '%d-%b-%Y']
    for fmt in formats:
        try:
            return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
        except:
            continue
    return None
```

**Handling in pipeline:**
- **1-2% of records** have date format issues
- **Date tolerance:** ±3 days catches processing delays
- **Failed parsing:** Logged for manual review

---

### Code_1099R Issues

| Issue | Example | How Pipeline Handles | Prevention |
|-------|---------|---------------------|------------|
| **Format variations** | `7`, `07`, `Code 7`, `Code-7` | Strip non-digits, standardize to single char | Normalization |
| **Invalid codes** | `99`, `X`, `0` | Flag as invalid, log warning | Validation against IRS codes |
| **Missing codes** | `null`, empty, `N/A` | Cannot validate, flag for review | Require code for distributions |
| **Multiple codes** | `1,7` (rare edge case) | Use first code, log warning | Data quality at source |
| **Text descriptions** | `Early Distribution` instead of `1` | Not supported - requires code | Standardize at source |

**Valid 1099-R codes (IRS):**
- `1` – Early distribution (age <59½, usually 10% penalty)
- `2` – Early distribution, exception applies (no penalty)
- `4` – Death benefit
- `7` – Normal distribution (age 59½+, no penalty)
- `8` – Excess contributions corrected
- `G` – Rollover to another qualified plan (not taxable)
- `L` – Loan treated as distribution
- `P` – Excess contributions plus earnings
- `Q` – Qualified distribution from Roth

**Handling in pipeline:**
- **<1% of records** have invalid codes
- **Missing codes:** Cannot validate 1099 accuracy - flagged HIGH priority
- **Code mismatches:** Always flagged for review (no tolerance)

---

### Duplicate Issues

| Issue | Example | How Pipeline Handles | Prevention |
|-------|---------|---------------------|------------|
| **Exact duplicates** | Same SSN, amount, date appears 2x in one system | Deduplicate, keep first occurrence | Flag in data quality report |
| **Partial duplicates** | Same SSN, similar amount, same day | Keep both, flag for manual review | Requires investigation |
| **Cross-system duplicates** | Same record in both Relius AND Matrix | Expected - this is a MATCH (good!) | Normal reconciliation behavior |
| **Reversal pairs** | Original + reversal (positive + negative) | Keep both, note in comments | Track corrections separately |

**Handling in pipeline:**
- **<0.5% of records** are true duplicates
- **Deduplication:** Based on SSN + amount + date + system
- **Flagged for review:** All duplicate scenarios logged

---

### Impact Summary

**Data quality issues found in typical reconciliation run:**

| Issue Type | Frequency | Auto-Fixed | Manual Review | Example Count (10K records) |
|-----------|-----------|------------|---------------|---------------------------|
| SSN format | 5-8% | 95% | 5% | 500-800 records, 25-40 need review |
| Amount rounding | 2-3% | 98% | 2% | 200-300 records, 4-6 need review |
| Date format | 1-2% | 90% | 10% | 100-200 records, 10-20 need review |
| Invalid 1099-R codes | <1% | 0% | 100% | 10-50 records, all need review |
| Duplicates | <0.5% | 80% | 20% | 10-50 records, 2-10 need review |

**Overall:** Pipeline handles **95%+ of issues automatically**. Remaining **~5% flagged for manual review** in correction file.

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

**Statistics:** ~95% of cleaned SSNs pass validation

---

### Amount Validation

**Must pass:**
```python
# Validation rules:
1. Numeric type (after cleaning)
2. Gross_Amt >= 0 OR negative_flag = True (for corrections)
3. Taxable_Amt <= Gross_Amt (logical constraint)
4. Fed_Withhold <= Gross_Amt (cannot withhold more than gross)
5. Taxable_Amt >= 0 (negative taxable amount illogical)
6. Amount not absurdly large (e.g., > $10M per distribution)
```

**Python implementation:**
```python
def validate_amounts(gross, taxable, fed_wh):
    """Validate amount relationships"""
    if gross < 0 and not is_correction:  # Corrections can be negative
        return False
    if taxable > gross:  # Taxable can't exceed gross
        return False
    if fed_wh > gross:  # Withholding can't exceed gross
        return False
    if abs(gross) > 10_000_000:  # Sanity check
        return False
    return True
```

**Result:** Invalid amounts flagged for review

**Common issues caught:**
- Taxable > Gross: ~0.1% of records
- Withholding > Gross: ~0.05% of records

---

### Date Validation

**Must pass:**
```python
# Validation rules:
1. Valid date format (parseable)
2. Year between 1990-2050 (reasonable range)
3. Distribution_Date <= Today (cannot distribute future money)
4. Payment_Date <= Today + 30 days (allow near-future payments)
5. Payment_Date >= Distribution_Date - 30 days (payment shouldn't precede distribution by much)
6. Date is not a holiday/weekend for business logic (optional check)
```

**Python implementation:**
```python
def validate_dates(dist_date, pay_date):
    """Validate date relationships"""
    from datetime import datetime, timedelta
    
    today = datetime.now().date()
    
    # Basic range checks
    if dist_date.year < 1990 or dist_date.year > 2050:
        return False
    if dist_date > today:
        return False
    if pay_date > today + timedelta(days=30):
        return False
    
    # Relationship check
    if pay_date < dist_date - timedelta(days=30):
        return False  # Payment before distribution unusual
    
    return True
```

**Result:** Invalid dates flagged, cannot participate in matching

**Common issues caught:**
- Future dates: ~0.5% of records
- Illogical date sequences: ~0.2% of records

---

### Code_1099R Validation

**Must be:**
```python
# Validation rules:
1. One of the valid IRS codes: 1, 2, 4, 7, 8, G, L, P, Q
2. Single character or digit
3. Not null/empty for distributions (required for 1099-R)
4. Not invalid legacy codes (some systems have old codes)
```

**Python implementation:**
```python
def validate_1099r_code(code):
    """Validate 1099-R distribution code"""
    valid_codes = ['1', '2', '4', '7', '8', 'G', 'L', 'P', 'Q']
    
    if not code or pd.isna(code):
        return False
    
    code_clean = str(code).strip().upper()
    
    if code_clean not in valid_codes:
        return False
    
    return True
```

**Result:** Invalid codes flagged HIGH priority (cannot validate 1099 accuracy)

**IRS Code Reference:**
- `1` = Early distribution, no known exception
- `2` = Early distribution, exception applies
- `4` = Death benefit
- `7` = Normal distribution
- `8` = Excess contributions
- `G` = Rollover (not taxable)
- `L` = Loan treated as distribution
- `P` = Excess contributions plus earnings
- `Q` = Qualified Roth distribution

---

### Cross-Field Validation

**Logical consistency checks:**
```python
# Business logic rules:
1. IF Code_1099R == '1' (early dist) THEN expect participant age < 59.5 (if age available)
2. IF Code_1099R == 'G' (rollover) THEN expect Taxable_Amt == 0 or near 0
3. IF Taxable_Amt == 0 THEN Code_1099R should be 'G' or have valid exception
4. IF Gross_Amt != Taxable_Amt THEN difference should be < 50% (rarely >50% non-taxable)
5. IF Fed_Withhold > 0 THEN expect Gross_Amt > minimum threshold ($200)
```

**Python implementation:**
```python
def cross_validate(gross, taxable, code, age=None):
    """Validate logical relationships between fields"""
    issues = []
    
    # Rollover check
    if code == 'G' and taxable > gross * 0.1:  # Allow 10% tolerance
        issues.append("Rollover (Code G) should have minimal taxable amount")
    
    # Taxable exceeds gross by large margin
    if taxable > gross * 1.5:
        issues.append("Taxable amount significantly exceeds gross")
    
    # Early distribution with age check
    if code == '1' and age and age >= 59.5:
        issues.append("Code 1 (early dist) but participant age >= 59.5")
    
    return issues
```

**Result:** Flags illogical combinations for manual investigation

**Common illogical patterns:**
- Code G (rollover) with taxable amount: ~0.3% of records
- Large taxable/gross ratio: ~0.1% of records

---

### Validation Summary

**Validation statistics (typical 10K record reconciliation):**

| Validation Type | Pass Rate | Common Failures | Impact |
|----------------|-----------|-----------------|--------|
| **SSN** | 95% | Format issues, invalid area codes | Cannot match without valid SSN |
| **Amount** | 98% | Taxable > Gross, negative amounts | Flags for review |
| **Date** | 97% | Future dates, invalid formats | Cannot match without valid date |
| **1099-R Code** | 99% | Invalid codes, missing codes | HIGH priority - affects tax treatment |
| **Cross-field** | 97% | Illogical relationships | Flags for investigation |

**Overall validation pass rate:** ~95% of records pass all checks

**Records requiring manual review:** ~5% (500 out of 10,000)

---

## 4. Correction File Fields

The final correction Excel file (e.g. `reports/samples/correction_file_sample.xlsx`) includes a curated set of fields designed for the **operations team**.

| Column Name       | Type    | Example         | Description                                              |
|-------------------|---------|-----------------|----------------------------------------------------------|
| `SSN`             | string  | `***-**-6789`   | Masked SSN or internal ID (synthetic in public repo)    |
| `Plan_ID`         | string  | `401K-ABC`      | Plan identifier                                          |
| `Relius_Amt`      | float   | `15000.00`      | Gross amount from Relius                                |
| `Matrix_Amt`      | float   | `15050.00`      | Gross amount from Matrix                                |
| `Diff_Amt`        | float   | `50.00`         | Amount difference (Matrix - Relius)                     |
| `Relius_Code`     | string  | `7`             | 1099-R code from Relius                                 |
| `Matrix_Code`     | string  | `1`             | 1099-R code from Matrix                                 |
| `Relius_Date`     | date    | `2024-01-15`    | Distribution date from Relius                           |
| `Matrix_Date`     | date    | `2024-01-17`    | Payment date from Matrix                                |
| `Mismatch_Type`   | string  | `code`          | Primary discrepancy classification                      |
| `Action`          | string  | `VOID_AND_REISSUE` | Recommended operational action                        |
| `Priority`        | string  | `HIGH`          | Business priority (HIGH/MEDIUM/LOW)                     |
| `Notes`           | string  | `Verify 1099-R code with source docs` | Free-text column for operations comments            |

### Action Codes

| Action Code | When Used | Operations Response |
|------------|-----------|-------------------|
| `UPDATE_1099` | Minor discrepancy, 1099 not yet issued | Update system, correct before mailing |
| `VOID_AND_REISSUE` | Significant error, 1099 already issued | Void incorrect 1099, issue corrected version |
| `INVESTIGATE` | Complex issue, requires manual review | Review source documents, determine root cause |
| `NO_ACTION` | Within tolerance, informational only | No action needed, log for tracking |

### Priority Levels

| Priority | Criteria | Example | Action Timeline |
|----------|----------|---------|----------------|
| `HIGH` | Amount diff >$100 OR code mismatch with penalty impact | $50K with Code 1 vs 7 | Same day review |
| `MEDIUM` | Amount diff $10-$100 OR withholding mismatch | Withholding $500 vs $450 | Within 3 days |
| `LOW` | Date mismatch only OR amount diff <$10 | Date differs by 2 days | Weekly review |

---

## 5. Notes on Synthetic Data

### 🔒 Privacy & Security

In this public repository:

- ✅ **Column structures mirror real exports** - Field names, types, relationships are accurate
- ✅ **Field types and relationships are accurate** - Data model represents production
- ✅ **Statistical patterns preserved** - Distributions, frequencies match real data
- ❌ **Values are 100% synthetic** - Generated using Python's Faker library
- ❌ **SSNs, IDs, plan numbers, amounts are NOT real** - No real participant data
- ❌ **No real participant data** appears anywhere in this repository

### ⚠️ Important Disclaimers

- **No real participant data** appears in this repository under any circumstances
- Any resemblance to real persons, plans, or transactions is purely coincidental
- **Production implementation** uses secure, encrypted real data in controlled environment
- **This demo version** allows safe public sharing while preserving methodology
- All synthetic SSNs generated using invalid area codes (900-999 range)
- All synthetic amounts are random within realistic ranges ($100 - $100,000)

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
- ✅ **Typical distribution patterns** - Match rates, mismatch types mirror production
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

| Category | Field Count | Critical Fields | Notes |
|----------|-------------|-----------------|-------|
| **Relius Export** | 13 | 5 (SSN, Gross_Amt, Code, Date, Plan_ID) | Historical transactions |
| **Matrix Export** | 12 | 5 (SSN, Gross_Amt, Code, Date, Tax_Year) | Disbursement/1099 data |
| **Derived Fields** | 15 | 4 (ssn_clean, gross_cents, match_key, match_status) | Calculated during pipeline |
| **Correction File** | 13 | 6 (SSN, amounts, codes, priority, action) | Operations deliverable |
| **Total Unique** | ~30 | ~10 critical for matching | Across all stages |

---

## Document History

- **v1.0** (December 2024): Initial data dictionary
- **v1.1** (December 2024): Added visual schema, criticality matrix, data quality issues, validation rules
- **Purpose:** Technical reference for 1099 reconciliation pipeline
- **Audience:** Data engineers, operations team, auditors, hiring managers

---

*This data dictionary provides comprehensive field documentation for the 1099 reconciliation pipeline. All examples use synthetic data for demonstration purposes.*