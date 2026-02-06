# Copilot Instructions for 1099 Reconciliation Pipeline

## Project Overview

This is an **automated data reconciliation pipeline** for retirement plan distributions. It reconciles transaction data between two financial systems (Relius and Matrix), applies business rules via four independent "engines", and produces tax-form correction recommendations.

**Core Problem:** Relius (transaction source) and Matrix (disbursement/1099-R processor) don't always agree on amounts, dates, and tax codes—leading to incorrect 1099-R forms mailed to participants.

## Architecture: The Four Engines

The pipeline processes data through four independent analysis engines, each producing distinct corrections:

| Engine | File | Input | Output | Purpose |
|--------|------|-------|--------|---------|
| **A** | `src/engines/match_planid.py` | Relius + Matrix + config rules | Matched/unmatched transaction pairs | Reconcile inherited-plan distributions (tax-code corrections) |
| **B** | `src/engines/age_taxcode_analysis.py` | Matrix + Relius demographics (DOB) | Tax-code suggestions based on age/term | Audit age-based non-Roth tax codes (59.5+→Code 7, etc.) |
| **C** | `src/engines/roth_taxable_analysis.py` | Matrix + Relius Roth basis + demographics | Roth taxable amounts & tax codes | Validate Roth taxable/basis logic and Roth code rules (Code B*) |
| **D** | `src/engines/ira_rollover_analysis.py` | Matrix-only (IRA plans) | Rollover tax-form corrections | Align IRA rollover tax-form selections (G/H codes → No Tax) |

**Key Design:** Engines are intentionally independent—you can run all four or select specific ones. Each produces its own output and correction recommendations.

## Critical Data Flow

```
Raw Excel Exports
    ↓
    ├─→ cleaning/clean_relius.py → canonical DataFrame
    ├─→ cleaning/clean_matrix.py → canonical DataFrame
    ├─→ cleaning/clean_relius_demo.py (demographics)
    └─→ cleaning/clean_relius_roth_basis.py (Roth data)
    ↓
Engines A, B, C, D (independent analysis)
    ↓
outputs/build_correction_file.py (consolidates results)
    ↓
Matrix-ready Excel correction template
```

## Configuration: The Single Source of Truth

**File:** `src/config.py` (541 lines)

This file centralizes ALL business logic and column mappings. Do not hardcode rules elsewhere. Key sections:

- **Column mappings:** `MATRIX_COLUMN_MAP`, `RELIUS_COLUMN_MAP` – raw export headers → canonical names (e.g., "Distribution Amount" → "gross_amt")
- **Core columns:** `MATRIX_CORE_COLUMNS`, `RELIUS_CORE_COLUMNS` – minimum fields required per system
- **Matching config:** `MATCH_KEYS`, `MAX_DELAY_DAYS` – controls how transactions are joined (plan_id + ssn + gross_amt, asymmetric date window)
- **Inherited plans:** `INHERITED_PLAN_IDS`, tax code mapping for cash/rollover distributions
- **Age rules:** `AGE_TAXCODE_CONFIG` – age thresholds, expected codes by age/term status
- **Roth rules:** Plan ID patterns ("300005*" or ending with "R"), Roth code enforcement (Code B for non-rollover)

**When to modify config:**
- Adding/removing required columns → update `*_CORE_COLUMNS`
- Changing inherited plan list → update `INHERITED_PLAN_IDS`
- Adjusting date tolerance → modify `MAX_DELAY_DAYS`
- Tweaking age thresholds → update `AGE_TAXCODE_CONFIG`

## Canonical Schema (Critical for Matching)

After cleaning, all dataframes use these standard field names. **Use these exact names** when writing new engines:

**Core Identifiers:**
- `plan_id` – Retirement plan ID
- `ssn` – 9-digit normalized SSN
- `transaction_id` / `trans_id_relius` – System-specific transaction ID

**Amounts & Dates:**
- `gross_amt` – Total distribution amount (matching key)
- `txn_date` (Matrix) / `exported_date` (Relius) – Transaction timing
- `taxable_amt` – Subject to income tax (used by Roth engine)

**Tax Codes:**
- `tax_code_1` – Primary 1099-R box 7 code (e.g., "7", "1", "G", "B")
- `tax_code_2` – Secondary code (e.g., "G" for rollover, "H" for IRA rollover)
- `suggested_tax_code_1` / `suggested_tax_code_2` – Recommended corrections
- `new_tax_code` – Combined/final code (e.g., "4G", "7", "B2")

**Distribution Metadata:**
- `dist_category_relius` – Derived from DISTRNAM: "rollover", "cash_distribution", "rmd", etc.
- `dist_code_1` (Relius) – Source distribution code
- `participant_dob` – Participant date of birth (used by age engine)
- `term_date` – Participant termination date (affects age-based codes)

**Roth-Specific:**
- `first_roth_tax_year` – Initial Roth contribution year
- `roth_basis_amt` – After-tax contributions on record
- `roth_initial_contribution_year` (Matrix) – Custodian's Roth start year

**Status & Reason:**
- `match_status` – "match_no_action", "match_needs_correction", "date_out_of_range", "unmatched_relius", "unmatched_matrix"
- `correction_reason` – Machine-readable token(s): "inherited_rollover", "age_59_5", "code_1_should_be_7", etc.

## Matching Logic: Core Rules

**Engine A Matching (inherited-plan reconciliation):**

1. **Join on match keys:** `plan_id + ssn + gross_amt`
2. **Date tolerance:** Matrix `txn_date` must be ≥ Relius `exported_date` AND ≤ `exported_date + MAX_DELAY_DAYS`
3. **Inherited plan rules:** If plan is inherited:
   - Rollover distributions → recommend Code 4 + G
   - Cash distributions → recommend Code 4 (or based on plan-specific mapping)
   - Non-inherited → no correction suggested (unless other rule triggers)

**Engine B (Age-based, non-Roth):**

- Age at distribution ≥ 59.5 → Code 7 expected
- Age < 59.5 + terminated ≥ 55 years old → Code 2 expected  
- Age < 59.5 + no term date or term < 55 → Code 1 expected
- Skip if Roth plan (Engine C handles those)

**Engine C (Roth):**

- Roth plans identified by `plan_id` pattern ("300005*" or ends with "R")
- Non-rollover distributions: Code B* expected (B1, B2, B3, etc.)
- Rollover distributions: Code H expected
- Taxable amount logic: `taxable_amt = gross_amt - roth_basis_amt`

**Engine D (IRA rollover tax-form):**

- IRA plans only (identified in config)
- If `tax_code_1` or `tax_code_2` is G or H (rollover codes) → tax form should be "No Tax" (or code 0)
- Mismatch → flag for correction

## Testing & Validation

**Test Structure:** `tests/` mirrors `src/`:
- `tests/pipelines/` – Integration tests for multi-engine workflows (test that outputs have required columns, match_status values)
- `tests/validators/` – Field validation tests (SSN normalization, date parsing, amount rounding)
- `tests/ira_rollover/` – Engine D specific tests
- `tests/roth_taxable/` – Engine C specific tests

**Key Test Files:**
- `test_new_tax_code_unified.py` – End-to-end engine outputs (all four engines)
- `test_date_filter_engines_ab.py` – Date filtering for Engines A & B
- `test_apply_date_filter.py` – Validator for date filtering logic

**Run Tests:**
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/pipelines/test_new_tax_code_unified.py

# Run with coverage
pytest --cov=src tests/
```

## Notebooks: Analysis & Iteration

Notebooks are NOT production code. They're for exploratory analysis and demonstration:

- `00_generate_sample_data.ipynb` – Generates synthetic data (run first after cloning)
- `01_data_understanding.ipynb` – EDA on cleaned inputs
- `02_cleaning_pipeline.ipynb` – Showcase all cleaning steps
- `03_match_planid_analysis.ipynb` – Engine A results & visualizations
- `04_match_demo_analysis.ipynb` – Engine B (age-based) results
- `05_match_roth_basis_analysis.ipynb` – Engine C (Roth) results
- `06–10` – Visualization notebooks (match rates, correction distribution, etc.)

**When writing/editing notebooks:**
- Use absolute imports: `from src.engines.match_planid import reconcile_relius_matrix`
- Import config at top: `from src.config import MATCH_KEYS, MAX_DELAY_DAYS`
- Run cleaning modules explicitly (don't assume intermediate CSVs exist)
- Don't hardcode file paths; use `pathlib.Path` and relative imports

## Code Conventions (from `.cursor/rules/`)

**Naming:**
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `SCREAMING_SNAKE_CASE`
- Private methods: `_leading_underscore`

**Docstrings:**
- Every module starts with a docstring explaining purpose, design goals, inputs, outputs, typical usage
- Example: See top of `src/engines/match_planid.py`

**Data Quality:**
- Normalize SSN to 9-digit string: `ssn.str.replace("-", "").str.zfill(9)`
- Parse dates with `pd.to_datetime(..., errors="coerce")` (invalid → NaT, don't fail)
- Amounts: `pd.to_numeric(errors="coerce")` 
- Tax codes: strip/uppercase, handle both "7" and "Code 7" formats

**Error Handling:**
- Fail fast with meaningful error messages
- Use assertions for sanity checks: `assert df["match_status"].isin(VALID_STATUSES)`
- Log warnings for data quality issues (e.g., missing DOB in demographics)

## Common Patterns

### Adding a New Engine

1. Create `src/engines/my_engine.py` with docstring explaining purpose
2. Import config and canonical columns at top
3. Write function `def run_my_engine(matrix_df: pd.DataFrame, ...) -> pd.DataFrame:`
4. Populate required output columns: `match_status`, `suggested_tax_code_1`, `new_tax_code`, `correction_reason`
5. Add test in `tests/my_engine/test_my_engine.py` (verify status values and output schema)
6. Hook into `build_correction_dataframe()` in `src/outputs/build_correction_file.py` (consolidation step)

### Adding a New Business Rule

1. Modify `src/config.py` to define the rule parameter (e.g., new age threshold, plan ID pattern)
2. Update the relevant engine logic (e.g., `src/engines/age_taxcode_analysis.py`)
3. Add test case in `tests/` with expected input/output
4. Update `docs/matching_logic.md` with rule definition and examples

### Adding a New Column to Output

1. **Verify column exists in cleaning step:** Update `src/config.py` column mappings/core columns
2. **Populate in engine:** Add logic to derive or copy to output DataFrame
3. **Export in correction file:** Update `src/outputs/build_correction_file.py` to include column in template
4. **Test:** Add assertion in pipeline test that column is present and has expected values

## Integration with Notebooks

Notebooks execute engine functions and consolidate results:

```python
# In notebook (after import from src)
from src.engines.match_planid import reconcile_relius_matrix
from src.engines.age_taxcode_analysis import run_age_taxcode_analysis
from src.engines.roth_taxable_analysis import run_roth_taxable_analysis
from src.engines.ira_rollover_analysis import run_ira_rollover_analysis
from src.outputs.build_correction_file import build_correction_dataframe

# Run engines
engine_a_result = reconcile_relius_matrix(relius_df, matrix_df, apply_business_rules=True)
engine_b_result = run_age_taxcode_analysis(matrix_df, demographics_df, demographics_index_col="plan_id_ssn")
engine_c_result = run_roth_taxable_analysis(matrix_df, roth_basis_df, demographics_df)
engine_d_result = run_ira_rollover_analysis(matrix_df)

# Consolidate
combined_df = build_correction_dataframe(engine_a_result, engine_b_result, engine_c_result, engine_d_result)

# Export
combined_df.to_excel("output.xlsx", index=False)
```

## Key Files at a Glance

- **Config & schemas:** `src/config.py` (business rules, column mappings)
- **Data cleaning:** `src/cleaning/clean_*.py` (normalize inputs to canonical schema)
- **Engines:** `src/engines/match_planid.py`, `age_taxcode_analysis.py`, `roth_taxable_analysis.py`, `ira_rollover_analysis.py`
- **Output/export:** `src/outputs/build_correction_file.py`, `export_utils.py`
- **Validators:** `src/core/validators.py` (data quality checks)
- **Normalizers:** `src/core/normalizers.py` (SSN, date, amount, tax code standardization)
- **Documentation:** `docs/business_context.md`, `docs/matching_logic.md`, `docs/data_dictionary.md`

## Privacy & Synthetic Data

All data in this repo is **synthetic** (generated by `src/core/generate_sample_data.py` using Faker). Original production pipeline handles real SSNs and tax codes but cannot be shared for compliance reasons.

When testing/developing:
- Use `notebooks/00_generate_sample_data.ipynb` to refresh sample data
- Never commit real participant data
- Mask/anonymize examples in documentation

---

**Last Updated:** February 2026  
**See also:** `.cursor/rules/` for universal Python/Git standards, `README.md` for project background
