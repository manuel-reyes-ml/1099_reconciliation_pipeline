## Summary
This PR extends the Matrix ingestion pipeline to support Engine C (Roth taxable analysis) by adding two required fields to the canonical schema and normalizing them during cleaning.

## What changed
- **config**
  - Mapped Matrix raw columns to canonical names:
    - `Fed Taxable Amount` → `fed_taxable_amt`
    - `Roth Initial Contribution Year` → `roth_initial_contribution_year`
  - Added both fields to `MATRIX_CORE_COLUMNS` so they flow through the pipeline.
- **clean_matrix**
  - Normalized `fed_taxable_amt` using numeric coercion (safe handling of blanks/strings).
  - Normalized `roth_initial_contribution_year` to a pandas nullable integer (`Int64`) for consistent downstream rule logic.

## Why
Engine C requires both **taxable amount** and **Roth initial contribution year** to:
- validate Roth taxable calculations,
- apply qualified distribution checks (age + 5-year test),
- and populate correction template fields for taxable/year updates.

This PR is intentionally **additive** and does not change reconciliation keys or existing engine behavior.

## How to verify
### Notebook smoke check
1. Load a Matrix export (sample/synthetic).
2. Run `clean_matrix()`.
3. Confirm:
   - Columns exist: `fed_taxable_amt`, `roth_initial_contribution_year`
   - Types are correct:
     - `fed_taxable_amt` is numeric
     - `roth_initial_contribution_year` is `Int64` (nullable integer)
   - Existing outputs remain unchanged for non-Roth workflows.

### CLI import check
```bash
python -c "from src.clean_matrix import clean_matrix; print('OK: clean_matrix import')"
```

## Impact / Risk
- **Risk level:** Low  
- **Impact:** Adds two canonical fields required for Roth analysis; enables downstream Engine C without altering current match/correction behavior.  
- **Compatibility:** No changes to matching keys or existing correction exports. Missing columns in an input file should result in nulls (NaN/NA) rather than failures.

## Project hygiene
- No real data committed (synthetic/sample only)
- Issue tracked on Project board
- Scope limited to Issue 1 changes

Closes #1
