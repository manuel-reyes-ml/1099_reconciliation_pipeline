#Docstring for config.py
"""

Configuration for the 1099 reconciliation pipeline.

Central place for:
- Directory Paths
- Matching tolerances
- Column names mappings (raw Excel -> canonical names)
- Core columns to keep from wide exports
- Match key columns for de-duplication and reconciliation

"""

from dataclasses import dataclass #create simple classes for configuration
from pathlib import Path #object-oriented filesystem paths instead of strings



# --- Base paths ----------------------------------------------------------------

# src/ -> project root
BASE_DIR = Path(__file__).resolve().parents[1]
#parents[0] = config.py directory = src/
#parents[1] = project root directory = 1099_reconciliation_pipeline/

DATA_DIR = BASE_DIR / "data"
SAMPLE_DIR = DATA_DIR / "sample"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_FIGURES_DIR = REPORTS_DIR / "figures"
REPORTS_SAMPLES_DIR = REPORTS_DIR / "samples"

LOGS_DIR = BASE_DIR / "logs"

# Ensure key folders exist when running locally (safe no-op if they exist)
for _path in [
    DATA_DIR,
    SAMPLE_DIR,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    REPORTS_DIR,
    REPORTS_FIGURES_DIR,
    REPORTS_SAMPLES_DIR,
    LOGS_DIR,
]:
    _path.mkdir(parents=True, exist_ok=True)
    #Make directory:
    #- parents=True: create any missing parent directories
    #- exist_ok=True: no error if directory already exists



# --- Matching configuration ------------------------------------------------------------

@dataclass(frozen=True)
class MatchingConfig:
#dataclass creates init, repr, eq, etc. methods automatically
#frozen=True makes instances immutable, once created cannot be changed

    """
    
    Configuration for matching logic.

    amount_tolerance_cents:
        Maximum allowed difference in gross amount between systems to still consider
        amounts "matching" (e.g., 100 = $1.00).
    date_tolerance_days:
        Maximum allowed difference in days between distribution and payment dates
        (e.g. 3 = +/- 3 days).

    """

    amount_tolerance_cents: int = 100  # $1.00 tolerance
    date_tolerance_days: int = 3        # +/- 3 days tolerance

MATCHING_CONFIG = MatchingConfig()
#Creates a singleton instance of MatchingConfig with default values
#For example, to access amount_tolerance_cents:
    #from src.config import MATCHING_CONFIG
    #tol_amount = MATCHING_CONFIG.amount_tolerance_cents
    #tol_days = MATCHING_CONFIG.date_tolerance_days



# --- Column name mapping (raw -> canonical) --------------------------------------------

# IMPORTANT:
# Left side keys MUST match the header names in your actual Excel exports.
# If your files use different labels (e.g. "Participant Id" instead of
# "Participant_ID"), adjust the keys accordingly.

RELIUS_COLUMN_MAP = {
    # Raw column name       # Canonical name
    "Participant_ID": "participant_id",
    "SSN": "ssn",
    "Plan_ID": "plan_id",
    "Trans_Type": "trans_type",
    "Gross_Amt": "gross_amt",
    "Taxable_Amt": "taxable_amt",
    "Fed_Withhold": "fed_withhold",
    "State_Withhold": "state_withhold",
    "Distribution_Date": "dist_date",
    "Tax_Year": "tax_year",
    "Code_1099R": "code_1099r",
    "Check_Num": "check_num",
}


MATRIX_COLUMN_MAP = {
    # Raw column name       # Canonical name
    "Participant_ID": "participant_id",
    "SSN": "ssn",
    "Check_Num": "check_num",
    "Gross_Amt": "gross_amt",
    "Taxable_Amt": "taxable_amt",
    "Fed_Withhold": "fed_withhold",
    "State_Withhold": "state_withhold",
    "Code_1099R": "code_1099r",
    "Payment_Date": "pay_date",
    "Tax_Year": "tax_year",
}



# --- Core columns & match keys ----------------------------------------------------

# These are the canonical columns we actually want to KEEP from the wide
# 100-column exports. Cleaning functions will subset to these.

RELIUS_CORE_COLUMNS = [
    "participant_id",
    "ssn",
    "plan_id",
    "trans_type",
    "gross_amt",
    "taxable_amt",
    "fed_withhold",
    "state_withhold",
    "dist_date",
    "tax_year",
    "code_1099r",
    "check_num",
]

MATRIX_CORE_COLUMNS = [
    "participant_id",
    "ssn",
    "gross_amt",
    "taxable_amt",
    "fed_withhold",
    "state_withhold",
    "pay_date",
    "tax_year",
    "code_1099r",
    "check_num",
]

# Columns used specifically as keys for:
# - dropping duplicate rows inside each system
# - matching Relius vs Matrix records
# You can tweak these depending on how you define a "transaction".

RELIUS_MATCH_KEYS = [
    "ssn",
    "gross_amt",
    "dist_date",
    "tax_year",
    "plan_id",
]

MATRIX_MATCH_KEYS = [
    "ssn",
    "gross_amt",
    "pay_date",
    "tax_year",
]



# --- Utility flags ------------------------------------------------------------

# If you ever want to quickly swap between sample data and "real" exports
# in your notebooks or scripts, you can use this flag as a default.
USE_SAMPLE_DATA_DEFAULT = True