#Docstring for config.py
"""

Configuration for the 1099 reconciliation pipeline.

Central place for:
- Directory Paths
- Matching tolerances
- Column names mappings (raw Excel -> canonical names)
- Core columns to keep from wide exports
- Match key columns for de-duplication and reconciliation
- Business rule configuration (inherited plans, code fixes, etc.)

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

@dataclass(frozen=True) #Decorator to create data class
class MatchingConfig:
#dataclass creates init, repr, eq, etc. methods automatically
#frozen=True makes instances immutable, once created cannot be changed

    """
    
    Configuration for matching logic.

    amount_tolerance_cents:
        Maximum allowed difference in gross amount between systems to still consider
        amounts "matching" (e.g., 100 = $1.00).
    max_date_lag_days:
        Maximum allowed difference in days between Relius exported_date and Matrix
        transaction date. We expect:
        
            0 <= txn_date - exported_date <= max_date_lag_days

        because Relius exports first and Matrix posts later.

    """

    amount_tolerance_cents: int = 100  # $1.00 tolerance
    max_date_lag_days: int = 10        # up to 10 days after export

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
#
# Canonical names are what the rest of the pipeline will use.

# Relius export (HUGE file, we only care about a subset)

RELIUS_COLUMN_MAP = {
    # Raw column name       # Canonical name
    "PLANID_1":       "plan_id",
    "SSNUM_1":        "ssn",
    "FIRSTNAM":       "first_name",
    "LASTNAM":        "last_name",
    "STATEADDR":      "state",
    "GROSSDISTRAMT":  "gross_amt",
    "EXPORTEDDATE":   "exported_date",
    "DISTR1CD":       "dist_code_1",    # primary 1099-R code (box 7)
    "TAXYR":          "tax_year",
    "DISTRNAM":       "dist_name",   # helps identify "individual vs rollover" distributions
    # add more as needed later
}



MATRIX_COLUMN_MAP = {
    # Raw column name       # Canonical name
    "Matrix Account":      "matrix_account",    # appears in correction template
    "Client Account":      "plan_id",           # aligns with PLANID_1
    "Participant SSN":     "ssn",
    "Participant Name":    "participant_name",
    "Participant State":   "state",
    "Gross Amount":        "gross_amt",
    "Transaction Date":    "txn_date",
    "Transaction Type":    "txn_method",
    "Tax Code":            "tax_code_1",        # primary tax code
    "Tax Code 2":          "tax_code_2",        # secondary tax code (if used)
    "Tax Form":            "tax_form",
    "Distribution Type":   "dist_type",
    "Transaction Id":      "transaction_id",
    # add more if needed (e.g. Created Date, Approved Date...)
}




# --- Core columns & match keys ----------------------------------------------------

# These are the canonical columns we actually want to KEEP from the wide
# 100-column exports. Cleaning functions will subset to these to drop the noise.

RELIUS_CORE_COLUMNS = [
    "plan_id",
    "ssn",
    "first_name",
    "last_name",
    "state",
    "gross_amt",
    "exported_date",
    "tax_year",
    "dist_code_1",
    "dist_name",
]

MATRIX_CORE_COLUMNS = [
    "plan_id",
    "ssn",
    "participant_name",
    "state",
    "gross_amt",
    "txn_date",
    "txn_method",
    "tax_code_1",
    "tax_code_2",
    "tax_form",
    "dist_type",
    "transaction_id",
    "matrix_account",
]


# Columns used specifically as keys for:
# - dropping duplicate rows inside each system
# - matching Relius vs Matrix records
# You can tweak these depending on how you define a "transaction".
#
# Note: for the actual match we will ALSO apply the "future-only" date lag
# constraint (0 <= txn_date - exported_date <= max_date_lag_days).

RELIUS_MATCH_KEYS = [
    "plan_id",
    "ssn",
    "gross_amt",
    "exported_date",
    "tax_year",
]

MATRIX_MATCH_KEYS = [
    "plan_id",
    "ssn",
    "gross_amt",
    "txn_date",
]



# --- Business rules (plan-specific 1099 code logic) ---------------------------

# Inherited plan IDs that have special 1099-R code handling.
INHERITED_PLAN_IDS = {
    "300004PLAT",
    "300004MBD",
    "300004MBDII",
}


# We store business rules as dictionaries here.
# A later module (e.g. business_rules.py or inside match_transactions.py) will
# interpret and apply these to the matched DataFrame.

SPECIAL_CODE_RULES = [
    {
        # For inherited plans, a final distribution coded as 7M
        # should be corrected to code 4.
        "name": "inherited_final_7_to_4",
        "plan_ids": INHERITED_PLAN_IDS,
        "condition": "final_distribution",   # to be derived from tot_distr_cd / dist_type
        "current_tax_code_1": {"7"},
        "new_tax_code_1": "4",
        "new_tax_code_2": None,
        "action": "UPDATE_1099",
        "priority": "HIGH",
    },
    {
        # For inherited plans, a rollover distribution needs:
        #   tax_code_1 = G   (rollover)
        #   tax_code_2 = 4   (death / inherited)
        "name": "inherited_rollover_G_and_4",
        "plan_ids": INHERITED_PLAN_IDS,
        "condition": "rollover_distribution",   # to be derived from dist_type / other flags
        "current_tax_code_1": {"7"},           # placeholder: whatever mis-code they currently use
        "new_tax_code_1": "G",
        "new_tax_code_2": "4",
        "action": "UPDATE_1099",
        "priority": "HIGH",
    },
]

# In the future we can add more rule sets here, for other plan groups or logic.



# --- Utility flags ------------------------------------------------------------

# If you ever want to quickly swap between sample data and "real" exports
# in your notebooks or scripts, you can use this flag as a default.
USE_SAMPLE_DATA_DEFAULT = True