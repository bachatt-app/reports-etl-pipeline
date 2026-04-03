"""
CAMS column mapping and BigQuery MERGE SQL.

CSV → BQ mapping
----------------
CAMS CSV headers are ALL_CAPS and match BigQuery target column names exactly,
so CSV_TO_BQ is an identity mapping for all non-computed, non-overflow columns.

Unknown columns in the CSV are placed into EXTRA_COLUMN1…5 overflow slots.
Any unknown columns beyond the 5 slots trigger an alert email.
"""

from src.processors.schema.mapper import ColumnMapper

# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

# All BigQuery target columns in INSERT order (matches MERGE query exactly).
CAMS_BQ_COLUMNS: list[str] = [
    "AMC_CODE", "FOLIO_NO", "PRODCODE", "SCHEME", "INV_NAME", "TRXNTYPE", "TRXNNO",
    "TRXNMODE", "TRXNSTAT", "USERCODE", "USRTRXNO", "TRADDATE", "POSTDATE",
    "PURPRICE", "UNITS", "AMOUNT", "BROKCODE", "SUBBROK", "BROKPERC", "BROKCOMM",
    "ALTFOLIO", "REP_DATE", "TIME1", "TRXNSUBTYP", "APPLICATIO", "TRXN_NATUR",
    "TAX", "TOTAL_TAX", "TE_15H", "MICR_NO", "REMARKS", "SWFLAG", "OLD_FOLIO",
    "SEQ_NO", "REINVEST_F", "MULT_BROK", "STT", "LOCATION", "SCHEME_TYP",
    "TAX_STATUS", "LOAD", "SCANREFNO", "PAN", "INV_IIN", "TARG_SRC_S",
    "TRXN_TYPE_", "TICOB_TRTY", "TICOB_TRNO", "TICOB_POST", "DP_ID",
    "TRXN_CHARG", "ELIGIB_AMT", "SRC_OF_TXN", "TRXN_SUFFI", "SIPTRXNNO",
    "TER_LOCATI", "EUIN", "EUIN_VALID", "EUIN_OPTED", "SUB_BRK_AR",
    "EXCH_DC_FL", "SRC_BRK_CO", "SYS_REGN_D", "AC_NO", "BANK_NAME",
    "REVERSAL_C", "EXCHANGE_F", "CA_INITIAT", "GST_STATE_", "IGST_AMOUN",
    "CGST_AMOUN", "SGST_AMOUN", "REV_REMARK", "ORIGINAL_T", "STAMP_DUTY",
    "FOLIO_OLD", "SCHEME_FOL", "AMC_REF_NO", "REQUEST_RE", "TRANSMISSI",
    # Overflow slots for unknown CSV columns
    "EXTRA_COLUMN1", "EXTRA_COLUMN2", "EXTRA_COLUMN3", "EXTRA_COLUMN4", "EXTRA_COLUMN5",
    # Computed by BQ MERGE — never uploaded in CSV
    "trad_date", "row_hash", "created_at",
]

_OVERFLOW = {"EXTRA_COLUMN1", "EXTRA_COLUMN2", "EXTRA_COLUMN3", "EXTRA_COLUMN4", "EXTRA_COLUMN5"}
_COMPUTED = {"trad_date", "row_hash", "created_at"}

# Identity mapping: CSV col name == BQ col name for all known columns.
_KNOWN_COLS: list[str] = [
    c for c in CAMS_BQ_COLUMNS if c not in _OVERFLOW and c not in _COMPUTED
]
CAMS_CSV_TO_BQ: dict[str, str] = {col: col for col in _KNOWN_COLS}


class CamsColumnMapper(ColumnMapper):
    BQ_COLUMNS = CAMS_BQ_COLUMNS
    CSV_TO_BQ = CAMS_CSV_TO_BQ
    COMPUTED_COLUMNS = _COMPUTED
    OVERFLOW_COLUMNS = sorted(_OVERFLOW)  # EXTRA_COLUMN1 … EXTRA_COLUMN5 in order


# ---------------------------------------------------------------------------
# BigQuery MERGE SQL
# ---------------------------------------------------------------------------
# Placeholders: {project}  {dataset}
# The external table R2_cams_reports_external is hive-partitioned on
# year/month/day columns derived from the GCS path structure.
# ---------------------------------------------------------------------------

CAMS_MERGE_SQL = """\
MERGE INTO `{project}.{dataset}.R2_cams_final_view` AS Target
USING (
  WITH normalized_data AS (
    SELECT
      t.*,
      SAFE.PARSE_DATE('%Y%m%d', CAST(TRADDATE AS STRING)) AS trad_date_clean,
      CURRENT_TIMESTAMP() AS created_at
    FROM `{project}.{dataset}.R2_cams_reports_external` AS t
    WHERE
      year  = EXTRACT(YEAR  FROM CURRENT_DATE())
      AND month = EXTRACT(MONTH FROM CURRENT_DATE())
      AND day   = EXTRACT(DAY   FROM CURRENT_DATE())
  ),
  hashed_data AS (
    SELECT *,
      FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        AMC_CODE, FOLIO_NO, PRODCODE, SCHEME, TRXNTYPE, TRXNNO, TRXNMODE,
        TRXNSTAT, USERCODE, USRTRXNO, TRADDATE, POSTDATE,
        CAST(PURPRICE AS NUMERIC), CAST(UNITS AS NUMERIC), CAST(AMOUNT AS NUMERIC),
        BROKCODE, SUBBROK, ALTFOLIO, TRXNSUBTYP, APPLICATIO, TRXN_NATUR,
        MICR_NO, SWFLAG, OLD_FOLIO, SEQ_NO, CAST(STT AS NUMERIC),
        SCANREFNO, PAN, SIPTRXNNO, CAST(STAMP_DUTY AS NUMERIC)
      ))) AS row_hash
    FROM normalized_data
  )
  SELECT * FROM hashed_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      AMC_CODE, FOLIO_NO, PRODCODE, SCHEME, TRXNTYPE, TRXNNO, TRXNMODE,
      TRXNSTAT, USERCODE, USRTRXNO, TRADDATE, POSTDATE,
      CAST(PURPRICE AS NUMERIC), CAST(UNITS AS NUMERIC), CAST(AMOUNT AS NUMERIC),
      BROKCODE, SUBBROK, ALTFOLIO, TRXNSUBTYP, APPLICATIO, TRXN_NATUR,
      MICR_NO, SWFLAG, OLD_FOLIO, SEQ_NO, CAST(STT AS NUMERIC),
      SCANREFNO, PAN, SIPTRXNNO, CAST(STAMP_DUTY AS NUMERIC)
    ORDER BY
      CASE WHEN trad_date_clean IS NOT NULL THEN 1 ELSE 0 END DESC,
      trad_date_clean DESC, TIME1 DESC
  ) = 1
) AS Source
ON Target.row_hash = Source.row_hash
WHEN NOT MATCHED THEN
  INSERT (
    AMC_CODE, FOLIO_NO, PRODCODE, SCHEME, INV_NAME, TRXNTYPE, TRXNNO,
    TRXNMODE, TRXNSTAT, USERCODE, USRTRXNO, TRADDATE, POSTDATE,
    PURPRICE, UNITS, AMOUNT, BROKCODE, SUBBROK, BROKPERC, BROKCOMM,
    ALTFOLIO, REP_DATE, TIME1, TRXNSUBTYP, APPLICATIO, TRXN_NATUR,
    TAX, TOTAL_TAX, TE_15H, MICR_NO, REMARKS, SWFLAG, OLD_FOLIO,
    SEQ_NO, REINVEST_F, MULT_BROK, STT, LOCATION, SCHEME_TYP,
    TAX_STATUS, LOAD, SCANREFNO, PAN, INV_IIN, TARG_SRC_S,
    TRXN_TYPE_, TICOB_TRTY, TICOB_TRNO, TICOB_POST, DP_ID,
    TRXN_CHARG, ELIGIB_AMT, SRC_OF_TXN, TRXN_SUFFI, SIPTRXNNO,
    TER_LOCATI, EUIN, EUIN_VALID, EUIN_OPTED, SUB_BRK_AR,
    EXCH_DC_FL, SRC_BRK_CO, SYS_REGN_D, AC_NO, BANK_NAME,
    REVERSAL_C, EXCHANGE_F, CA_INITIAT, GST_STATE_, IGST_AMOUN,
    CGST_AMOUN, SGST_AMOUN, REV_REMARK, ORIGINAL_T, STAMP_DUTY,
    FOLIO_OLD, SCHEME_FOL, AMC_REF_NO, REQUEST_RE, TRANSMISSI,
    EXTRA_COLUMN1, EXTRA_COLUMN2, EXTRA_COLUMN3, EXTRA_COLUMN4, EXTRA_COLUMN5,
    trad_date, row_hash, created_at
  )
  VALUES (
    Source.AMC_CODE, Source.FOLIO_NO, Source.PRODCODE, Source.SCHEME,
    Source.INV_NAME, Source.TRXNTYPE, Source.TRXNNO, Source.TRXNMODE,
    Source.TRXNSTAT, Source.USERCODE, Source.USRTRXNO, Source.TRADDATE,
    Source.POSTDATE,
    CAST(Source.PURPRICE AS NUMERIC), CAST(Source.UNITS AS NUMERIC),
    CAST(Source.AMOUNT AS NUMERIC),
    Source.BROKCODE, Source.SUBBROK,
    CAST(Source.BROKPERC AS NUMERIC), CAST(Source.BROKCOMM AS NUMERIC),
    Source.ALTFOLIO, Source.REP_DATE, Source.TIME1, Source.TRXNSUBTYP,
    Source.APPLICATIO, Source.TRXN_NATUR,
    CAST(Source.TAX AS NUMERIC), CAST(Source.TOTAL_TAX AS NUMERIC),
    Source.TE_15H, Source.MICR_NO, Source.REMARKS, Source.SWFLAG,
    Source.OLD_FOLIO, Source.SEQ_NO, Source.REINVEST_F, Source.MULT_BROK,
    CAST(Source.STT AS NUMERIC),
    Source.LOCATION, Source.SCHEME_TYP, Source.TAX_STATUS,
    CAST(Source.LOAD AS NUMERIC),
    Source.SCANREFNO, Source.PAN, Source.INV_IIN, Source.TARG_SRC_S,
    Source.TRXN_TYPE_, Source.TICOB_TRTY, Source.TICOB_TRNO,
    Source.TICOB_POST, Source.DP_ID,
    CAST(Source.TRXN_CHARG AS NUMERIC), CAST(Source.ELIGIB_AMT AS NUMERIC),
    Source.SRC_OF_TXN, Source.TRXN_SUFFI, Source.SIPTRXNNO,
    Source.TER_LOCATI, Source.EUIN, Source.EUIN_VALID, Source.EUIN_OPTED,
    Source.SUB_BRK_AR, Source.EXCH_DC_FL, Source.SRC_BRK_CO,
    Source.SYS_REGN_D, Source.AC_NO, Source.BANK_NAME, Source.REVERSAL_C,
    Source.EXCHANGE_F, Source.CA_INITIAT, Source.GST_STATE_,
    CAST(Source.IGST_AMOUN AS NUMERIC), CAST(Source.CGST_AMOUN AS NUMERIC),
    CAST(Source.SGST_AMOUN AS NUMERIC),
    Source.REV_REMARK, Source.ORIGINAL_T, CAST(Source.STAMP_DUTY AS NUMERIC),
    Source.FOLIO_OLD, Source.SCHEME_FOL, Source.AMC_REF_NO,
    Source.REQUEST_RE, Source.TRANSMISSI,
    Source.EXTRA_COLUMN1, Source.EXTRA_COLUMN2, Source.EXTRA_COLUMN3,
    Source.EXTRA_COLUMN4, Source.EXTRA_COLUMN5,
    Source.trad_date_clean, Source.row_hash, Source.created_at
  )
"""
