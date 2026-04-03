"""
KFintech column mapping and BigQuery MERGE SQL.

CSV → BQ mapping
----------------
KFintech CSV headers use CamelCase (e.g. "Product_Code", "Folio_Number").
BigQuery target columns are lowercase_with_underscores.
CSV_TO_BQ defines the explicit mapping for every known column.

There are no overflow slots for KFintech — any unknown CSV column triggers
an alert email so the team can extend the schema deliberately.
"""

from src.processors.schema.mapper import ColumnMapper

# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

KFINTECH_BQ_COLUMNS: list[str] = [
    "product_code", "fund", "folio_number", "scheme_code", "fund_description",
    "transaction_head", "transaction_number", "switch_ref_no", "instrument_number",
    "investor_name", "transaction_mode", "transaction_status", "branch_name",
    "branch_transaction_no", "transaction_date", "process_date",
    "price", "units", "amount", "agent_code", "sub_broker_code",
    "brokerage_percentage", "commission", "investor_id", "report_date",
    "report_time", "transaction_sub", "application_number", "transaction_id",
    "transaction_description", "transaction_type", "instrument_date",
    "instrument_bank", "dividend_option", "transaction_flag", "nav", "stt",
    "load_amount", "ihno", "branch_code", "inward_number", "pan1", "remarks",
    "nav_date", "pan2", "pan3", "tds_amount", "sch1", "pln1", "to_product_code",
    "td_trxnmode", "client_id", "dp_id", "status", "rej_trno_org_no",
    "sub_tran_type", "tr_charges", "atm_card_status", "atm_card_remarks",
    "nct_change_date", "isin", "city_category", "port_date", "new_unqno",
    "euin", "sub_broker_arn_code", "euin_valid_indicator",
    "euin_declaration_indicator", "asset_type", "sip_regn_date", "scheme",
    "plan", "insure_amount", "agent_code_change_request_date", "div_per",
    "common_account_number", "exchange_org_tr_type", "electronic_transaction_flag",
    "sip_regn_slno", "cheque_clearnce", "investor_state", "stamp_duty_charges",
    "feed_type", "guard_pan_no",
    # Computed by BQ MERGE — never uploaded in CSV
    "txn_date", "row_hash", "created_at",
]

_COMPUTED = {"txn_date", "row_hash", "created_at"}

# CamelCase CSV header → lowercase BQ column name
KFINTECH_CSV_TO_BQ: dict[str, str] = {
    "Product_Code":                    "product_code",
    "Fund":                            "fund",
    "Folio_Number":                    "folio_number",
    "Scheme_Code":                     "scheme_code",
    "Fund_Description":                "fund_description",
    "Transaction_Head":                "transaction_head",
    "Transaction_Number":              "transaction_number",
    "Switch_Ref__No_":                 "switch_ref_no",
    "Instrument_Number":               "instrument_number",
    "Investor_Name":                   "investor_name",
    "Transaction_Mode":                "transaction_mode",
    "Transaction_Status":              "transaction_status",
    "Branch_Name":                     "branch_name",
    "Branch_Transaction_No":           "branch_transaction_no",
    "Transaction_Date":                "transaction_date",
    "Process_Date":                    "process_date",
    "Price":                           "price",
    "Units":                           "units",
    "Amount":                          "amount",
    "Agent_Code":                      "agent_code",
    "Sub_Broker_Code":                 "sub_broker_code",
    "Brokerage_Percentage":            "brokerage_percentage",
    "Commission":                      "commission",
    "Investor_ID":                     "investor_id",
    "Report_Date":                     "report_date",
    "Report_Time":                     "report_time",
    "Transaction_Sub":                 "transaction_sub",
    "Application_Number":              "application_number",
    "Transaction_ID":                  "transaction_id",
    "Transaction_Description":         "transaction_description",
    "Transaction_Type":                "transaction_type",
    "Instrument_Date":                 "instrument_date",
    "Instrument_Bank":                 "instrument_bank",
    "Dividend_Option":                 "dividend_option",
    "Transaction_Flag":                "transaction_flag",
    "Nav":                             "nav",
    "STT":                             "stt",
    "Load_Amount":                     "load_amount",
    "Ihno":                            "ihno",
    "Branch_Code":                     "branch_code",
    "Inward_Number":                   "inward_number",
    "PAN1":                            "pan1",
    "Remarks":                         "remarks",
    "Nav_Date":                        "nav_date",
    "PAN2":                            "pan2",
    "PAN3":                            "pan3",
    "TDSAmount":                       "tds_amount",
    "sch1":                            "sch1",
    "pln1":                            "pln1",
    "ToProductCode":                   "to_product_code",
    "td_trxnmode":                     "td_trxnmode",
    "ClientId":                        "client_id",
    "DpId":                            "dp_id",
    "Status":                          "status",
    "RejTrnoOrgNo":                    "rej_trno_org_no",
    "SubTranType":                     "sub_tran_type",
    "TrCharges":                       "tr_charges",
    "ATMCardStatus":                   "atm_card_status",
    "ATMCardRemarks":                  "atm_card_remarks",
    "NCT_Change_Date":                 "nct_change_date",
    "ISIN":                            "isin",
    "CityCategory":                    "city_category",
    "PortDate":                        "port_date",
    "NewUnqno":                        "new_unqno",
    "EUIN":                            "euin",
    "Sub_Broker_ARN_Code":             "sub_broker_arn_code",
    "EUIN_Valid_Indicator":            "euin_valid_indicator",
    "EUIN_Declaration_Indicator":      "euin_declaration_indicator",
    "AssetType":                       "asset_type",
    "SIP_Regn_Date":                   "sip_regn_date",
    "Scheme":                          "scheme",
    "Plan":                            "plan",
    "Insure_Amount":                   "insure_amount",
    "Agent_Code_Change_request_Date":  "agent_code_change_request_date",
    "DivPer":                          "div_per",
    "Common_Account_Number":           "common_account_number",
    "Exchange_OrgTrType":              "exchange_org_tr_type",
    "Electronic_transaction_Flag":     "electronic_transaction_flag",
    "sipregslno":                      "sip_regn_slno",
    "chequeclearnce":                  "cheque_clearnce",
    "InvestorState":                   "investor_state",
    "Stamp_Duty_Charges":              "stamp_duty_charges",
    "Feed_Type":                       "feed_type",
    "GuardPanNo":                      "guard_pan_no",
}


class KFintechColumnMapper(ColumnMapper):
    BQ_COLUMNS = KFINTECH_BQ_COLUMNS
    CSV_TO_BQ = KFINTECH_CSV_TO_BQ
    COMPUTED_COLUMNS = _COMPUTED
    OVERFLOW_COLUMNS = []   # No overflow — alert on ALL unknown columns


# ---------------------------------------------------------------------------
# BigQuery MERGE SQL
# ---------------------------------------------------------------------------
# Placeholders: {project}  {dataset}
# ---------------------------------------------------------------------------

KFINTECH_MERGE_SQL = """\
MERGE INTO `{project}.{dataset}.R2_kfintech_final_view_v5` AS Target
USING (
  WITH normalized_data AS (
    SELECT
      t.*,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(Transaction_Date AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(Transaction_Date AS STRING))
      ) AS transaction_date_clean,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(Process_Date AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(Process_Date AS STRING))
      ) AS process_date_clean,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(Report_Date AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(Report_Date AS STRING))
      ) AS report_date_clean,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(Instrument_Date AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(Instrument_Date AS STRING))
      ) AS instrument_date_clean,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(Nav_Date AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(Nav_Date AS STRING))
      ) AS nav_date_clean,
      COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(PortDate AS STRING)),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(PortDate AS STRING))
      ) AS port_date_clean,
      CURRENT_TIMESTAMP() AS created_at
    FROM `{project}.{dataset}.R2_kfintech_reports_external` AS t
    WHERE
      year  = EXTRACT(YEAR  FROM CURRENT_DATE())
      AND month = EXTRACT(MONTH FROM CURRENT_DATE())
      AND day   = EXTRACT(DAY   FROM CURRENT_DATE())
  ),
  hashed_data AS (
    SELECT *,
      FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        Folio_Number, Scheme_Code, Transaction_Number, Switch_Ref__No_,
        Instrument_Number,
        CAST(Price AS NUMERIC), CAST(Units AS NUMERIC), CAST(Amount AS NUMERIC),
        Transaction_ID, CAST(Nav AS NUMERIC), CAST(STT AS NUMERIC),
        CAST(Load_Amount AS NUMERIC), Ihno, Inward_Number, PAN1,
        CAST(TDSAmount AS NUMERIC), ISIN, NewUnqno,
        CAST(Stamp_Duty_Charges AS NUMERIC)
      ))) AS row_hash
    FROM normalized_data
  )
  SELECT * FROM hashed_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      Folio_Number, Scheme_Code, Transaction_Number, Switch_Ref__No_,
      Instrument_Number,
      CAST(Price AS NUMERIC), CAST(Units AS NUMERIC), CAST(Amount AS NUMERIC),
      Transaction_ID, CAST(Nav AS NUMERIC), CAST(STT AS NUMERIC),
      CAST(Load_Amount AS NUMERIC), Ihno, Inward_Number, PAN1,
      CAST(TDSAmount AS NUMERIC), ISIN, NewUnqno,
      CAST(Stamp_Duty_Charges AS NUMERIC)
    ORDER BY
      CASE WHEN transaction_date_clean IS NOT NULL THEN 1 ELSE 0 END DESC,
      transaction_date_clean DESC, Report_Time DESC
  ) = 1
) AS Source
ON Target.row_hash = Source.row_hash
WHEN NOT MATCHED THEN
  INSERT (
    product_code, fund, folio_number, scheme_code, fund_description,
    transaction_head, transaction_number, switch_ref_no, instrument_number,
    investor_name, transaction_mode, transaction_status, branch_name,
    branch_transaction_no, transaction_date, process_date,
    price, units, amount, agent_code, sub_broker_code,
    brokerage_percentage, commission, investor_id, report_date,
    report_time, transaction_sub, application_number, transaction_id,
    transaction_description, transaction_type, instrument_date,
    instrument_bank, dividend_option, transaction_flag, nav, stt,
    load_amount, ihno, branch_code, inward_number, pan1, remarks,
    nav_date, pan2, pan3, tds_amount, sch1, pln1, to_product_code,
    td_trxnmode, client_id, dp_id, status, rej_trno_org_no, sub_tran_type,
    tr_charges, atm_card_status, atm_card_remarks, nct_change_date, isin,
    city_category, port_date, new_unqno, euin, sub_broker_arn_code,
    euin_valid_indicator, euin_declaration_indicator, asset_type,
    sip_regn_date, scheme, plan, insure_amount,
    agent_code_change_request_date, div_per, common_account_number,
    exchange_org_tr_type, electronic_transaction_flag, sip_regn_slno,
    cheque_clearnce, investor_state, stamp_duty_charges,
    feed_type, guard_pan_no, txn_date, row_hash, created_at
  )
  VALUES (
    Source.Product_Code, Source.Fund, Source.Folio_Number, Source.Scheme_Code,
    Source.Fund_Description, Source.Transaction_Head, Source.Transaction_Number,
    Source.Switch_Ref__No_, Source.Instrument_Number, Source.Investor_Name,
    Source.Transaction_Mode, Source.Transaction_Status, Source.Branch_Name,
    Source.Branch_Transaction_No,
    Source.transaction_date_clean, Source.process_date_clean,
    CAST(Source.Price AS NUMERIC), CAST(Source.Units AS NUMERIC),
    CAST(Source.Amount AS NUMERIC),
    Source.Agent_Code, Source.Sub_Broker_Code, Source.Brokerage_Percentage,
    Source.Commission, Source.Investor_ID,
    Source.report_date_clean, Source.Report_Time,
    Source.Transaction_Sub, Source.Application_Number, Source.Transaction_ID,
    Source.Transaction_Description, Source.Transaction_Type,
    Source.instrument_date_clean, Source.Instrument_Bank,
    Source.Dividend_Option, Source.Transaction_Flag,
    CAST(Source.Nav AS NUMERIC), CAST(Source.STT AS NUMERIC),
    CAST(Source.Load_Amount AS NUMERIC),
    Source.Ihno, Source.Branch_Code, Source.Inward_Number, Source.PAN1,
    Source.Remarks, Source.nav_date_clean, Source.PAN2, Source.PAN3,
    CAST(Source.TDSAmount AS NUMERIC),
    Source.sch1, Source.pln1, Source.ToProductCode, Source.td_trxnmode,
    Source.ClientId, Source.DpId, Source.Status, Source.RejTrnoOrgNo,
    Source.SubTranType, Source.TrCharges, Source.ATMCardStatus,
    Source.ATMCardRemarks, Source.NCT_Change_Date, Source.ISIN,
    Source.CityCategory, Source.port_date_clean, Source.NewUnqno,
    Source.EUIN, Source.Sub_Broker_ARN_Code, Source.EUIN_Valid_Indicator,
    Source.EUIN_Declaration_Indicator, Source.AssetType, Source.SIP_Regn_Date,
    Source.Scheme, Source.Plan, Source.Insure_Amount,
    Source.Agent_Code_Change_request_Date, Source.DivPer,
    Source.Common_Account_Number, Source.Exchange_OrgTrType,
    Source.Electronic_transaction_Flag, Source.sipregslno,
    Source.chequeclearnce, Source.InvestorState,
    CAST(Source.Stamp_Duty_Charges AS NUMERIC),
    Source.Feed_Type, Source.GuardPanNo,
    Source.transaction_date_clean,  -- txn_date
    Source.row_hash, Source.created_at
  )
"""
