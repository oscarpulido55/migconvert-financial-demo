import os
from datetime import datetime
from typing import List, Dict, Any, Union
from google.cloud import bigquery

class AnalyticsRepository:
    _analytics_project_id: str
    _sales_project_id: str
    _hr_project_id: str
    _bq_client: bigquery.Client

    def __init__(self, configuration):
        self._analytics_project_id = configuration.get_connection_string("AnalyticsGoldDatabase")
        if not self._analytics_project_id:
            raise ValueError("AnalyticsGoldDatabase project ID missing.")

        self._sales_project_id = configuration.get_connection_string("SalesProdDatabase")
        if not self._sales_project_id:
            raise ValueError("SalesProdDatabase project ID missing.")

        self._hr_project_id = configuration.get_connection_string("CorporateHRDatabase")
        if not self._hr_project_id:
            raise ValueError("CorporateHRDatabase project ID missing.")

        self._bq_client = bigquery.Client()

    async def get_regional_sales_summaries_async(self, fiscal_quarter: str) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                rep.DesignatedTradeContinent AS GlobalTerritoryName,
                SUM(rep.GrossSettledLedgerBalance) AS GrossReconciledRevenue,
                COUNT(rep.DistinctRetailTransKey) AS AggregatedCartTransactions,
                AVG(rep.DerivedProfitabilitiesIndex) AS CalculatedProfitOperatingMargin
            FROM `{self._analytics_project_id}.aggregation.GlobalBusinessSalesSummaries` AS rep
            WHERE rep.FiscalQuarterDesignation = @QuarterDesignator
            GROUP BY rep.DesignatedTradeContinent
        """
        query_job = self._bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("QuarterDesignator", "STRING", fiscal_quarter)
                ]
            )
        )
        return [dict(row) for row in query_job.result()]

    async def assess_staff_in_store_performance_async(self, global_store_branch_code: str) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                sales.HandlingCashierInternalRef AS EmployeeCorporationUniqueId,
                CONCAT(hr.LegalGovernmentGivenName, ' ', hr.LegalGovernmentSurName) AS CompleteLegalName,
                COUNT(sales.OrderInternalId) AS AssistedInStoreCheckoutsCount,
                SUM(sales.TotalCalculatedSum) AS SummedUpsellBasketValue,
                hr.CurrentStationedCorporateTerritory AS StoreAssignmentRegion
            FROM `{self._sales_project_id}.dbo.GlobalOrdersRegistry` AS sales
            INNER JOIN `{self._hr_project_id}.payroll.RetainedFTEContracts` AS hr
                ON sales.HandlingCashierInternalRef = hr.UniversalCorporationEmployeeNumber
            WHERE hr.AssignedPhysicalRetailLocationCode = @StoreCode
            GROUP BY
                sales.HandlingCashierInternalRef,
                hr.LegalGovernmentGivenName,
                hr.LegalGovernmentSurName,
                hr.CurrentStationedCorporateTerritory
        """
        query_job = self._bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("StoreCode", "STRING", global_store_branch_code)
                ]
            )
        )
        return [dict(row) for row in query_job.result()]

    async def extract_reconciled_returns_metric_async(self, retail_division_code: str, minimum_threshold: datetime) -> int:
        if retail_division_code == "APPAREL":
            returns_table_fqn = f"`{self._analytics_project_id}.quality_returns.FaultyApparelIntake`"
        else:
            returns_table_fqn = f"`{self._analytics_project_id}.quality_returns.StandardHardwareIntake`"

        query = f"""
            SELECT COUNT(1)
            FROM {returns_table_fqn}
            WHERE ReceivingAuditCompleteDate >= @EarliestAcceptedDate
        """
        query_job = self._bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("EarliestAcceptedDate", "TIMESTAMP", minimum_threshold)
                ]
            )
        )
        result_iterator = iter(query_job.result())
        first_row = next(result_iterator, None)
        if first_row:
            return first_row[0]
        return 0

    async def store_calculated_price_variances_async(self, record: Dict[str, Any]) -> int:
        query = f"""
            INSERT INTO `{self._analytics_project_id}.merchandise.AutomatedPriceCorrections`
                (BatchTransactionToken, GlobalStockkeepBarcode, BaselineCalculatedVal, TargetSimulatedVal, TriggeringEconometricFormula)
            VALUES
                (@CorrectionBatchToken, @StoreCatalogMerchandiseSku, @PreModificationRetailPrice, @AdjustedFutureRetailPrice, @AppliedPriceElasticityHeuristic)
        """
        query_job = self._bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("CorrectionBatchToken", "STRING", record.get("CorrectionBatchToken")),
                    bigquery.ScalarQueryParameter("StoreCatalogMerchandiseSku", "STRING", record.get("StoreCatalogMerchandiseSku")),
                    bigquery.ScalarQueryParameter("PreModificationRetailPrice", "FLOAT", record.get("PreModificationRetailPrice")),
                    bigquery.ScalarQueryParameter("AdjustedFutureRetailPrice", "FLOAT", record.get("AdjustedFutureRetailPrice")),
                    bigquery.ScalarQueryParameter("AppliedPriceElasticityHeuristic", "STRING", record.get("AppliedPriceElasticityHeuristic"))
                ]
            )
        )
        query_job.result()
        return query_job.num_dml_affected_rows

    async def verify_corporate_workforce_status_async(self, employee_number: str) -> bool:
        query = f"""
            SELECT WorkforceEmployedFlag
            FROM `{self._hr_project_id}.payroll.RetainedFTEContracts`
            WHERE UniversalCorporationEmployeeNumber = @EmpNo
        """
        query_job = self._bq_client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("EmpNo", "STRING", employee_number)
                ]
            )
        )
        result_iterator = iter(query_job.result())
        val_row = next(result_iterator, None)

        if val_row:
            return bool(val_row[0])
        return False
