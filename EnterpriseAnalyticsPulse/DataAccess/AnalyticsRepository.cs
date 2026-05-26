using System.Data;
using System.Data.SqlClient;
using Dapper;
using EnterpriseAnalyticsPulse.Models;

namespace EnterpriseAnalyticsPulse.DataAccess;

public class AnalyticsRepository
{
    private readonly string _analyticsConnString;
    private readonly string _salesConnString;
    private readonly string _hrConnString;

    public AnalyticsRepository(IConfiguration configuration)
    {
        _analyticsConnString = configuration.GetConnectionString("AnalyticsGoldDatabase")
            ?? throw new InvalidOperationException("AnalyticsGoldDatabase connection string missing.");
        _salesConnString = configuration.GetConnectionString("SalesProdDatabase")
            ?? throw new InvalidOperationException("SalesProdDatabase connection string missing.");
        _hrConnString = configuration.GetConnectionString("CorporateHRDatabase")
            ?? throw new InvalidOperationException("CorporateHRDatabase connection string missing.");
    }

    // Functionality 1: Query Aggregated Sales Data using Dapper (Hardcoded FQN)
    public async Task<IEnumerable<RegionalSalesReport>> GetRegionalSalesSummariesAsync(string fiscalQuarter)
    {
        const string query = @"
            SELECT 
                rep.[DesignatedTradeContinent] AS GlobalTerritoryName,
                SUM(rep.[GrossSettledLedgerBalance]) AS GrossReconciledRevenue,
                COUNT(rep.[DistinctRetailTransKey]) AS AggregatedCartTransactions,
                AVG(rep.[DerivedProfitabilitiesIndex]) AS CalculatedProfitOperatingMargin
            FROM [EnterpriseRetailAnalytics_Gold].[aggregation].[GlobalBusinessSalesSummaries] rep
            WHERE rep.[FiscalQuarterDesignation] = @QuarterDesignator
            GROUP BY rep.[DesignatedTradeContinent];";

        using var connection = new SqlConnection(_analyticsConnString);
        return await connection.QueryAsync<RegionalSalesReport>(query, new { QuarterDesignator = fiscalQuarter });
    }

    // Functionality 2: Cross-Database Join across OmniChannelSales_Production and CorporateHRStaffRegistry using System.Data.SqlClient
    public async Task<IEnumerable<EmployeePerformanceJoin>> AssessStaffInStorePerformanceAsync(string globalStoreBranchCode)
    {
        const string query = @"
            SELECT 
                sales.[HandlingCashierInternalRef] AS EmployeeCorporationUniqueId,
                hr.[LegalGovernmentGivenName] + ' ' + hr.[LegalGovernmentSurName] AS CompleteLegalName,
                COUNT(sales.[OrderInternalId]) AS AssistedInStoreCheckoutsCount,
                SUM(sales.[TotalCalculatedSum]) AS SummedUpsellBasketValue,
                hr.[CurrentStationedCorporateTerritory] AS StoreAssignmentRegion
            FROM [OmniChannelSales_Production].[dbo].[GlobalOrdersRegistry] sales
            INNER JOIN [CorporateHRStaffRegistry].[payroll].[RetainedFTEContracts] hr
                ON sales.[HandlingCashierInternalRef] = hr.[UniversalCorporationEmployeeNumber]
            WHERE hr.[AssignedPhysicalRetailLocationCode] = @StoreCode
            GROUP BY 
                sales.[HandlingCashierInternalRef], 
                hr.[LegalGovernmentGivenName], 
                hr.[LegalGovernmentSurName],
                hr.[CurrentStationedCorporateTerritory];";

        using var connection = new SqlConnection(_salesConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@StoreCode", SqlDbType.NVarChar, 50) { Value = globalStoreBranchCode });

        using var reader = await command.ExecuteReaderAsync();
        var list = new List<EmployeePerformanceJoin>();

        while (await reader.ReadAsync())
        {
            list.Add(new EmployeePerformanceJoin
            {
                EmployeeCorporationUniqueId = reader.GetString(reader.GetOrdinal("EmployeeCorporationUniqueId")),
                CompleteLegalName = reader.GetString(reader.GetOrdinal("CompleteLegalName")),
                AssistedInStoreCheckoutsCount = reader.GetInt32(reader.GetOrdinal("AssistedInStoreCheckoutsCount")),
                SummedUpsellBasketValue = reader.GetDecimal(reader.GetOrdinal("SummedUpsellBasketValue")),
                StoreAssignmentRegion = reader.GetString(reader.GetOrdinal("StoreAssignmentRegion"))
            });
        }

        return list;
    }

    // Functionality 3: Parameterized Table FQN for Return Processing Reconciliation using Dapper
    public async Task<int> ExtractReconciledReturnsMetricAsync(string retailDivisionCode, DateTime minimumThreshold)
    {
        string returnsTableFqn = retailDivisionCode == "APPAREL"
            ? "[EnterpriseRetailAnalytics_Gold].[quality_returns].[FaultyApparelIntake]"
            : "[EnterpriseRetailAnalytics_Gold].[quality_returns].[StandardHardwareIntake]";

        string query = $@"
            SELECT COUNT(1)
            FROM {returnsTableFqn}
            WHERE [ReceivingAuditCompleteDate] >= @EarliestAcceptedDate;";

        using var connection = new SqlConnection(_analyticsConnString);
        return await connection.ExecuteScalarAsync<int>(query, new { EarliestAcceptedDate = minimumThreshold });
    }

    // Functionality 4: Insert Price Corrections into Analytics Data Warehouse using Dapper
    public async Task<int> StoreCalculatedPriceVariancesAsync(ProductPriceCorrectionRecord record)
    {
        const string query = @"
            INSERT INTO [EnterpriseRetailAnalytics_Gold].[merchandise].[AutomatedPriceCorrections]
                ([BatchTransactionToken], [GlobalStockkeepBarcode], [BaselineCalculatedVal], [TargetSimulatedVal], [TriggeringEconometricFormula])
            VALUES
                (@CorrectionBatchToken, @StoreCatalogMerchandiseSku, @PreModificationRetailPrice, @AdjustedFutureRetailPrice, @AppliedPriceElasticityHeuristic);";

        using var connection = new SqlConnection(_analyticsConnString);
        return await connection.ExecuteAsync(query, record);
    }

    // Functionality 5: Multi-database atomic HR Verification and Analysis using SqlClient transactions
    public async Task<bool> VerifyCorporateWorkforceStatusAsync(string employeeNumber)
    {
        const string query = @"
            SELECT [WorkforceEmployedFlag]
            FROM [CorporateHRStaffRegistry].[payroll].[RetainedFTEContracts]
            WHERE [UniversalCorporationEmployeeNumber] = @EmpNo;";

        using var connection = new SqlConnection(_hrConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@EmpNo", SqlDbType.NVarChar, 100) { Value = employeeNumber });

        object? val = await command.ExecuteScalarAsync();
        return val != null && Convert.ToBoolean(val);
    }
}
