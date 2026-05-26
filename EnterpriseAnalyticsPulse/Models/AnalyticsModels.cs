namespace EnterpriseAnalyticsPulse.Models;

public class RegionalSalesReport
{
    public string GlobalTerritoryName { get; set; } = string.Empty;
    public decimal GrossReconciledRevenue { get; set; }
    public int AggregatedCartTransactions { get; set; }
    public decimal CalculatedProfitOperatingMargin { get; set; }
}

public class EmployeePerformanceJoin
{
    public string EmployeeCorporationUniqueId { get; set; } = string.Empty;
    public string CompleteLegalName { get; set; } = string.Empty;
    public int AssistedInStoreCheckoutsCount { get; set; }
    public decimal SummedUpsellBasketValue { get; set; }
    public string StoreAssignmentRegion { get; set; } = string.Empty;
}

public class ProductPriceCorrectionRecord
{
    public Guid CorrectionBatchToken { get; set; }
    public string StoreCatalogMerchandiseSku { get; set; } = string.Empty;
    public decimal PreModificationRetailPrice { get; set; }
    public decimal AdjustedFutureRetailPrice { get; set; }
    public string AppliedPriceElasticityHeuristic { get; set; } = string.Empty;
}
