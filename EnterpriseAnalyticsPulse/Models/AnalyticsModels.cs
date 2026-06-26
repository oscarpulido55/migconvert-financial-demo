class RegionalSalesReport:
    GlobalTerritoryName: str = ""
    GrossReconciledRevenue: float = 0.0
    AggregatedCartTransactions: int = 0
    CalculatedProfitOperatingMargin: float = 0.0

class EmployeePerformanceJoin:
    EmployeeCorporationUniqueId: str = ""
    CompleteLegalName: str = ""
    AssistedInStoreCheckoutsCount: int = 0
    SummedUpsellBasketValue: float = 0.0
    StoreAssignmentRegion: str = ""

class ProductPriceCorrectionRecord:
    CorrectionBatchToken: str = ""
    StoreCatalogMerchandiseSku: str = ""
    PreModificationRetailPrice: float = 0.0
    AdjustedFutureRetailPrice: float = 0.0
    AppliedPriceElasticityHeuristic: str = ""
