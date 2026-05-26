namespace FulfillmentRoutingManagement.Models;

public class ShipmentDispatch
{
    public Guid TrackingIdentifier { get; set; }
    public Guid AssociatedOrderUniqueId { get; set; }
    public string AssignedCourierCode { get; set; } = string.Empty;
    public string RoutePathHash { get; set; } = string.Empty;
    public string DispatchResolutionState { get; set; } = string.Empty;
    public DateTime EstimatedTransitCompleteDate { get; set; }
    public string RecipientLocality { get; set; } = string.Empty;
}

public class WarehouseReplenishmentRequest
{
    public string SupplierUniqueCode { get; set; } = string.Empty;
    public string MerchandiseItemCatalogNumber { get; set; } = string.Empty;
    public int PendingConsignmentVolume { get; set; }
    public DateTime ReplenishmentScheduleDate { get; set; }
}

public class OperationsLogisticsMetric
{
    public string OperationsHubCode { get; set; } = string.Empty;
    public decimal HubOnTimeDispatchIndex { get; set; }
    public int ConcurrentlyOperatingSortationBots { get; set; }
    public int ActiveUnresolvedFrictionNotices { get; set; }
}
