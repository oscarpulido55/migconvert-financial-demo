using System.Data;
using System.Data.SqlClient;
using Dapper;
using FulfillmentRoutingManagement.Models;

namespace FulfillmentRoutingManagement.DataAccess;

public class FulfillmentRepository
{
    private readonly string _logisticsConnString;
    private readonly string _partnerConnString;
    private readonly string _salesConnString;

    public FulfillmentRepository(IConfiguration configuration)
    {
        _logisticsConnString = configuration.GetConnectionString("LogisticsMasterDatabase")
            ?? throw new InvalidOperationException("LogisticsMasterDatabase connection string missing.");
        _partnerConnString = configuration.GetConnectionString("PartnerSyncDatabase")
            ?? throw new InvalidOperationException("PartnerSyncDatabase connection string missing.");
        _salesConnString = configuration.GetConnectionString("SalesOmniDatabase")
            ?? throw new InvalidOperationException("SalesOmniDatabase connection string missing.");
    }

    // Functionality 1: Dispatch Shipment using Dapper
    public async Task<int> CreateDispatchManifestAsync(ShipmentDispatch dispatch)
    {
        const string query = @"
            INSERT INTO [GlobalSupplyChainPlatform].[manifests].[ActiveDispatchTracking] 
                ([TrackingKey], [OrderAnchorId], [ForwarderAssignedId], [OptimizedRouteVector], [FulfillmentPhase], [PlannedDeliverDate], [DestinationTerritory])
            VALUES 
                (@TrackingIdentifier, @AssociatedOrderUniqueId, @AssignedCourierCode, @RoutePathHash, @DispatchResolutionState, @EstimatedTransitCompleteDate, @RecipientLocality);";

        using var connection = new SqlConnection(_logisticsConnString);
        return await connection.ExecuteAsync(query, dispatch);
    }

    // Functionality 2: Cross-Database Join across GlobalSupplyChainPlatform and OmniChannelSales_Production using SqlClient
    public async Task<IEnumerable<ShipmentDispatch>> GetInboundTransitRecordsAsync(string customerRegion)
    {
        const string query = @"
            SELECT 
                disp.[TrackingKey] AS TrackingIdentifier,
                disp.[OrderAnchorId] AS AssociatedOrderUniqueId,
                disp.[ForwarderAssignedId] AS AssignedCourierCode,
                disp.[OptimizedRouteVector] AS RoutePathHash,
                disp.[FulfillmentPhase] AS DispatchResolutionState,
                disp.[PlannedDeliverDate] AS EstimatedTransitCompleteDate,
                disp.[DestinationTerritory] AS RecipientLocality
            FROM [GlobalSupplyChainPlatform].[manifests].[ActiveDispatchTracking] disp
            INNER JOIN [OmniChannelSales_Production].[dbo].[GlobalOrdersRegistry] sales
                ON disp.[OrderAnchorId] = sales.[OrderInternalId]
            WHERE disp.[DestinationTerritory] = @TargetLocality 
              AND disp.[FulfillmentPhase] IN ('DISPATCHED', 'IN_TRANSIT');";

        using var connection = new SqlConnection(_logisticsConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@TargetLocality", SqlDbType.NVarChar, 100) { Value = customerRegion });

        using var reader = await command.ExecuteReaderAsync();
        var results = new List<ShipmentDispatch>();

        while (await reader.ReadAsync())
        {
            results.Add(new ShipmentDispatch
            {
                TrackingIdentifier = reader.GetGuid(reader.GetOrdinal("TrackingIdentifier")),
                AssociatedOrderUniqueId = reader.GetGuid(reader.GetOrdinal("AssociatedOrderUniqueId")),
                AssignedCourierCode = reader.GetString(reader.GetOrdinal("AssignedCourierCode")),
                RoutePathHash = reader.GetString(reader.GetOrdinal("RoutePathHash")),
                DispatchResolutionState = reader.GetString(reader.GetOrdinal("DispatchResolutionState")),
                EstimatedTransitCompleteDate = reader.GetDateTime(reader.GetOrdinal("EstimatedTransitCompleteDate")),
                RecipientLocality = reader.GetString(reader.GetOrdinal("RecipientLocality"))
            });
        }

        return results;
    }

    // Functionality 3: Parameterized Table FQN for Partner API data pull using Dapper
    public async Task<IEnumerable<WarehouseReplenishmentRequest>> ReconcileSupplierStockAsync(string partnerDomainCode)
    {
        string partnerTableFqn = partnerDomainCode == "FEDEX_CARGO"
            ? "[ThirdPartyLogisticsPartners_Sync].[carrier_fdx].[UpcomingCargoPallets]"
            : "[ThirdPartyLogisticsPartners_Sync].[carrier_generic].[PendingShipments]";

        string query = $@"
            SELECT 
                [ThirdPartyOrgReference] AS SupplierUniqueCode,
                [PackageStandardBarcode] AS MerchandiseItemCatalogNumber,
                [CartonsLoadedQuantity] AS PendingConsignmentVolume,
                [ScheduledDropoffTimestamp] AS ReplenishmentScheduleDate
            FROM {partnerTableFqn}
            WHERE [VerificationLockStatus] = @LockStatus;";

        using var connection = new SqlConnection(_partnerConnString);
        return await connection.QueryAsync<WarehouseReplenishmentRequest>(query, new { LockStatus = "CONFIRMED" });
    }

    // Functionality 4: Fetch Operations Logistics Metrics using Dapper
    public async Task<OperationsLogisticsMetric?> GetHubAnalyticsAsync(string operationsHubCode)
    {
        const string query = @"
            SELECT 
                [DistributionNodeIdentifier] AS OperationsHubCode,
                [SlaPerformanceMetricRatio] AS HubOnTimeDispatchIndex,
                [AutonomousGuidanceUnitsOnline] AS ConcurrentlyOperatingSortationBots,
                [TriggeredSystemDisruptionAlerts] AS ActiveUnresolvedFrictionNotices
            FROM [GlobalSupplyChainPlatform].[facility].[AutomationOperationsTelemetry]
            WHERE [DistributionNodeIdentifier] = @HubCode;";

        using var connection = new SqlConnection(_logisticsConnString);
        return await connection.QueryFirstOrDefaultAsync<OperationsLogisticsMetric>(query, new { HubCode = operationsHubCode });
    }

    // Functionality 5: Batch Status Updates using transaction and standard SqlClient
    public async Task<int> UpdateBatchShippingStatesAsync(string targetStatus, string currentStatus)
    {
        const string query = @"
            UPDATE [GlobalSupplyChainPlatform].[manifests].[ActiveDispatchTracking]
            SET [FulfillmentPhase] = @NewStatus
            WHERE [FulfillmentPhase] = @OldStatus;";

        using var connection = new SqlConnection(_logisticsConnString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();
        try
        {
            using var command = new SqlCommand(query, connection, transaction);
            command.Parameters.Add(new SqlParameter("@NewStatus", SqlDbType.NVarChar, 50) { Value = targetStatus });
            command.Parameters.Add(new SqlParameter("@OldStatus", SqlDbType.NVarChar, 50) { Value = currentStatus });

            int rowsAffected = await command.ExecuteNonQueryAsync();
            transaction.Commit();
            return rowsAffected;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
}
