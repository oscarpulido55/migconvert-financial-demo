using System.Data;
using System.Data.SqlClient;
using Dapper;
using StorefrontCheckoutServices.Models;

namespace StorefrontCheckoutServices.DataAccess;

public class StorefrontRepository
{
    private readonly string _ordersConnString;
    private readonly string _customersConnString;
    private readonly string _inventoryConnString;

    public StorefrontRepository(IConfiguration configuration)
    {
        _ordersConnString = configuration.GetConnectionString("OrdersDatabase") 
            ?? throw new InvalidOperationException("OrdersDatabase connection string missing.");
        _customersConnString = configuration.GetConnectionString("CustomersDatabase") 
            ?? throw new InvalidOperationException("CustomersDatabase connection string missing.");
        _inventoryConnString = configuration.GetConnectionString("InventoryDatabase") 
            ?? throw new InvalidOperationException("InventoryDatabase connection string missing.");
    }

    // Functionality 1: Cross-Database Join Query using Dapper (Hardcoded FQN)
    public async Task<IEnumerable<Order>> GetComprehensiveOrderHistoryAsync(string customerId)
    {
        // Demonstrating cross-database inner join across OmniChannelSales_Production and CustomerIdentityCorp_Main
        const string query = @"
            SELECT 
                ord.[OrderInternalId] AS OrderId,
                ord.[SubCustomerId] AS CustomerId,
                cust.[BillingFullName] AS CustomerName,
                ord.[ResolutionStatus] AS OrderStatus,
                ord.[TotalCalculatedSum] AS TotalAmount,
                ord.[TransactionExecutionDate] AS CreatedTimestamp,
                ord.[GatewayPaymentCode] AS TransactionGatewayCode
            FROM [OmniChannelSales_Production].[dbo].[GlobalOrdersRegistry] ord
            INNER JOIN [CustomerIdentityCorp_Main].[sales].[VerifiedMasterCustomerAccounts] cust
                ON ord.[SubCustomerId] = cust.[EnterpriseCustomerUniqueId]
            WHERE ord.[SubCustomerId] = @CustomerId
            ORDER BY ord.[TransactionExecutionDate] DESC;";

        using var connection = new SqlConnection(_ordersConnString);
        return await connection.QueryAsync<Order>(query, new { CustomerId = customerId });
    }

    // Functionality 2: Insert Order using standard System.Data.SqlClient (Hardcoded FQN)
    public async Task<int> CreateOrderRecordAsync(Order order)
    {
        const string query = @"
            INSERT INTO [OmniChannelSales_Production].[sales].[GlobalOrdersRegistry] 
                ([OrderInternalId], [SubCustomerId], [ResolutionStatus], [TotalCalculatedSum], [TransactionExecutionDate], [GatewayPaymentCode]) 
            VALUES 
                (@OrderId, @CustomerId, @OrderStatus, @TotalAmount, @CreatedTimestamp, @GatewayCode);";

        using var connection = new SqlConnection(_ordersConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@OrderId", SqlDbType.UniqueIdentifier) { Value = order.OrderId });
        command.Parameters.Add(new SqlParameter("@CustomerId", SqlDbType.NVarChar, 100) { Value = order.CustomerId });
        command.Parameters.Add(new SqlParameter("@OrderStatus", SqlDbType.NVarChar, 50) { Value = order.OrderStatus });
        command.Parameters.Add(new SqlParameter("@TotalAmount", SqlDbType.Decimal) { Value = order.TotalAmount });
        command.Parameters.Add(new SqlParameter("@CreatedTimestamp", SqlDbType.DateTime2) { Value = order.CreatedTimestamp });
        command.Parameters.Add(new SqlParameter("@GatewayCode", SqlDbType.NVarChar, 50) { Value = order.TransactionGatewayCode });

        return await command.ExecuteNonQueryAsync();
    }

    // Functionality 3: Parameterized/Dynamic Table FQN Query using Dapper
    public async Task<IEnumerable<CartItem>> GetActiveUserCartAsync(string sessionId, string regionEnvironmentCode)
    {
        // Demonstrating string formatting for table FQN along with standard Dapper parameters for user inputs
        // Safe FQN building based on strict lookup mapping
        string databaseFqn = regionEnvironmentCode == "EMEA" 
            ? "[OmniChannelSales_Production].[eu_central].[CartWorkRecords]"
            : "[OmniChannelSales_Production].[na_east].[CartWorkRecords]";

        string query = $@"
            SELECT 
                [RecordIdentifier] AS CartItemId,
                [CustomerSessionKey] AS SessionId,
                [MerchandiseSku] AS SkuCode,
                [PackageRequestedCount] AS Quantity,
                [CalculatedSubPrice] AS UnitPrice
            FROM {databaseFqn}
            WHERE [CustomerSessionKey] = @SessionId;";

        using var connection = new SqlConnection(_ordersConnString);
        return await connection.QueryAsync<CartItem>(query, new { SessionId = sessionId });
    }

    // Functionality 4: Multi-Database verification check using System.Data.SqlClient
    public async Task<InventoryStatus?> CheckItemStockAvailabilityAsync(string skuCode, string warehouseId)
    {
        const string query = @"
            SELECT 
                inv.[UniversalItemIdentifier] AS SkuCode,
                inv.[RegionalLogisticsNode] AS WarehouseIdentifier,
                inv.[AggregatedStoreShelfStock] AS QuantityOnHand,
                inv.[CustomerReservedCount] AS AllocatedQuantity,
                inv.[FulfillmentEligibleFlag] AS IsReservable
            FROM [GlobalSupplyChainPlatform].[logistics].[MasterStorageInventory] inv
            WHERE inv.[UniversalItemIdentifier] = @SkuCode 
              AND inv.[RegionalLogisticsNode] = @WarehouseId;";

        using var connection = new SqlConnection(_inventoryConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@SkuCode", SqlDbType.NVarChar, 50) { Value = skuCode });
        command.Parameters.Add(new SqlParameter("@WarehouseId", SqlDbType.NVarChar, 50) { Value = warehouseId });

        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new InventoryStatus
            {
                SkuCode = reader.GetString(reader.GetOrdinal("SkuCode")),
                WarehouseIdentifier = reader.GetString(reader.GetOrdinal("WarehouseIdentifier")),
                QuantityOnHand = reader.GetInt32(reader.GetOrdinal("QuantityOnHand")),
                AllocatedQuantity = reader.GetInt32(reader.GetOrdinal("AllocatedQuantity")),
                IsReservable = reader.GetBoolean(reader.GetOrdinal("IsReservable"))
            };
        }

        return null;
    }

    // Functionality 5: Execute multiple updates across DB transactions using Dapper
    public async Task<bool> ProcessCustomerCheckoutCompletionAsync(string customerId, string sessionId)
    {
        const string clearCartQuery = @"
            DELETE FROM [OmniChannelSales_Production].[na_east].[CartWorkRecords]
            WHERE [CustomerSessionKey] = @SessionId;";

        const string updateCustomerStatusQuery = @"
            UPDATE [CustomerIdentityCorp_Main].[sales].[VerifiedMasterCustomerAccounts]
            SET [RecentPurchasePerformed] = 1,
                [LifecycleEventUpdated] = GETUTCDATE()
            WHERE [EnterpriseCustomerUniqueId] = @CustomerId;";

        using (var orderConnection = new SqlConnection(_ordersConnString))
        {
            await orderConnection.ExecuteAsync(clearCartQuery, new { SessionId = sessionId });
        }

        using (var customerConnection = new SqlConnection(_customersConnString))
        {
            await customerConnection.ExecuteAsync(updateCustomerStatusQuery, new { CustomerId = customerId });
        }

        return true;
    }
}
