namespace StorefrontCheckoutServices.Models;

public class Order
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public string OrderStatus { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public DateTime CreatedTimestamp { get; set; }
    public string TransactionGatewayCode { get; set; } = string.Empty;
}

public class CartItem
{
    public Guid CartItemId { get; set; }
    public string SessionId { get; set; } = string.Empty;
    public string SkuCode { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class InventoryStatus
{
    public string SkuCode { get; set; } = string.Empty;
    public string WarehouseIdentifier { get; set; } = string.Empty;
    public int QuantityOnHand { get; set; }
    public int AllocatedQuantity { get; set; }
    public bool IsReservable { get; set; }
}
