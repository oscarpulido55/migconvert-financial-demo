using Microsoft.AspNetCore.Mvc;
using StorefrontCheckoutServices.DataAccess;
using StorefrontCheckoutServices.Models;

namespace StorefrontCheckoutServices.Controllers;

[ApiController]
[Route("api/checkout")]
public class CheckoutController : ControllerBase
{
    private readonly StorefrontRepository _repository;
    private readonly ILogger<CheckoutController> _logger;

    public CheckoutController(StorefrontRepository repository, ILogger<CheckoutController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet("history/{customerId}")]
    public async Task<IActionResult> GetOrderHistory(string customerId)
    {
        try
        {
            var orders = await _repository.GetComprehensiveOrderHistoryAsync(customerId);
            return Ok(orders);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving order history for {CustomerId}", customerId);
            return StatusCode(500, new { Message = "Internal system error occurred while contacting enterprise databases." });
        }
    }

    [HttpPost("submit")]
    public async Task<IActionResult> SubmitOrder([FromBody] Order order)
    {
        try
        {
            order.OrderId = Guid.NewGuid();
            order.CreatedTimestamp = DateTime.UtcNow;
            await _repository.CreateOrderRecordAsync(order);
            return Created($"/api/checkout/orders/{order.OrderId}", order);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating order for {CustomerId}", order.CustomerId);
            return StatusCode(500, new { Message = "Internal system error occurred while generating order." });
        }
    }

    [HttpGet("cart/{sessionId}/{regionCode}")]
    public async Task<IActionResult> GetCart(string sessionId, string regionCode)
    {
        try
        {
            var cartItems = await _repository.GetActiveUserCartAsync(sessionId, regionCode);
            return Ok(cartItems);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching cart items for session {SessionId}", sessionId);
            return StatusCode(500, new { Message = "Internal system error fetching session cart details." });
        }
    }

    [HttpGet("inventory/{warehouseId}/{skuCode}")]
    public async Task<IActionResult> CheckStock(string warehouseId, string skuCode)
    {
        try
        {
            var stock = await _repository.CheckItemStockAvailabilityAsync(skuCode, warehouseId);
            if (stock == null) return NotFound(new { Message = "Item status not found in designated regional node." });
            return Ok(stock);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking stock for {SkuCode}", skuCode);
            return StatusCode(500, new { Message = "Internal system error retrieving warehouse metrics." });
        }
    }

    [HttpPost("finalize/{customerId}/{sessionId}")]
    public async Task<IActionResult> FinalizeCheckoutTransaction(string customerId, string sessionId)
    {
        try
        {
            var success = await _repository.ProcessCustomerCheckoutCompletionAsync(customerId, sessionId);
            return Ok(new { Success = success, TransactedTimestamp = DateTime.UtcNow });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error finalizing checkout for {CustomerId}", customerId);
            return StatusCode(500, new { Message = "Internal system error closing session transaction records." });
        }
    }
}
