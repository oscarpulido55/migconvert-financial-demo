using Microsoft.AspNetCore.Mvc;
using FulfillmentRoutingManagement.DataAccess;
using FulfillmentRoutingManagement.Models;

namespace FulfillmentRoutingManagement.Controllers;

[ApiController]
[Route("api/routing")]
public class RoutingController : ControllerBase
{
    private readonly FulfillmentRepository _repository;
    private readonly ILogger<RoutingController> _logger;

    public RoutingController(FulfillmentRepository repository, ILogger<RoutingController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpPost("dispatch")]
    public async Task<IActionResult> DispatchPackage([FromBody] ShipmentDispatch dispatch)
    {
        try
        {
            dispatch.TrackingIdentifier = Guid.NewGuid();
            await _repository.CreateDispatchManifestAsync(dispatch);
            return Created($"/api/routing/shipments/{dispatch.TrackingIdentifier}", dispatch);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating dispatch tracking entry.");
            return StatusCode(500, new { Message = "Enterprise logistics error creating tracking manifest." });
        }
    }

    [HttpGet("inbound/{regionName}")]
    public async Task<IActionResult> GetInboundShipments(string regionName)
    {
        try
        {
            var shipments = await _repository.GetInboundTransitRecordsAsync(regionName);
            return Ok(shipments);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching inbound shipments for territory {Region}", regionName);
            return StatusCode(500, new { Message = "Enterprise database sync failure across orders and logistics engines." });
        }
    }

    [HttpGet("partner-sync/{carrierDomain}")]
    public async Task<IActionResult> InspectPartnerConsignments(string carrierDomain)
    {
        try
        {
            var orders = await _repository.ReconcileSupplierStockAsync(carrierDomain);
            return Ok(orders);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching external partner sync stock info.");
            return StatusCode(500, new { Message = "Secure partner database sync gateway failed." });
        }
    }

    [HttpGet("metrics/{hubCode}")]
    public async Task<IActionResult> ViewFacilityMetrics(string hubCode)
    {
        try
        {
            var analytics = await _repository.GetHubAnalyticsAsync(hubCode);
            if (analytics == null) return NotFound(new { Message = "Hub code analytics not found." });
            return Ok(analytics);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching hub metrics for {Hub}", hubCode);
            return StatusCode(500, new { Message = "Automated facility system queries failed." });
        }
    }

    [HttpPut("phase-shift")]
    public async Task<IActionResult> ProcessBulkStatusEvolution([FromQuery] string targetPhase, [FromQuery] string sourcePhase)
    {
        try
        {
            int updated = await _repository.UpdateBatchShippingStatesAsync(targetPhase, sourcePhase);
            return Ok(new { RecordsEvolved = updated, ResolvedAt = DateTime.UtcNow });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error running bulk status state evolution.");
            return StatusCode(500, new { Message = "Transaction boundary failed during bulk state update." });
        }
    }
}
