using Microsoft.AspNetCore.Mvc;
using EnterpriseAnalyticsPulse.DataAccess;
using EnterpriseAnalyticsPulse.Models;

namespace EnterpriseAnalyticsPulse.Controllers;

[ApiController]
[Route("api/analytics")]
public class AnalyticsController : ControllerBase
{
    private readonly AnalyticsRepository _repository;
    private readonly ILogger<AnalyticsController> _logger;

    public AnalyticsController(AnalyticsRepository repository, ILogger<AnalyticsController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet("sales-summaries/{fiscalQuarter}")]
    public async Task<IActionResult> GetSalesSummaries(string fiscalQuarter)
    {
        try
        {
            var summaries = await _repository.GetRegionalSalesSummariesAsync(fiscalQuarter);
            return Ok(summaries);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching regional sales breakdown for {Quarter}", fiscalQuarter);
            return StatusCode(500, new { Message = "Analytical data warehouse aggregate processing exception." });
        }
    }

    [HttpGet("staff-performance/{storeCode}")]
    public async Task<IActionResult> ViewStaffSalesBreakdown(string storeCode)
    {
        try
        {
            var assessments = await _repository.AssessStaffInStorePerformanceAsync(storeCode);
            return Ok(assessments);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error running multi database inner join across sales and HR structures.");
            return StatusCode(500, new { Message = "Enterprise cross relational database join evaluation timeout." });
        }
    }

    [HttpGet("returns-intake/{divisionCode}")]
    public async Task<IActionResult> CheckDefectiveIntakeMetrics(string divisionCode, [FromQuery] DateTime minDate)
    {
        try
        {
            int returnCount = await _repository.ExtractReconciledReturnsMetricAsync(divisionCode, minDate);
            return Ok(new { Division = divisionCode, AnalyzedEarliest = minDate, FlaggedReturns = returnCount });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error querying dynamic table FQN returns logs.");
            return StatusCode(500, new { Message = "Secure data ingestion dynamic querying exception." });
        }
    }

    [HttpPost("price-correction")]
    public async Task<IActionResult> TriggerSimulatedPriceCorrections([FromBody] ProductPriceCorrectionRecord correction)
    {
        try
        {
            correction.CorrectionBatchToken = Guid.NewGuid();
            await _repository.StoreCalculatedPriceVariancesAsync(correction);
            return Created($"/api/analytics/corrections/{correction.CorrectionBatchToken}", correction);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error storing batch simulated price correction models.");
            return StatusCode(500, new { Message = "Analytics master catalog write transaction failed." });
        }
    }

    [HttpGet("audit-employee/{empNumber}")]
    public async Task<IActionResult> InspectEmploymentStatus(string empNumber)
    {
        try
        {
            bool isActive = await _repository.VerifyCorporateWorkforceStatusAsync(empNumber);
            return Ok(new { UniversalId = empNumber, VerifiedActive = isActive, RetrievedTimestamp = DateTime.UtcNow });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error verifying personnel records against corporate HR databases.");
            return StatusCode(500, new { Message = "Secure payroll active directory lookup failed." });
        }
    }
}
