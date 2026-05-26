using Microsoft.AspNetCore.Mvc;
using OmniLoyaltyGateway.DataAccess;
using OmniLoyaltyGateway.Models;

namespace OmniLoyaltyGateway.Controllers;

[ApiController]
[Route("api/loyalty")]
public class LoyaltyController : ControllerBase
{
    private readonly LoyaltyRepository _repository;
    private readonly ILogger<LoyaltyController> _logger;

    public LoyaltyController(LoyaltyRepository repository, ILogger<LoyaltyController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet("member/{accountCode}")]
    public async Task<IActionResult> GetMemberProfile(string accountCode)
    {
        try
        {
            var profile = await _repository.GetMemberTierInfoAsync(accountCode);
            if (profile == null) return NotFound(new { Message = "Member identity unverified or absent from tier tracking." });
            return Ok(profile);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching member tier info for {Account}", accountCode);
            return StatusCode(500, new { Message = "Loyalty system core communication error." });
        }
    }

    [HttpPost("assign-coupon")]
    public async Task<IActionResult> AssignRewardCoupon([FromBody] PromotionAssignment promotion)
    {
        try
        {
            promotion.EventAssignmentId = Guid.NewGuid();
            await _repository.InsertPromotionAssignmentAsync(promotion);
            return Created($"/api/loyalty/coupons/{promotion.EventAssignmentId}", promotion);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error assigning corporate promotion voucher.");
            return StatusCode(500, new { Message = "Failed saving promotion records into campaigns database." });
        }
    }

    [HttpGet("elite-promotions/{rankTier}")]
    public async Task<IActionResult> GetElitePromos(string rankTier)
    {
        try
        {
            var offers = await _repository.GetActivePromotionsForEliteMembersAsync(rankTier);
            return Ok(offers);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error evaluating elite promotion inner joins.");
            return StatusCode(500, new { Message = "Cross database evaluation join encountered transaction timeout." });
        }
    }

    [HttpGet("sweep-partner/{regionCode}")]
    public async Task<IActionResult> SweepPartnerRewards(string regionCode)
    {
        try
        {
            var rebates = await _repository.GatherCrossSweepRewardsAsync(regionCode);
            return Ok(rebates);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing finance dynamic FQN queries.");
            return StatusCode(500, new { Message = "Secure enterprise finance data warehouse communication failed." });
        }
    }

    [HttpPost("adjust/{accountCode}")]
    public async Task<IActionResult> UpdateBalance(string accountCode, [FromQuery] int pointAdjustment)
    {
        try
        {
            int updatedRows = await _repository.AdjustMemberRewardPointsAsync(accountCode, pointAdjustment);
            return Ok(new { RowsModified = updatedRows, CompletedAt = DateTime.UtcNow });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating member point balance.");
            return StatusCode(500, new { Message = "System atomic transaction failed to lock balance tracking rows." });
        }
    }
}
