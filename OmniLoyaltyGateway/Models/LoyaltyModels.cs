namespace OmniLoyaltyGateway.Models;

public class MemberTierInfo
{
    public string MemberAccountCode { get; set; } = string.Empty;
    public string GlobalRankingTier { get; set; } = string.Empty;
    public decimal AccumulatedSpendAmount { get; set; }
    public int AvailableBonusPoints { get; set; }
    public DateTime MilestoneReviewDate { get; set; }
}

public class PromotionAssignment
{
    public Guid EventAssignmentId { get; set; }
    public string MemberAccountCode { get; set; } = string.Empty;
    public string AppliedPromotionCode { get; set; } = string.Empty;
    public decimal DiscountCalculatedPercentage { get; set; }
    public DateTime ActiveWindowExpiresAt { get; set; }
}

public class CrossBrandSweptRewards
{
    public string InternalMemberHash { get; set; } = string.Empty;
    public string GlobalRetailBrandPartnerName { get; set; } = string.Empty;
    public decimal CrossSweepReconciledRebate { get; set; }
    public DateTime TransactedSystemReference { get; set; }
}
