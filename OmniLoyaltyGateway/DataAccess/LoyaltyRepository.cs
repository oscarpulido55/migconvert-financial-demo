using System.Data;
using System.Data.SqlClient;
using Dapper;
using OmniLoyaltyGateway.Models;

namespace OmniLoyaltyGateway.DataAccess;

public class LoyaltyRepository
{
    private readonly string _loyaltyConnString;
    private readonly string _promotionsConnString;
    private readonly string _financeConnString;

    public LoyaltyRepository(IConfiguration configuration)
    {
        _loyaltyConnString = configuration.GetConnectionString("LoyaltyCoreDatabase")
            ?? throw new InvalidOperationException("LoyaltyCoreDatabase connection string missing.");
        _promotionsConnString = configuration.GetConnectionString("PromotionsDatabase")
            ?? throw new InvalidOperationException("PromotionsDatabase connection string missing.");
        _financeConnString = configuration.GetConnectionString("FinanceMasterDatabase")
            ?? throw new InvalidOperationException("FinanceMasterDatabase connection string missing.");
    }

    // Functionality 1: Query Member Tier Info using System.Data.SqlClient (Hardcoded FQN)
    public async Task<MemberTierInfo?> GetMemberTierInfoAsync(string memberAccountCode)
    {
        const string query = @"
            SELECT 
                mem.[EnterpriseMemberKey] AS MemberAccountCode,
                mem.[ExcellenceGroupTier] AS GlobalRankingTier,
                mem.[AggregatedSpendVolume] AS AccumulatedSpendAmount,
                mem.[CurrentLiquidityRewardTokens] AS AvailableBonusPoints,
                mem.[AnniversaryAuditDate] AS MilestoneReviewDate
            FROM [CustomerLoyaltyRewardsCore].[tiering].[CorporateRewardMemberships] mem
            WHERE mem.[EnterpriseMemberKey] = @AccountCode;";

        using var connection = new SqlConnection(_loyaltyConnString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        command.Parameters.Add(new SqlParameter("@AccountCode", SqlDbType.NVarChar, 100) { Value = memberAccountCode });

        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new MemberTierInfo
            {
                MemberAccountCode = reader.GetString(reader.GetOrdinal("MemberAccountCode")),
                GlobalRankingTier = reader.GetString(reader.GetOrdinal("GlobalRankingTier")),
                AccumulatedSpendAmount = reader.GetDecimal(reader.GetOrdinal("AccumulatedSpendAmount")),
                AvailableBonusPoints = reader.GetInt32(reader.GetOrdinal("AvailableBonusPoints")),
                MilestoneReviewDate = reader.GetDateTime(reader.GetOrdinal("MilestoneReviewDate"))
            };
        }

        return null;
    }

    // Functionality 2: Assign Promotion using Dapper
    public async Task<int> InsertPromotionAssignmentAsync(PromotionAssignment assignment)
    {
        const string query = @"
            INSERT INTO [MarketingCampaignsPromotions_Active].[rewards].[AllocatedCouponsRegistry]
                ([AssignmentTokenId], [UserAccountRef], [CampaignAlphaCode], [DeductionProportion], [EligibilityWindowExpires])
            VALUES
                (@EventAssignmentId, @MemberAccountCode, @AppliedPromotionCode, @DiscountCalculatedPercentage, @ActiveWindowExpiresAt);";

        using var connection = new SqlConnection(_promotionsConnString);
        return await connection.ExecuteAsync(query, assignment);
    }

    // Functionality 3: Cross-Database Join across CustomerLoyaltyRewardsCore and MarketingCampaignsPromotions_Active using Dapper
    public async Task<IEnumerable<PromotionAssignment>> GetActivePromotionsForEliteMembersAsync(string tierRanking)
    {
        const string query = @"
            SELECT 
                cpn.[AssignmentTokenId] AS EventAssignmentId,
                cpn.[UserAccountRef] AS MemberAccountCode,
                cpn.[CampaignAlphaCode] AS AppliedPromotionCode,
                cpn.[DeductionProportion] AS DiscountCalculatedPercentage,
                cpn.[EligibilityWindowExpires] AS ActiveWindowExpiresAt
            FROM [MarketingCampaignsPromotions_Active].[rewards].[AllocatedCouponsRegistry] cpn
            INNER JOIN [CustomerLoyaltyRewardsCore].[tiering].[CorporateRewardMemberships] mem
                ON cpn.[UserAccountRef] = mem.[EnterpriseMemberKey]
            WHERE mem.[ExcellenceGroupTier] = @TargetTier
              AND cpn.[EligibilityWindowExpires] > GETUTCDATE();";

        using var connection = new SqlConnection(_promotionsConnString);
        return await connection.QueryAsync<PromotionAssignment>(query, new { TargetTier = tierRanking });
    }

    // Functionality 4: Dynamic/Parameterized Table FQN for Cross-Brand Purchase Sweeps using Dapper
    public async Task<IEnumerable<CrossBrandSweptRewards>> GatherCrossSweepRewardsAsync(string businessConglomerateRegion)
    {
        string financeFqn = businessConglomerateRegion == "LATAM"
            ? "[GlobalRetailFinanceMaster].[interorg_sweep].[RewardsReconciliation_Latam]"
            : "[GlobalRetailFinanceMaster].[interorg_sweep].[RewardsReconciliation_RestOfWorld]";

        string query = $@"
            SELECT 
                [EnterpriseBeneficiaryHash] AS InternalMemberHash,
                [PartnershipEntityDomain] AS GlobalRetailBrandPartnerName,
                [CashBackCrossCalculatedRebate] AS CrossSweepReconciledRebate,
                [FiduciaryTransferAuditTimestamp] AS TransactedSystemReference
            FROM {financeFqn}
            WHERE [SettlementStatusClearance] = @StatusClearance;";

        using var connection = new SqlConnection(_financeConnString);
        return await connection.QueryAsync<CrossBrandSweptRewards>(query, new { StatusClearance = "CLEARED" });
    }

    // Functionality 5: Execute Member Balance Adjustment using System.Data.SqlClient transaction
    public async Task<int> AdjustMemberRewardPointsAsync(string accountCode, int deltaAdjustment)
    {
        const string query = @"
            UPDATE [CustomerLoyaltyRewardsCore].[tiering].[CorporateRewardMemberships]
            SET [CurrentLiquidityRewardTokens] = [CurrentLiquidityRewardTokens] + @Delta
            WHERE [EnterpriseMemberKey] = @AccountCode;";

        using var connection = new SqlConnection(_loyaltyConnString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();
        try
        {
            using var command = new SqlCommand(query, connection, transaction);
            command.Parameters.Add(new SqlParameter("@Delta", SqlDbType.Int) { Value = deltaAdjustment });
            command.Parameters.Add(new SqlParameter("@AccountCode", SqlDbType.NVarChar, 100) { Value = accountCode });

            int rows = await command.ExecuteNonQueryAsync();
            transaction.Commit();
            return rows;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
}
