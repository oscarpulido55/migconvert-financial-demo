import os
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from google.api_core.exceptions import GoogleAPIError

# Represents the data model `OmniLoyaltyGateway.Models.MemberTierInfo`
class MemberTierInfo:
    def __init__(self, MemberAccountCode: str, GlobalRankingTier: str, AccumulatedSpendAmount: Decimal, AvailableBonusPoints: int, MilestoneReviewDate: datetime):
        self.MemberAccountCode = MemberAccountCode
        self.GlobalRankingTier = GlobalRankingTier
        self.AccumulatedSpendAmount = AccumulatedSpendAmount
        self.AvailableBonusPoints = AvailableBonusPoints
        self.MilestoneReviewDate = MilestoneReviewDate

    @classmethod
    def from_row(cls, row: bigquery.Row):
        return cls(
            MemberAccountCode=row.MemberAccountCode,
            GlobalRankingTier=row.GlobalRankingTier,
            AccumulatedSpendAmount=row.AccumulatedSpendAmount,
            AvailableBonusPoints=row.AvailableBonusPoints,
            MilestoneReviewDate=row.MilestoneReviewDate
        )

# Represents the data model `OmniLoyaltyGateway.Models.PromotionAssignment`
class PromotionAssignment:
    def __init__(self, EventAssignmentId: str, MemberAccountCode: str, AppliedPromotionCode: str, DiscountCalculatedPercentage: float, ActiveWindowExpiresAt: datetime):
        self.EventAssignmentId = EventAssignmentId
        self.MemberAccountCode = MemberAccountCode
        self.AppliedPromotionCode = AppliedPromotionCode
        self.DiscountCalculatedPercentage = DiscountCalculatedPercentage
        self.ActiveWindowExpiresAt = ActiveWindowExpiresAt

    @classmethod
    def from_row(cls, row: bigquery.Row):
        return cls(
            EventAssignmentId=row.EventAssignmentId,
            MemberAccountCode=row.MemberAccountCode,
            AppliedPromotionCode=row.AppliedPromotionCode,
            DiscountCalculatedPercentage=float(row.DiscountCalculatedPercentage),
            ActiveWindowExpiresAt=row.ActiveWindowExpiresAt
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "EventAssignmentId": self.EventAssignmentId,
            "MemberAccountCode": self.MemberAccountCode,
            "AppliedPromotionCode": self.AppliedPromotionCode,
            "DiscountCalculatedPercentage": self.DiscountCalculatedPercentage,
            "ActiveWindowExpiresAt": self.ActiveWindowExpiresAt,
        }

# Represents the data model `OmniLoyaltyGateway.Models.CrossBrandSweptRewards`
class CrossBrandSweptRewards:
    def __init__(self, InternalMemberHash: str, GlobalRetailBrandPartnerName: str, CrossSweepReconciledRebate: Decimal, TransactedSystemReference: datetime):
        self.InternalMemberHash = InternalMemberHash
        self.GlobalRetailBrandPartnerName = GlobalRetailBrandPartnerName
        self.CrossSweepReconciledRebate = CrossSweepReconciledRebate
        self.TransactedSystemReference = TransactedSystemReference

    @classmethod
    def from_row(cls, row: bigquery.Row):
        return cls(
            InternalMemberHash=row.InternalMemberHash,
            GlobalRetailBrandPartnerName=row.GlobalRetailBrandPartnerName,
            CrossSweepReconciledRebate=row.CrossSweepReconciledRebate,
            TransactedSystemReference=row.TransactedSystemReference
        )

# Namespace conversion: `OmniLoyaltyGateway.DataAccess` implicitly covered by Python module/class structure.
class LoyaltyRepository:
    _gcp_project_id: str
    _loyalty_dataset_id: str
    _promotions_dataset_id: str
    _finance_dataset_id: str

    _bigquery_client: bigquery.Client # Replaces SqlConnection

    def __init__(self, configuration: Dict[str, Any]): # configuration from `IConfiguration` to Python dict
        self._gcp_project_id = configuration.get("GcpProjectId") or os.environ.get("GCP_PROJECT_ID")
        if not self._gcp_project_id:
            raise ValueError("GCP_PROJECT_ID configuration or environment variable missing.")

        self._loyalty_dataset_id = configuration.get("LoyaltyCoreDataset") or os.environ.get("LOYALTY_CORE_DATASET", "customer_loyalty_rewards_core_tiering")
        self._promotions_dataset_id = configuration.get("PromotionsDataset") or os.environ.get("PROMOTIONS_DATASET", "marketing_campaigns_promotions_active_rewards")
        self._finance_dataset_id = configuration.get("FinanceMasterDataset") or os.environ.get("FINANCE_MASTER_DATASET", "global_retail_finance_master_interorg_sweep")

        self._bigquery_client = bigquery.Client(project=self._gcp_project_id)

    # Functionality 1: Query Member Tier Info using BigQuery client
    async def get_member_tier_info_async(self, member_account_code: str) -> Optional[MemberTierInfo]:
        # SQL Server FQN `[CustomerLoyaltyRewardsCore].[tiering].[CorporateRewardMemberships]`
        # converted to BigQuery FQN: `project_id.dataset_id.table_name`
        query = f"""
            SELECT
                mem.EnterpriseMemberKey AS MemberAccountCode,
                mem.ExcellenceGroupTier AS GlobalRankingTier,
                mem.AggregatedSpendVolume AS AccumulatedSpendAmount,
                mem.CurrentLiquidityRewardTokens AS AvailableBonusPoints,
                mem.AnniversaryAuditDate AS MilestoneReviewDate
            FROM `{self._gcp_project_id}.{self._loyalty_dataset_id}.corporate_reward_memberships` AS mem
            WHERE mem.EnterpriseMemberKey = @account_code;
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("account_code", "STRING", member_account_code)
            ]
        )

        try:
            query_job = self._bigquery_client.query(query, job_config=job_config)
            results = query_job.result()

            first_row = next(iter(results), None)
            if first_row:
                return MemberTierInfo.from_row(first_row)
            return None
        except GoogleAPIError as e:
            print(f"BigQuery Error in GetMemberTierInfoAsync: {e}")
            raise

    # Functionality 2: Assign Promotion using BigQuery client (Dapper equivalent for DML)
    async def insert_promotion_assignment_async(self, assignment: PromotionAssignment) -> int:
        # SQL Server FQN `[MarketingCampaignsPromotions_Active].[rewards].[AllocatedCouponsRegistry]`
        # converted to BigQuery FQN
        query = f"""
            INSERT INTO `{self._gcp_project_id}.{self._promotions_dataset_id}.allocated_coupons_registry`
                (AssignmentTokenId, UserAccountRef, CampaignAlphaCode, DeductionProportion, EligibilityWindowExpires)
            VALUES
                (@EventAssignmentId, @MemberAccountCode, @AppliedPromotionCode, @DiscountCalculatedPercentage, @ActiveWindowExpiresAt);
        """

        params = assignment.to_dict()

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("EventAssignmentId", "STRING", params["EventAssignmentId"]),
                ScalarQueryParameter("MemberAccountCode", "STRING", params["MemberAccountCode"]),
                ScalarQueryParameter("AppliedPromotionCode", "STRING", params["AppliedPromotionCode"]),
                ScalarQueryParameter("DiscountCalculatedPercentage", "FLOAT64", params["DiscountCalculatedPercentage"]),
                ScalarQueryParameter("ActiveWindowExpiresAt", "TIMESTAMP", params["ActiveWindowExpiresAt"]),
            ]
        )

        try:
            query_job = self._bigquery_client.query(query, job_config=job_config)
            query_job.result()
            return query_job.num_dml_affected_rows
        except GoogleAPIError as e:
            print(f"BigQuery Error in InsertPromotionAssignmentAsync: {e}")
            raise

    # Functionality 3: Cross-Database Join across BigQuery datasets (Dapper equivalent for select)
    async def get_active_promotions_for_elite_members_async(self, tier_ranking: str) -> List[PromotionAssignment]:
        # Converted FQN and `GETUTCDATE()` to `CURRENT_TIMESTAMP()`
        query = f"""
            SELECT
                cpn.AssignmentTokenId AS EventAssignmentId,
                cpn.UserAccountRef AS MemberAccountCode,
                cpn.CampaignAlphaCode AS AppliedPromotionCode,
                cpn.DeductionProportion AS DiscountCalculatedPercentage,
                cpn.EligibilityWindowExpires AS ActiveWindowExpiresAt
            FROM `{self._gcp_project_id}.{self._promotions_dataset_id}.allocated_coupons_registry` AS cpn
            INNER JOIN `{self._gcp_project_id}.{self._loyalty_dataset_id}.corporate_reward_memberships` AS mem
                ON cpn.UserAccountRef = mem.EnterpriseMemberKey
            WHERE mem.ExcellenceGroupTier = @target_tier
              AND cpn.EligibilityWindowExpires > CURRENT_TIMESTAMP();
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("target_tier", "STRING", tier_ranking)
            ]
        )

        try:
            query_job = self._bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            return [PromotionAssignment.from_row(row) for row in results]
        except GoogleAPIError as e:
            print(f"BigQuery Error in GetActivePromotionsForEliteMembersAsync: {e}")
            raise

    # Functionality 4: Dynamic/Parameterized Table FQN for Cross-Brand Purchase Sweeps (Dapper equivalent)
    async def gather_cross_sweep_rewards_async(self, business_conglomerate_region: str) -> List[CrossBrandSweptRewards]:
        if business_conglomerate_region == "LATAM":
            finance_table_id = "rewards_reconciliation_latam"
        else:
            finance_table_id = "rewards_reconciliation_rest_of_world"

        finance_fqn = f"`{self._gcp_project_id}.{self._finance_dataset_id}.{finance_table_id}`"

        query = f"""
            SELECT
                EnterpriseBeneficiaryHash AS InternalMemberHash,
                PartnershipEntityDomain AS GlobalRetailBrandPartnerName,
                CashBackCrossCalculatedRebate AS CrossSweepReconciledRebate,
                FiduciaryTransferAuditTimestamp AS TransactedSystemReference
            FROM {finance_fqn}
            WHERE SettlementStatusClearance = @status_clearance;
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("status_clearance", "STRING", "CLEARED")
            ]
        )

        try:
            query_job = self._bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            return [CrossBrandSweptRewards.from_row(row) for row in results]
        except GoogleAPIError as e:
            print(f"BigQuery Error in GatherCrossSweepRewardsAsync: {e}")
            raise

    # Functionality 5: Execute Member Balance Adjustment using BigQuery atomic DML
    async def adjust_member_reward_points_async(self, account_code: str, delta_adjustment: int) -> int:
        # SQL Server FQN `[CustomerLoyaltyRewardsCore].[tiering].[CorporateRewardMemberships]`
        # converted to BigQuery FQN
        query = f"""
            UPDATE `{self._gcp_project_id}.{self._loyalty_dataset_id}.corporate_reward_memberships`
            SET CurrentLiquidityRewardTokens = CurrentLiquidityRewardTokens + @delta
            WHERE EnterpriseMemberKey = @account_code;
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("delta", "INT64", delta_adjustment),
                ScalarQueryParameter("account_code", "STRING", account_code),
            ]
        )

        try:
            # BigQuery DML operations are atomic per statement.
            # The try-except block here serves a similar purpose to the C# transaction
            # with rollback, by catching errors during the DML execution.
            query_job = self._bigquery_client.query(query, job_config=job_config)
            query_job.result()
            return query_job.num_dml_affected_rows
        except GoogleAPIError as e:
            print(f"BigQuery Error in AdjustMemberRewardPointsAsync: {e}")
            raise
