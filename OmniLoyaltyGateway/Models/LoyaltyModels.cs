@dataclass
class MemberTierInfo:
    member_account_code: str = ""
    global_ranking_tier: str = ""
    accumulated_spend_amount: Decimal = Decimal(0)
    available_bonus_points: int = 0
    milestone_review_date: datetime = datetime(1, 1, 1)

@dataclass
class PromotionAssignment:
    event_assignment_id: UUID = UUID('00000000-0000-0000-0000-000000000000')
    member_account_code: str = ""
    applied_promotion_code: str = ""
    discount_calculated_percentage: Decimal = Decimal(0)
    active_window_expires_at: datetime = datetime(1, 1, 1)

@dataclass
class CrossBrandSweptRewards:
    internal_member_hash: str = ""
    global_retail_brand_partner_name: str = ""
    cross_sweep_reconciled_rebate: Decimal = Decimal(0)
    transacted_system_reference: datetime = datetime(1, 1, 1)
