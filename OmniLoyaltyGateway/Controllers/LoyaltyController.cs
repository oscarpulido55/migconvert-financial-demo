router = APIRouter(
    prefix="/api/loyalty",
    tags=["Loyalty"],
)

@router.get("/member/{account_code}")
async def get_member_profile(
    account_code: str,
    repository: "LoyaltyRepository" = Depends(),
    logger: "logging.Logger" = Depends(lambda: logging.getLogger(__name__))
):
    try:
        profile = await repository.get_member_tier_info(account_code)
        if profile is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "Member identity unverified or absent from tier tracking."}
            )
        return profile
    except Exception as ex:
        logger.error("Error fetching member tier info for %s: %s", account_code, ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Loyalty system core communication error."}
        )

@router.post("/assign-coupon")
async def assign_reward_coupon(
    promotion: "PromotionAssignment",
    repository: "LoyaltyRepository" = Depends(),
    logger: "logging.Logger" = Depends(lambda: logging.getLogger(__name__))
):
    try:
        promotion.event_assignment_id = uuid.uuid4()
        await repository.insert_promotion_assignment(promotion)
        # FastAPI typically returns the created object with a 201 status for POST,
        # implicitly fulfilling the "Created" equivalent.
        return promotion
    except Exception as ex:
        logger.error("Error assigning corporate promotion voucher: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed saving promotion records into campaigns database."}
        )

@router.get("/elite-promotions/{rank_tier}")
async def get_elite_promos(
    rank_tier: str,
    repository: "LoyaltyRepository" = Depends(),
    logger: "logging.Logger" = Depends(lambda: logging.getLogger(__name__))
):
    try:
        offers = await repository.get_active_promotions_for_elite_members(rank_tier)
        return offers
    except Exception as ex:
        logger.error("Error evaluating elite promotion inner joins: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Cross database evaluation join encountered transaction timeout."}
        )

@router.get("/sweep-partner/{region_code}")
async def sweep_partner_rewards(
    region_code: str,
    repository: "LoyaltyRepository" = Depends(),
    logger: "logging.Logger" = Depends(lambda: logging.getLogger(__name__))
):
    try:
        rebates = await repository.gather_cross_sweep_rewards(region_code)
        return rebates
    except Exception as ex:
        logger.error("Error executing finance dynamic FQN queries: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Secure enterprise finance data warehouse communication failed."}
        )

@router.post("/adjust/{account_code}")
async def update_balance(
    account_code: str,
    point_adjustment: int,
    repository: "LoyaltyRepository" = Depends(),
    logger: "logging.Logger" = Depends(lambda: logging.getLogger(__name__))
):
    try:
        updated_rows = await repository.adjust_member_reward_points(account_code, point_adjustment)
        return {
            "rows_modified": updated_rows,
            "completed_at": datetime.now(timezone.utc)
        }
    except Exception as ex:
        logger.error("Error updating member point balance: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "System atomic transaction failed to lock balance tracking rows."}
        )
