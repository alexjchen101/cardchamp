"""
Status Detection and Deal Stage Logic

Maps CoPilot merchant statuses to HubSpot deal stages.
"""

# HubSpot Deal Stage IDs
STAGE_INTERESTED = "qualifiedtobuy"  # Stage 1: Interested
STAGE_CONTRACT_SENT = "contractsent"  # Stage 6: Contract Sent
STAGE_BOARDED = "3573354"  # Stage 7: Boarded
STAGE_LIVE = "closedwon"  # Stage 8: Live Customer

def get_deal_stage_from_status(status_data, signature_data=None):
    """
    Determine which HubSpot deal stage based on CoPilot status and signature.
    
    Flow:
    1. Interested (Stage 1) - Default
    2. Contract Sent (Stage 6) - Signature sent/signed
    3. Boarded (Stage 7) - gatewayBoardingStatusCd = BOARDED
    4. Live (Stage 8) - boardingProcessStatusCd = LIVE
    
    Args:
        status_data: Status response from CoPilot API
        signature_data: Signature response from CoPilot API (optional)
        
    Returns:
        tuple: (stage_id, stage_name, stage_number)
    """
    merchant_status = status_data.get("merchantStatus", {})
    boarding_status = merchant_status.get("boardingProcessStatusCd", "")
    gateway_boarding = merchant_status.get("gatewayBoardingStatusCd", "")
    
    # Priority order: most progressed wins
    if boarding_status == "LIVE":
        return (STAGE_LIVE, "Live", 8)
    
    if gateway_boarding == "BOARDED":
        return (STAGE_BOARDED, "Boarded", 7)
    
    # Check signature status if provided
    if signature_data:
        sig_status = signature_data.get("signatureStatus", {})
        sig_status_cd = sig_status.get("signatureStatusCd", "")
        
        # Any signature activity → Contract Sent
        if sig_status_cd in ["SENT", "PENDING", "SIGNED"]:
            return (STAGE_CONTRACT_SENT, "Contract Sent", 6)
    
    # Default: Interested
    return (STAGE_INTERESTED, "Interested", 1)


def get_status(status_data, is_initial_setup=False):
    """
    Get HubSpot Status field based on CoPilot status.
    
    HubSpot Field: status_2__cloned_
    
    Logic:
    - Initial setup → "Potential Merchant"
    - LIVE / BOARDED → "Current Merchant" (HubSpot label: Customer)
    - Other active pre-live statuses → "Potential Merchant"
    - CANCELLED → None (manual update required)
    
    Args:
        status_data: Status response from CoPilot API
        is_initial_setup: If True, returns "Potential Merchant"
        
    Returns:
        str: HubSpot status value, or None
    """
    if is_initial_setup:
        return "Potential Merchant"
    
    merchant_status = status_data.get("merchantStatus", {})
    boarding_status = merchant_status.get("boardingProcessStatusCd", "")
    gateway_boarding = merchant_status.get("gatewayBoardingStatusCd", "")
    cancelled_datetime = merchant_status.get("cancelledDatetime")

    if cancelled_datetime is not None:
        return None

    if boarding_status == "LIVE" or gateway_boarding == "BOARDED":
        return "Current Merchant"

    return "Potential Merchant"


def get_current_processor(status_data):
    """
    Get current processor based on CoPilot status.
    
    Logic:
    - LIVE / BOARDED → "CardChamp"
    - CANCELLED → "" (blank)
    - Other → None (don't update)
    
    Args:
        status_data: Status response from CoPilot API
        
    Returns:
        str or None: Processor value to set, or None to skip update
    """
    merchant_status = status_data.get("merchantStatus", {})
    boarding_status = merchant_status.get("boardingProcessStatusCd", "")
    gateway_boarding = merchant_status.get("gatewayBoardingStatusCd", "")
    
    # Check if merchant is cancelled
    cancelled_datetime = merchant_status.get("cancelledDatetime")
    
    if boarding_status == "LIVE" or gateway_boarding == "BOARDED":
        return "CardChamp"
    
    elif cancelled_datetime is not None:
        return ""  # Blank if cancelled
    
    # Don't update for other statuses
    return None


def print_status_summary(status_data, signature_data=None, merchant_data=None):
    """
    Print simple status summary.
    """
    merchant_status = status_data.get("merchantStatus", {})
    boarding_status = merchant_status.get("boardingProcessStatusCd", "UNKNOWN")
    
    stage_id, stage_name, stage_num = get_deal_stage_from_status(status_data, signature_data)
    contact_status = get_status(status_data)
    processor = get_current_processor(status_data)
    
    print(f"\n--- STATUS LOGIC ---")
    print(f"CoPilot Status: {boarding_status}")
    print(f"→ Deal Stage: {stage_name} (Stage {stage_num})")
    print(f"→ Contact Status: {contact_status if contact_status else '(no change)'}")
    print(f"→ Current Processor: {processor if processor is not None else '(no change)'}")
