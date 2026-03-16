"""
Field Mapping Utilities

Maps CoPilot data formats to HubSpot field formats.
"""

import re
from mcc_mapping import get_industry_from_mcc

# State code to full name mapping
STATE_CODE_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington, DC"
}


# Business-specific fields (excluded when using numbered properties for multi-business)
BUSINESS_SPECIFIC_FIELDS = {"company", "merchant_id", "monthly_processing_volume"}


def map_copilot_to_hubspot(merchant_data, current_contact_props=None, exclude_business_specific=False):
    """
    Map CoPilot merchant data to HubSpot contact fields.
    Uses existing HubSpot properties (no custom fields needed).
    
    Args:
        merchant_data: Full merchant data from CoPilot API
        current_contact_props: Existing HubSpot contact properties (to check what not to overwrite)
        exclude_business_specific: If True, omit company/merchant_id/monthly_processing_volume
            (use map_business_to_hubspot for numbered fields instead)
        
    Returns:
        dict: HubSpot field updates
    """
    if current_contact_props is None:
        current_contact_props = {}
    
    merchant = merchant_data.get("merchant", {})
    ownership = merchant.get("ownership", {}).get("owner", {})
    demographic = merchant.get("demographic", {})
    processing = merchant.get("processing", {})
    
    updates = {}
    
    # ========== BASIC INFO ==========
    
    # Company Name (DBA)
    if merchant.get('dbaName'):
        updates['company'] = merchant['dbaName']
    
    # Owner Name - split into first/last
    owner_name = ownership.get('ownerName')
    if owner_name:
        parts = owner_name.strip().split(None, 1)  # Split on first space
        if len(parts) >= 1:
            updates['firstname'] = parts[0]
        if len(parts) >= 2:
            updates['lastname'] = parts[1]
    
    # Email - DON'T overwrite (user said so)
    # owner_email = ownership.get('ownerEmail')
    # if owner_email and not current_contact_props.get('email'):
    #     updates['email'] = owner_email
    
    # Phone - only if empty (don't overwrite)
    if not current_contact_props.get('phone') and ownership.get('ownerPhone'):
        updates['phone'] = ownership['ownerPhone']
    
    # Mobile Phone
    if ownership.get('ownerMobilePhone'):
        updates['mobilephone'] = ownership['ownerMobilePhone']
    
    # ========== ADDRESS ==========
    
    business_address = demographic.get("businessAddress", {})
    
    # State - convert code to full name
    state_code = business_address.get('stateCd')
    if state_code:
        updates['state'] = STATE_CODE_TO_NAME.get(state_code, state_code)
    
    # Zip
    if business_address.get('zip'):
        updates['zip'] = business_address['zip']
    
    # City
    if business_address.get('city'):
        updates['city'] = business_address['city']
    
    # Address (full formatted)
    if business_address:
        address_parts = []
        if business_address.get('address1'):
            address_parts.append(business_address['address1'])
        if business_address.get('address2'):
            address_parts.append(business_address['address2'])
        if address_parts:
            updates['address'] = ", ".join(address_parts)
    
    # ========== BUSINESS INFO ==========
    
    # Website
    website = demographic.get("websiteAddress")
    if website:
        updates['website'] = website
    
    platform_details = processing.get("platformDetails", {})
    
    # MCC Code / Industry - Map to "8041 - Chiropractors" format
    mcc_id = platform_details.get("mccId")
    if mcc_id:
        # Use MCC mapping to get proper HubSpot industry value
        updates['industry'] = get_industry_from_mcc(mcc_id)
    
    # Platform - Map backend platform code to HubSpot values
    backend_platform = platform_details.get("backEndPlatformCd")
    if backend_platform:
        # Map CoPilot backend platform codes to HubSpot values
        # FDNOB = First Data North, FDOMA = First Data Omaha, etc.
        platform_map = {
            "FDNOB": "North",      # First Data North
            "RCNOF": "North",      # RapidConnect North  
            "FDOMA": "Omaha",      # First Data Omaha
            "RCOMA": "Omaha",      # RapidConnect Omaha
            "TSYS": "TSYS CardConnect",
            "BUYPASS": "Buypass"
        }
        # Use backend platform, default to North if not found
        updates['platform'] = platform_map.get(backend_platform, "North")
    
    # Point of Sale / Equipment
    equipment = merchant.get("equipment", [])
    if equipment:
        # Take first equipment item
        first_item = equipment[0]
        make = first_item.get("make", "")
        model = first_item.get("model", "")
        
        # Try to match to HubSpot POS options
        equipment_str = f"{make} {model}".strip()
        
        # Common mappings
        if "clover" in equipment_str.lower():
            if "mini" in equipment_str.lower():
                updates['point_of_sale'] = "Clover Mini"
            else:
                updates['point_of_sale'] = "Clover POS"
        elif "ingenico" in equipment_str.lower():
            if "220" in equipment_str:
                updates['point_of_sale'] = "Cardpointe Ingenico iCT220"
            elif "250" in equipment_str:
                updates['point_of_sale'] = "Ingenico iCT250"
            else:
                updates['point_of_sale'] = "Cardpointe Ingenico iCT220"
        elif "vx520" in equipment_str.lower():
            updates['point_of_sale'] = "Verifone VX520"
        elif "pax" in equipment_str.lower():
            updates['point_of_sale'] = "PAX S80"
        else:
            # Default to generic
            updates['point_of_sale'] = "POS System"
    
    # ========== FINANCIAL INFO ==========
    
    # Monthly Processing Volume - map to HubSpot ranges
    volume_details = processing.get("volumeDetails", {})
    avg_volume = volume_details.get("averageMonthlyVolume")
    if avg_volume:
        updates['monthly_processing_volume'] = volume_to_range(avg_volume)
    
    # ========== IDs (IMPORTANT DISTINCTION) ==========
    
    # Merchant Account Number - The PROCESSING MID (assigned after boarding)
    # NOTE: Three different IDs exist:
    #   1. copilot_account (HubSpot) = Manually entered to FIND merchant (e.g., 170761464)
    #   2. merchantId (CoPilot) = Internal CoPilot ID (e.g., 170761464)
    #   3. merchant_id (HubSpot) = PROCESSING MID from backEndMid (e.g., 496615994886)
    #
    # We sync the PROCESSING MID (backEndMid), not the internal merchantId
    backend_mid = platform_details.get("backEndMid")
    if backend_mid:
        updates['merchant_id'] = str(backend_mid)
    
    # ACH / E-Check Provider
    ach_options = processing.get("blueChexSecOptions", {})
    if ach_options and any(ach_options.values()):
        updates['ach___e_check_provider'] = "Fiserv ACH"
    
    # ========== PERSONAL INFO ==========
    
    # Date of Birth - Convert to proper format if needed
    owner_dob = ownership.get("ownerDob")
    if owner_dob:
        # CoPilot format: "05/01/1961"
        # HubSpot might need YYYY-MM-DD, but let's try as-is first
        updates['date_of_birth'] = owner_dob
    
    if exclude_business_specific:
        for f in BUSINESS_SPECIFIC_FIELDS:
            updates.pop(f, None)
    
    return updates


def extract_deal_amount(merchant_data):
    """Extract deal amount from CoPilot data"""
    processing = merchant_data.get("merchant", {}).get("processing", {})
    volume_details = processing.get("volumeDetails", {})
    return volume_details.get("averageMonthlyVolume", 0)


def extract_equipment_list(merchant_data):
    """Extract equipment/POS from CoPilot"""
    equipment = merchant_data.get("merchant", {}).get("equipment", [])
    if not equipment:
        return None
    
    equipment_list = []
    for item in equipment:
        make = item.get("make", "")
        model = item.get("model", "")
        if make and model:
            equipment_list.append(f"{make} {model}")
        elif make:
            equipment_list.append(make)
        elif model:
            equipment_list.append(model)
    
    return "; ".join(equipment_list) if equipment_list else None


def get_pricing_type(merchant_data):
    """Extract pricing type from CoPilot"""
    pricing = merchant_data.get("merchant", {}).get("pricing", {})
    
    if pricing.get("flatPricing"):
        return "Flat Pricing"
    elif pricing.get("icPlusPricing"):
        return "IC Plus Pricing"
    elif pricing.get("swipeNonSwipePricing"):
        return "Swipe/Non-Swipe Pricing"
    
    return None


def check_ach_enabled(merchant_data):
    """Check if ACH is enabled"""
    processing = merchant_data.get("merchant", {}).get("processing", {})
    ach_options = processing.get("blueChexSecOptions", {})
    
    if not ach_options:
        return False
    
    # Check if any ACH option is enabled
    return any(ach_options.values())


def get_copilot_accounts_from_contact(contact_props):
    """
    Get list of CoPilot Account IDs from HubSpot contact.
    Supports: copilot_account (slash-separated), copilot_account_1..6 (numbered).
    
    Format: "x / y / z" - LAST (z) is primary for shared fields.
    When adding new: "new / x / y / z" - z stays primary, no re-update needed.
    
    Returns list with LAST entry first (primary first for processing).
    """
    ids = []
    # Single field: slash-separated
    single = contact_props.get("copilot_account", "").strip()
    if single:
        for part in re.split(r"\s*/\s*", single):
            part = part.strip()
            if part:
                ids.append(part)
        if ids:
            ids.reverse()  # Last in input = first/primary
            return ids
    # Numbered fields: highest index is primary
    for i in range(1, 7):
        val = contact_props.get(f"copilot_account_{i}", "").strip()
        if val:
            ids.append(val)
    ids.reverse()
    return ids


def get_company_names_slash_separated(merchant_data_list):
    """
    Build slash-separated company names for multi-business.
    e.g. "Preston A / Preston B / Preston C"
    
    Args:
        merchant_data_list: List of merchant data dicts from CoPilot
        
    Returns:
        str or None: "Name1 / Name2 / Name3" or None if empty
    """
    names = []
    for md in merchant_data_list:
        dba = md.get("merchant", {}).get("dbaName")
        if dba:
            names.append(dba)
    return " / ".join(names) if names else None


def volume_to_range(avg_volume):
    """Map numeric volume to HubSpot dropdown range."""
    if not avg_volume:
        return None
    if avg_volume < 5000:
        return "Under 5K"
    elif avg_volume < 20000:
        return "5-20K"
    elif avg_volume < 50000:
        return "20-50K"
    elif avg_volume < 100000:
        return "50-100K"
    elif avg_volume < 500000:
        return "100-500K"
    else:
        return "500K+"


def get_mcc_code(merchant_data):
    """Extract MCC code"""
    processing = merchant_data.get("merchant", {}).get("processing", {})
    platform_details = processing.get("platformDetails", {})
    return str(platform_details.get("mccId", "")) if platform_details.get("mccId") else None


def get_cash_discount(merchant_data):
    """
    Determine if merchant uses Cash Discount pricing (Flat Rate).
    
    Returns:
        str: "Yes" if flatPricing exists, "No" otherwise
        
    Note: HubSpot field 'cash_discount' doesn't exist yet - needs to be created as custom field
    """
    merchant = merchant_data.get("merchant", {})
    pricing = merchant.get("pricing", {})
    
    if pricing.get("flatPricing"):
        return "Yes"
    else:
        return "No"


def get_pci_compliance_info(merchant_data):
    """
    Extract PCI compliance-related information from CoPilot.
    
    Returns:
        dict: PCI-related fields from CoPilot
        
    Note: 
    - HubSpot field 'pci_non_compliance' doesn't exist yet - needs to be created
    - Logic for determining compliance status is unclear from CoPilot data
    - Available fields: pciProgramCd, pciAnnualFee, pciComplianceServiceFee
    """
    merchant = merchant_data.get("merchant", {})
    fees = merchant.get("fees", {})
    clover = merchant.get("cloverSecurityAndTransarmor", {})
    
    return {
        "pciProgramCd": fees.get("pciProgramCd"),  # e.g., "ANNUAL"
        "pciAnnualFee": fees.get("pciAnnualFee"),  # e.g., 0
        "pciComplianceServiceFee": clover.get("pciComplianceServiceFee"),  # Usually null
        "pciComplianceServiceFeeFrequency": clover.get("pciComplianceServiceFeeFrequency"),
        # TODO: Unclear what indicates non-compliance
        # Possible interpretations:
        # - pciAnnualFee > 0 means non-compliant?
        # - pciComplianceServiceFee exists means non-compliant?
        # Need clarification on business logic
    }
