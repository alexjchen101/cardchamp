#!/usr/bin/env python3
"""
Initial Setup Sync (Status-Blind)

Creates/updates contact and deal - ALWAYS sets to "Interested" stage.
Syncs all available contact fields regardless of CoPilot status.

Use this for:
- Testing new contacts
- Initial baseline setup
- When you don't want status to affect deal stage yet

Usage:
    python3 sync_initial_setup.py <copilot_account_number> [contact_email]
    
Example:
    python3 sync_initial_setup.py 170761464 test@test.com
"""

import sys
from copilot import MerchantAPI
from hubspot.client import HubSpotClient
from field_mappings import (
    map_copilot_to_hubspot,
    get_copilot_accounts_from_contact,
    get_company_names_slash_separated,
    extract_deal_amount,
    volume_to_range,
)

# Deal stage (always "Interested" for initial setup)
STAGE_INTERESTED = "qualifiedtobuy"


def sync_initial_setup(contact_email):
    """
    Initial setup: Create/update contact and deal, ignoring status.
    Always sets deal stage to "Interested" and Status to "Potential Merchant".
    """
    print("="*60)
    print(f"INITIAL SETUP: {contact_email}")
    print("="*60)
    
    copilot = MerchantAPI()
    hubspot = HubSpotClient()
    
    # Step 1: Get CoPilot Account # from HubSpot
    print(f"\n1. Finding HubSpot contact ({contact_email})...")
    
    try:
        search_results = hubspot.search_contacts_by_email(contact_email)
        
        if not search_results.get("results"):
            print(f"   ✗ Contact not found")
            return False
        
        contact_id = search_results["results"][0]["id"]
        copilot_props = ["copilot_account"] + [f"copilot_account_{i}" for i in range(1, 7)]
        contact = hubspot.get_contact(contact_id, properties=copilot_props)
        contact_props = contact.get("properties", {})
        copilot_ids = get_copilot_accounts_from_contact(contact_props)
        
        if not copilot_ids:
            print(f"   ✗ No CoPilot Account # in HubSpot")
            return False
        
        print(f"   ✓ Contact ID: {contact_id}")
        print(f"   ✓ CoPilot Account(s): {', '.join(copilot_ids)}")
        
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False
    
    # Step 2: Get HubSpot contact data
    try:
        contact = hubspot.get_contact(contact_id, properties=[
            "firstname", "lastname", "email", "phone", "mobilephone",
            "company", "lifecyclestage", "hubspot_owner_id", "status_2__cloned_",
            "state", "zip", "city", "address", "website", "platform", "industry",
            "monthly_processing_volume", "merchant_id", "date_of_birth"
        ])
        current_props = contact.get("properties", {})
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False
    
    # Step 3: Fetch from CoPilot (iterate if multiple businesses)
    multi_business = len(copilot_ids) > 1
    first_merchant_data = None
    all_merchant_data = []
    total_volume = 0
    
    for idx, copilot_id in enumerate(copilot_ids, 1):
        print(f"\n3.{idx}. Fetching CoPilot data for {copilot_id}...")
        
        try:
            merchant_data = copilot.get_merchant(copilot_id)
            merchant = merchant_data.get("merchant", {})
            
            if idx == 1:
                first_merchant_data = merchant_data
            all_merchant_data.append(merchant_data)
            
            print(f"   ✓ Retrieved: {merchant.get('dbaName', 'N/A')}")
            
            if multi_business:
                vol = merchant.get("processing", {}).get("volumeDetails", {}).get("averageMonthlyVolume")
                if vol:
                    total_volume += vol
            
            # Create deal if doesn't exist
            deal_name = f"{merchant.get('dbaName', 'Unknown')} - {copilot_id}"
            existing_deals = hubspot.get_deals_for_contact(contact_id)
            matching_deal = next((d for d in existing_deals if d.get('properties', {}).get('dealname') == deal_name), None)
            
            if matching_deal:
                print(f"   ℹ️  Deal exists: {deal_name}")
            else:
                deal_amount = extract_deal_amount(merchant_data)
                deal_props = {"dealname": deal_name, "dealstage": STAGE_INTERESTED}
                if deal_amount:
                    deal_props["amount"] = str(deal_amount)
                if current_props.get("hubspot_owner_id"):
                    deal_props["hubspot_owner_id"] = current_props["hubspot_owner_id"]
                response = hubspot._request("POST", "/crm/v3/objects/deals", {"properties": deal_props})
                hubspot.associate_deal_with_contact(response["id"], contact_id)
                print(f"   ✓ Deal created: {deal_name}")
            
            if idx == 1:
                ownership = merchant.get("ownership", {}).get("owner", {})
                demographic = merchant.get("demographic", {})
                processing = merchant.get("processing", {})
                print(f"\n--- COPILOT RAW DATA ---")
                print(f"dbaName: {merchant.get('dbaName')}")
                print(f"ownerName: {ownership.get('ownerName')}")
                print(f"backEndMid: {processing.get('platformDetails', {}).get('backEndMid')}")
                print(f"averageMonthlyVolume: {processing.get('volumeDetails', {}).get('averageMonthlyVolume')}")
                print(f"customerId: {merchant.get('customerId')}")
        
        except Exception as e:
            print(f"   ✗ Error: {e}")
            return False
    
    # Step 4: Map and update contact fields
    print(f"\n4. Mapping CoPilot → HubSpot fields...")
    
    print(f"\n--- HUBSPOT BEFORE ---")
    for key in sorted(current_props.keys()):
        print(f"{key}: {current_props.get(key) or '(empty)'}")
    
    hubspot_updates = map_copilot_to_hubspot(
        first_merchant_data, current_props,
        exclude_business_specific=multi_business
    )
    
    # When multi-business: company = slash-separated, same order as input (primary at end)
    if multi_business:
        company_names = get_company_names_slash_separated(all_merchant_data[::-1])
        if company_names:
            hubspot_updates["company"] = company_names
        first_merchant = first_merchant_data.get("merchant", {})
        platform = first_merchant.get("processing", {}).get("platformDetails", {})
        if platform.get("backEndMid"):
            hubspot_updates["merchant_id"] = str(platform["backEndMid"])
        if total_volume:
            hubspot_updates["monthly_processing_volume"] = volume_to_range(total_volume)
    
    # ALWAYS set Contact Status to "Potential Merchant" on initial setup
    hubspot_updates["status_2__cloned_"] = "Potential Merchant"
    
    print(f"\n--- SENDING TO HUBSPOT ({len(hubspot_updates)} fields) ---")
    for field, value in sorted(hubspot_updates.items()):
        print(f"{field}: {value if value else '(empty)'}")
    
    print(f"\n--- CHANGES ---")
    for field, new_value in sorted(hubspot_updates.items()):
        current_value = current_props.get(field, "")
        if current_value == "":
            print(f"NEW: {field} = {new_value}")
        elif str(current_value) != str(new_value):
            print(f"CHANGED: {field} = '{current_value}' → '{new_value}'")
    
    try:
        hubspot.update_contact(contact_id, hubspot_updates)
        print(f"\n   ✓ Contact updated")
    except Exception as e:
        print(f"   ✗ Error updating contact: {e}")
        return False
    
    # Success
    print("\n" + "="*60)
    print("✓ INITIAL SETUP COMPLETE")
    print("="*60)
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 sync_initial_setup.py <contact_email>")
        print("Example: python3 sync_initial_setup.py test@test.com")
        sys.exit(1)
    
    contact_email = sys.argv[1]
    
    success = sync_initial_setup(contact_email)
    sys.exit(0 if success else 1)
