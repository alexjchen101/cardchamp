#!/usr/bin/env python3
"""
Status-Aware Sync

Checks CoPilot status and updates HubSpot accordingly:
- Detects boarding status (Interested / Contract Sent / Boarded)
- Updates deal stage to match reality
- Updates all contact fields
- Sets lifecycle stage if boarded ("Customer")
- Sets current processor if boarded ("CardChamp") or cancelled (blank)

This is the "production" sync that should run regularly (daily/hourly).

Usage:
    python3 sync_with_status.py <copilot_account_number> [contact_email]
    
Example:
    python3 sync_with_status.py 170761464 test@test.com
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
from status_logic import (
    get_deal_stage_from_status,
    get_status,
    get_current_processor,
)


def sync_with_status(contact_email):
    """
    Status-aware sync: Updates contact/deal based on CoPilot status.
    """
    print("="*60)
    print(f"STATUS-AWARE SYNC: {contact_email}")
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
    
    # Step 2: Get HubSpot contact data (needed for deals and updates)
    try:
        contact = hubspot.get_contact(contact_id, properties=[
            "firstname", "lastname", "email", "phone", "mobilephone",
            "company", "lifecyclestage", "hubspot_owner_id", "current_processor",
            "state", "zip", "city", "address", "website", "platform", "industry",
            "monthly_processing_volume", "merchant_id", "date_of_birth", "status_2__cloned_"
        ])
        current_props = contact.get("properties", {})
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False
    
    # Step 3: Fetch from CoPilot (iterate if multiple businesses)
    multi_business = len(copilot_ids) > 1
    first_merchant_data = None
    first_status_data = None
    first_signature_data = None
    all_merchant_data = []
    total_volume = 0
    
    for idx, copilot_id in enumerate(copilot_ids, 1):
        print(f"\n2.{idx}. Fetching CoPilot data for {copilot_id}...")
        
        try:
            merchant_data = copilot.get_merchant(copilot_id)
            status_data = copilot.get_status(copilot_id)
            signature_data = copilot.get_signature(copilot_id)
            
            merchant = merchant_data.get("merchant", {})
            ownership = merchant.get("ownership", {}).get("owner", {})
            demographic = merchant.get("demographic", {})
            processing = merchant.get("processing", {})
            
            print(f"   ✓ Retrieved: {merchant.get('dbaName')}")
            
            if idx == 1:
                first_merchant_data = merchant_data
                first_status_data = status_data
                first_signature_data = signature_data
            all_merchant_data.append(merchant_data)
            
            if multi_business:
                vol = merchant.get("processing", {}).get("volumeDetails", {}).get("averageMonthlyVolume")
                if vol:
                    total_volume += vol
            
            # Create/update deal for this business
            if idx == 1:
                print(f"\n--- DEALS ---")
            deal_name = f"{merchant.get('dbaName', 'Unknown')} - {copilot_id}"
            stage_id, stage_name, stage_num = get_deal_stage_from_status(status_data, signature_data)
            existing_deals = hubspot.get_deals_for_contact(contact_id)
            matching_deal = next((d for d in existing_deals if d.get('properties', {}).get('dealname') == deal_name), None)
            
            if matching_deal:
                deal_id = matching_deal.get('id')
                current_stage = matching_deal.get('properties', {}).get('dealstage')
                deal_updates = {"dealstage": stage_id}
                deal_amount = extract_deal_amount(merchant_data)
                if deal_amount and stage_num >= 6:
                    deal_updates["amount"] = str(deal_amount)
                if current_stage != stage_id or (deal_amount and stage_num >= 6):
                    hubspot._request("PATCH", f"/crm/v3/objects/deals/{deal_id}", {"properties": deal_updates})
                    print(f"   ✓ Deal updated: {deal_name} → {stage_name}")
            else:
                deal_amount = extract_deal_amount(merchant_data)
                owner_id = current_props.get("hubspot_owner_id")
                deal_props = {"dealname": deal_name, "dealstage": stage_id}
                if deal_amount:
                    deal_props["amount"] = str(deal_amount)
                if owner_id:
                    deal_props["hubspot_owner_id"] = owner_id
                response = hubspot._request("POST", "/crm/v3/objects/deals", {"properties": deal_props})
                hubspot.associate_deal_with_contact(response["id"], contact_id)
                print(f"   ✓ Deal created: {deal_name} ({stage_name})")
            
            # Print raw data (first business only for brevity)
            if idx == 1:
                ownership = merchant.get("ownership", {}).get("owner", {})
                processing = merchant.get("processing", {})
                platform_details = processing.get("platformDetails", {})
                volume_details = processing.get("volumeDetails", {})
                print(f"\n--- COPILOT RAW DATA (Business 1) ---")
                print(f"dbaName: {merchant.get('dbaName')}")
                print(f"ownerName: {ownership.get('ownerName')}")
                print(f"backEndMid: {platform_details.get('backEndMid')}")
                print(f"averageMonthlyVolume: {volume_details.get('averageMonthlyVolume')}")
                print(f"customerId: {merchant.get('customerId')}")
            
            if idx == 1:
                merchant_status = status_data.get("merchantStatus", {})
                boarding_status = merchant_status.get('boardingProcessStatusCd')
                contact_status_val = get_status(status_data)
                processor = get_current_processor(status_data)
                print(f"\n--- STATUS LOGIC ---")
                print(f"Status '{boarding_status}' will set:")
                print(f"→ Deal Stage: {stage_name} (Stage {stage_num})")
                print(f"→ Contact Status: {contact_status_val if contact_status_val else '(no change)'}")
                print(f"→ Current Processor: {processor if processor is not None else '(no change)'}")
            
        except Exception as e:
            print(f"   ✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    # Step 4: Map and update contact fields (including status-driven ones)
    print(f"\n" + "="*60)
    print(f"4. Updating contact fields...")
    print("="*60)
    
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
    
    # Add status-driven fields (from first business)
    contact_status = get_status(first_status_data)
    if contact_status:
        hubspot_updates["status_2__cloned_"] = contact_status
    
    processor = get_current_processor(first_status_data)
    if processor is not None:
        hubspot_updates["current_processor"] = processor
    
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
        print(f"\n   ✓ Contact updated with {len(hubspot_updates)} fields")
    except Exception as e:
        print(f"   ✗ Error updating contact: {e}")
        return False
    
    # Success
    _, summary_stage_name, _ = get_deal_stage_from_status(first_status_data, first_signature_data)
    print("\n" + "="*60)
    print("✓ STATUS-AWARE SYNC COMPLETE")
    print("="*60)
    print(f"\nContact: {contact_email} (ID: {contact_id})")
    print(f"CoPilot Account(s): {', '.join(copilot_ids)}")
    print(f"Deal Stage: {summary_stage_name} (first business)")
    print(f"Fields Updated: {len(hubspot_updates)}")
    print("="*60 + "\n")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 sync_with_status.py <contact_email>")
        print("Example: python3 sync_with_status.py test@test.com")
        sys.exit(1)
    
    contact_email = sys.argv[1]
    
    success = sync_with_status(contact_email)
    sys.exit(0 if success else 1)
