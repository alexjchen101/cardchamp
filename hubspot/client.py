"""
HubSpot API Client

Handles all HubSpot CRM API operations for contacts and deals.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()


class HubSpotClient:
    """
    Client for HubSpot CRM API operations.
    
    Usage:
        client = HubSpotClient()
        
        # Search for contacts with CoPilot Account Numbers
        contacts = client.search_contacts_with_copilot_id()
        
        # Update contact
        client.update_contact(contact_id, {"lifecyclestage": "customer"})
        
        # Create deal
        deal_id = client.create_deal("Deal Name", contact_id, "stage_id")
    """
    
    def __init__(self):
        """Initialize HubSpot client with access token from .env"""
        self.access_token = os.getenv("HUBSPOT_ACCESS_TOKEN")
        self.base_url = "https://api.hubapi.com"
        
        if not self.access_token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN not found in .env file")
    
    def _get_headers(self) -> dict:
        """Build headers for HubSpot API requests"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """
        Make HTTP request to HubSpot API.
        
        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint (e.g., "/crm/v3/objects/contacts")
            data: Optional JSON data for POST/PATCH
            
        Returns:
            dict: JSON response from API
            
        Raises:
            Exception: If request fails
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data
        )
        
        if response.status_code >= 400:
            raise Exception(f"HubSpot API Error {response.status_code}: {response.text}")
        
        return response.json() if response.text else {}
    
    # =========================================================================
    # CONTACT OPERATIONS
    # =========================================================================
    
    def search_contacts_by_email(self, email: str) -> dict:
        """
        Search for a contact by email address.
        
        Args:
            email: Email address to search for
            
        Returns:
            dict: Search response with results array
        """
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }],
            "properties": [
                "firstname", "lastname", "email", "phone", "mobilephone",
                "company", "lifecyclestage", "hubspot_owner_id"
            ],
            "limit": 1
        }
        
        return self._request("POST", "/crm/v3/objects/contacts/search", payload)
    
    def search_contacts_with_copilot_id(self, property_name: str = "copilot_account_number") -> list:
        """
        Search for all contacts that have a CoPilot Account Number.
        
        Args:
            property_name: Name of the custom property storing CoPilot Account #
            
        Returns:
            list: List of contact records with CoPilot Account Numbers
        """
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": property_name,
                    "operator": "HAS_PROPERTY"
                }]
            }],
            "properties": [
                "firstname", "lastname", "email", "phone", "mobilephone",
                property_name, "lifecyclestage"
            ],
            "limit": 100
        }
        
        response = self._request("POST", "/crm/v3/objects/contacts/search", payload)
        return response.get("results", [])
    
    def get_contact(self, contact_id: str, properties: list = None) -> dict:
        """
        Get a contact by ID.
        
        Args:
            contact_id: HubSpot contact ID
            properties: List of properties to retrieve
            
        Returns:
            dict: Contact record
        """
        endpoint = f"/crm/v3/objects/contacts/{contact_id}"
        if properties:
            props_str = ",".join(properties)
            endpoint += f"?properties={props_str}"
        
        return self._request("GET", endpoint)
    
    def get_contact_property_names(self) -> set:
        """
        Get set of contact property names that exist in HubSpot.
        Used to filter updates when numbered properties may not exist yet.
        """
        response = self._request("GET", "/crm/v3/properties/contacts")
        return {p["name"] for p in response.get("results", [])}
    
    def update_contact(self, contact_id: str, properties: dict, filter_to_existing: bool = False) -> dict:
        """
        Update contact properties.
        
        Args:
            contact_id: HubSpot contact ID
            properties: Dictionary of property names and values
            filter_to_existing: If True, only send properties that exist in HubSpot
                (skips numbered fields like company_1 if not created yet)
            
        Returns:
            dict: Updated contact record
        """
        if filter_to_existing:
            valid = self.get_contact_property_names()
            filtered = {k: v for k, v in properties.items() if k in valid}
            skipped = set(properties.keys()) - set(filtered.keys())
            if skipped:
                print(f"   ⚠ Skipped {len(skipped)} properties (not in HubSpot): {', '.join(sorted(skipped)[:5])}{'...' if len(skipped) > 5 else ''}")
            properties = filtered
        payload = {"properties": properties}
        return self._request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", payload)
    
    # =========================================================================
    # DEAL OPERATIONS
    # =========================================================================
    
    def create_deal(self, deal_name: str, contact_id: str, stage_id: str, 
                    amount: float = None, properties: dict = None) -> str:
        """
        Create a deal and associate it with a contact.
        
        Args:
            deal_name: Name of the deal
            contact_id: HubSpot contact ID to associate with
            stage_id: Deal stage ID (e.g., "appointmentscheduled")
            amount: Deal amount
            properties: Additional deal properties
            
        Returns:
            str: New deal ID
        """
        deal_props = {
            "dealname": deal_name,
            "dealstage": stage_id
        }
        
        if amount is not None:
            deal_props["amount"] = str(amount)
        
        if properties:
            deal_props.update(properties)
        
        payload = {"properties": deal_props}
        response = self._request("POST", "/crm/v3/objects/deals", payload)
        deal_id = response["id"]
        
        # Associate deal with contact
        self.associate_deal_with_contact(deal_id, contact_id)
        
        return deal_id
    
    def update_deal(self, deal_id: str, properties: dict) -> dict:
        """
        Update deal properties.
        
        Args:
            deal_id: HubSpot deal ID
            properties: Dictionary of property names and values
            
        Returns:
            dict: Updated deal record
            
        Example:
            client.update_deal("456", {
                "dealstage": "closedwon",
                "amount": "50000"
            })
        """
        payload = {"properties": properties}
        return self._request("PATCH", f"/crm/v3/objects/deals/{deal_id}", payload)
    
    def get_deals_for_contact(self, contact_id: str) -> list:
        """
        Get all deals associated with a contact.
        
        Args:
            contact_id: HubSpot contact ID
            
        Returns:
            list: List of deal records with properties
        """
        # Get associated deal IDs
        endpoint = f"/crm/v4/objects/contacts/{contact_id}/associations/deals"
        response = self._request("GET", endpoint)
        deal_associations = response.get("results", [])
        
        # Fetch full deal records with properties
        deals = []
        for assoc in deal_associations:
            deal_id = assoc.get("toObjectId")
            if deal_id:
                try:
                    deal = self._request("GET", f"/crm/v3/objects/deals/{deal_id}", {"properties": "dealname,dealstage,amount"})
                    deals.append(deal)
                except Exception:
                    # Skip if deal can't be fetched
                    continue
        
        return deals
    
    def associate_deal_with_contact(self, deal_id: str, contact_id: str) -> dict:
        """
        Associate a deal with a contact.
        
        Args:
            deal_id: HubSpot deal ID
            contact_id: HubSpot contact ID
            
        Returns:
            dict: Association confirmation
        """
        # Use v4 associations API with proper format
        endpoint = f"/crm/v4/objects/deals/{deal_id}/associations/default/contacts/{contact_id}"
        
        payload = [{
            "associationCategory": "HUBSPOT_DEFINED",
            "associationTypeId": 3  # Deal to Contact
        }]
        
        return self._request("PUT", endpoint, payload)
    
    # =========================================================================
    # PIPELINE & STAGE OPERATIONS
    # =========================================================================
    
    def get_deal_pipelines(self) -> list:
        """
        Get all deal pipelines and their stages.
        
        Returns:
            list: List of pipelines with stages
        """
        response = self._request("GET", "/crm/v3/pipelines/deals")
        return response.get("results", [])
