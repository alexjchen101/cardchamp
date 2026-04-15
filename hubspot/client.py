"""
HubSpot API Client

Handles all HubSpot CRM API operations for contacts and deals.
"""

from __future__ import annotations

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

# CoPilot sync must only ever set the primary ``email`` field. Do not PATCH HubSpot
# secondary-email aggregates (often read-only / UI-managed); stripping avoids
# accidentally overwriting additional addresses if a caller merges full props.
_CONTACT_PATCH_EMAIL_DENYLIST = frozenset({"hs_additional_emails"})


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
        self.timeout_seconds = float(os.getenv("HUBSPOT_TIMEOUT_SECONDS", "30"))
        self.max_retries = int(os.getenv("HUBSPOT_MAX_RETRIES", "4"))
        self.retry_backoff_seconds = float(os.getenv("HUBSPOT_RETRY_BACKOFF_SECONDS", "2"))
        
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

        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise Exception(f"HubSpot request failed after retries: {exc}") from exc
                time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                continue

            if response.status_code < 400:
                return response.json() if response.text else {}

            if response.status_code in retryable_statuses and attempt < self.max_retries:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = self.retry_backoff_seconds * (2 ** attempt)
                else:
                    delay = self.retry_backoff_seconds * (2 ** attempt)
                time.sleep(delay)
                continue

            raise Exception(f"HubSpot API Error {response.status_code}: {response.text}")
    
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
    
    def search_contacts_with_copilot_id(self, property_name: str = "copilot_account") -> list:
        """
        First page of contacts where the CoPilot field is set (default: ``copilot_account``).

        HubSpot returns at most ``limit`` rows; use ``paging.next.after`` to fetch more
        for a full tenant sync.
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

    def iter_contacts_with_property(self, property_name: str = "copilot_account", limit: int = 100):
        """
        Yield all contacts where ``property_name`` is set, following HubSpot paging.
        """
        after = None
        while True:
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
                "limit": limit
            }
            if after is not None:
                payload["after"] = after
            response = self._request("POST", "/crm/v3/objects/contacts/search", payload)
            results = response.get("results", [])
            for row in results:
                yield row
            after = ((response.get("paging") or {}).get("next") or {}).get("after")
            if not after:
                break
    
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

    def get_deal_property_names(self) -> set:
        """Set of deal property internal names (for optional custom fields like ``sales_code``)."""
        response = self._request("GET", "/crm/v3/properties/deals")
        return {p["name"] for p in response.get("results", [])}
    
    def get_contact_property_definition(self, property_name: str) -> dict:
        """
        Full property schema (options, type, fieldType) for enum / multi-select fields.
        
        Multi-select checkbox values must be sent as semicolon-separated **option values**
        (see each option's ``value`` — often same as ``label`` for custom properties).
        """
        from urllib.parse import quote
        safe = quote(property_name, safe="")
        return self._request("GET", f"/crm/v3/properties/contacts/{safe}")
    
    def update_contact(self, contact_id: str, properties: dict, filter_to_existing: bool = False) -> dict:
        """
        Update contact properties.

        CoPilot sync uses only the primary ``email`` field for the merchant owner address.
        Secondary-email aggregate properties (e.g. ``hs_additional_emails``) are stripped
        so integrations never overwrite additional addresses configured in HubSpot.

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
        denied = _CONTACT_PATCH_EMAIL_DENYLIST & set(properties.keys())
        if denied:
            properties = {k: v for k, v in properties.items() if k not in _CONTACT_PATCH_EMAIL_DENYLIST}
            print(
                f"   ℹ️  Omitting non-primary email fields from PATCH (CoPilot only sets email): {', '.join(sorted(denied))}"
            )
        payload = {"properties": properties}
        return self._request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", payload)

    def delete_contact(self, contact_id: str) -> dict:
        """
        Archive a contact in HubSpot (CRM v3 DELETE archives the record).

        Args:
            contact_id: HubSpot contact object ID

        Returns:
            Empty dict on 204, or JSON body if HubSpot returns one
        """
        return self._request("DELETE", f"/crm/v3/objects/contacts/{contact_id}")

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
                    props = "dealname,dealstage,amount,hubspot_owner_id,sales_code"
                    deal = self._request("GET", f"/crm/v3/objects/deals/{deal_id}?properties={props}")
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
    
    def list_owners(self, limit: int = 500) -> list:
        """
        CRM users (owners). Each ``id`` is valid for ``hubspot_owner_id`` on contacts/deals.

        Paginated; increase limit or add paging if you have many users.
        """
        response = self._request("GET", f"/crm/v3/owners?limit={limit}")
        return response.get("results", [])
    
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


def deal_name_for_sync(merchant: dict) -> str:
    """
    HubSpot ``dealname`` for deals created by sync: DBA only (no trailing `` - {copilot_id}``).
    """
    dba = str((merchant or {}).get("dbaName") or "Unknown").strip()
    return dba or "Unknown"


def _back_end_mid(merchant: dict) -> str | None:
    plat = (merchant.get("processing") or {}).get("platformDetails") or {}
    v = plat.get("backEndMid")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def deal_matches_merchant_business(
    dealname: str | None,
    merchant: dict,
    copilot_id: str,
) -> bool:
    """
    True if this HubSpot deal title belongs to this CoPilot business.

    Matches DBA-only, legacy ``{DBA} - {copilot_id}``, ``{DBA} - {backEndMid}``,
    or ``{DBA} - <digits>`` (numeric merchant / MID suffix) so we can find and
    rename old deals to DBA-only.
    """
    cur = (dealname or "").strip()
    dba = deal_name_for_sync(merchant)
    if cur == dba:
        return True
    if cur == f"{dba} - {copilot_id}":
        return True
    be = _back_end_mid(merchant)
    if be and cur == f"{dba} - {be}":
        return True
    if not cur.startswith(dba):
        return False
    rest = cur[len(dba) :].strip()
    if not rest.startswith("-"):
        return False
    suffix = rest.lstrip("-").strip()
    if suffix == str(copilot_id):
        return True
    if be and suffix == str(be):
        return True
    if suffix.replace(" ", "").isdigit():
        return True
    return False


def find_deal_for_merchant_business(
    existing_deals: list,
    merchant: dict,
    copilot_id: str,
):
    """
    Find a deal associated with this CoPilot merchant: DBA-only **or** legacy
    titles with a trailing `` - <id>`` (CoPilot id, back-end MID, or digits).
    """
    return next(
        (
            d
            for d in existing_deals
            if deal_matches_merchant_business(
                (d.get("properties") or {}).get("dealname"),
                merchant,
                copilot_id,
            )
        ),
        None,
    )
