"""
Merchant API Wrapper

All merchant-related operations for the CoPilot API.
"""

import base64
from pathlib import Path
from .client import CoPilotClient


class MerchantAPI:
    """
    Wrapper for all CoPilot Merchant API endpoints.
    
    Usage:
        api = MerchantAPI()
        
        # Create a merchant
        merchant_id = api.create_merchant(merchant_data)
        
        # Get merchant details
        merchant = api.get_merchant(merchant_id)
        
        # Check status
        status = api.get_status(merchant_id)
    """
    
    def __init__(self, client: CoPilotClient = None):
        """
        Initialize the Merchant API.
        
        Args:
            client: Optional CoPilotClient instance. If not provided,
                    creates a new one (loads from .env).
        """
        self.client = client or CoPilotClient()
    
    # =========================================================================
    # CORE MERCHANT OPERATIONS
    # =========================================================================
    
    def create_merchant(self, merchant_data: dict, template_id: str = None, 
                        sales_code: str = None) -> str:
        """
        Create a new merchant account.
        
        Args:
            merchant_data: Dictionary with merchant information (see build_merchant_payload)
            template_id: Template ID to use (defaults to env COPILOT_TEMPLATE_ID)
            sales_code: Sales code (defaults to env COPILOT_SALES_CODE)
            
        Returns:
            str: The new merchant's CoPilot ID
        """
        payload = {
            "templateId": template_id or self.client.template_id,
            "merchant": {
                "salesCode": sales_code or self.client.sales_code,
                **merchant_data.get("merchant", merchant_data)
            }
        }
        
        # Add ownerSiteUser if provided
        if "ownerSiteUser" in merchant_data:
            payload["ownerSiteUser"] = merchant_data["ownerSiteUser"]
        
        response = self.client.post("/merchant", payload)
        
        # Extract and return the merchant ID
        merchant_id = response.get("merchantId") or response.get("copilotMerchantId")
        print(f"[MerchantAPI] Created merchant: {merchant_id}")
        return merchant_id
    
    def get_merchant(self, merchant_id: str) -> dict:
        """
        Get all data for a merchant.
        
        Args:
            merchant_id: The CoPilot merchant ID
            
        Returns:
            dict: Full merchant object
        """
        return self.client.get(f"/merchant/{merchant_id}")
    
    def update_merchant(self, merchant_id: str, merchant_data: dict) -> dict:
        """
        Update a merchant's data.
        
        Note: Cannot modify template or sales code for existing merchants.
        
        Args:
            merchant_id: The CoPilot merchant ID
            merchant_data: Dictionary with updated merchant information
            
        Returns:
            dict: Updated merchant object
        """
        return self.client.put(f"/merchant/{merchant_id}", merchant_data)
    
    def get_status(self, merchant_id: str) -> dict:
        """
        Get merchant boarding status.
        
        Args:
            merchant_id: The CoPilot merchant ID
            
        Returns:
            dict: Status including account boarding and gateway boarding status
        """
        return self.client.get(f"/merchant/{merchant_id}/status")
    
    # =========================================================================
    # SUB-OBJECT OPERATIONS (Bank, Demographic, Fees, etc.)
    # =========================================================================
    
    # --- Bank Details ---
    def get_bank(self, merchant_id: str) -> dict:
        """Get merchant bank details (deposit and withdrawal accounts)."""
        return self.client.get(f"/merchant/{merchant_id}/bank")
    
    def update_bank(self, merchant_id: str, bank_data: dict) -> dict:
        """
        Update merchant bank details.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        """
        return self.client.put(f"/merchant/{merchant_id}/bank", bank_data)
    
    # --- Demographic ---
    def get_demographic(self, merchant_id: str) -> dict:
        """Get merchant demographic info (addresses, phone, timezone)."""
        response = self.client.get(f"/merchant/{merchant_id}/demographic")
        # API returns "demographics" (plural), normalize to "demographic" for consistency
        if "demographics" in response and "demographic" not in response:
            response["demographic"] = response.pop("demographics")
        return response
    
    def update_demographic(self, merchant_id: str, demographic_data: dict) -> dict:
        """
        Update merchant demographic info.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        """
        return self.client.put(f"/merchant/{merchant_id}/demographic", demographic_data)
    
    # --- Fees ---
    def get_fees(self, merchant_id: str) -> dict:
        """Get merchant fee configuration."""
        return self.client.get(f"/merchant/{merchant_id}/fees")
    
    def update_fees(self, merchant_id: str, fees_data: dict) -> dict:
        """
        Update merchant fees.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        """
        return self.client.put(f"/merchant/{merchant_id}/fees", fees_data)
    
    # --- Ownership ---
    def get_ownership(self, merchant_id: str) -> dict:
        """Get merchant ownership info (owner details, additional owners)."""
        return self.client.get(f"/merchant/{merchant_id}/ownership")
    
    def update_ownership(self, merchant_id: str, ownership_data: dict) -> dict:
        """
        Update merchant ownership info.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        """
        return self.client.put(f"/merchant/{merchant_id}/ownership", ownership_data)
    
    # --- Pricing ---
    def get_pricing(self, merchant_id: str) -> dict:
        """Get merchant pricing configuration."""
        return self.client.get(f"/merchant/{merchant_id}/pricing")
    
    def update_pricing(self, merchant_id: str, pricing_data: dict) -> dict:
        """
        Update merchant pricing.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        Only include ONE pricing type (flatPricing, icPlusPricing, or swipeNonSwipePricing).
        """
        return self.client.put(f"/merchant/{merchant_id}/pricing", pricing_data)
    
    # --- Processing ---
    def get_processing(self, merchant_id: str) -> dict:
        """Get merchant processing configuration."""
        return self.client.get(f"/merchant/{merchant_id}/processing")
    
    def update_processing(self, merchant_id: str, processing_data: dict) -> dict:
        """
        Update merchant processing configuration.
        Note: Supply ALL fields - omitted fields are overwritten with null.
        """
        return self.client.put(f"/merchant/{merchant_id}/processing", processing_data)
    
    # =========================================================================
    # SIGNATURE OPERATIONS
    # =========================================================================
    
    def create_signature(self, merchant_id: str) -> dict:
        """
        Generate a digital signature URL for the merchant to sign.
        
        Args:
            merchant_id: The CoPilot merchant ID
            
        Returns:
            dict: Contains the unique signing URL
        """
        return self.client.put(f"/merchant/{merchant_id}/signature", {})
    
    def get_signature(self, merchant_id: str) -> dict:
        """
        Get the current signature status.
        
        Args:
            merchant_id: The CoPilot merchant ID
            
        Returns:
            dict: Current signing status
        """
        return self.client.get(f"/merchant/{merchant_id}/signature")
    
    def delete_signature(self, merchant_id: str) -> dict:
        """
        Retract the signature URL and revert merchant to INPROG state.
        
        Args:
            merchant_id: The CoPilot merchant ID
            
        Returns:
            dict: Confirmation
        """
        return self.client.delete(f"/merchant/{merchant_id}/signature")
    
    # =========================================================================
    # ORDER OPERATIONS
    # =========================================================================
    
    def get_order(self, order_id: str) -> dict:
        """
        Get equipment order details.
        
        Args:
            order_id: The order ID
            
        Returns:
            dict: Order details including equipment
        """
        return self.client.get(f"/order/{order_id}")
    
    # =========================================================================
    # ATTACHMENT OPERATIONS
    # =========================================================================
    
    def upload_attachment(self, merchant_id: str, file_path: str, 
                          attachment_type: str, description: str = "") -> dict:
        """
        Upload a document attachment (e.g., voided check).
        
        Args:
            merchant_id: The CoPilot merchant ID
            file_path: Path to the file to upload
            attachment_type: Type code (e.g., 'VOIDBNKCHK1' for voided check)
            description: Optional description
            
        Returns:
            dict: Upload confirmation
        """
        file_path = Path(file_path)
        
        # Read and encode file
        with open(file_path, "rb") as f:
            file_content = base64.b64encode(f.read()).decode("utf-8")
        
        # Determine MIME type
        mime_types = {
            ".pdf": "application/pdf",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
        }
        mime_type = mime_types.get(file_path.suffix.lower(), "application/octet-stream")
        
        payload = {
            "attachment": {
                "fileName": file_path.name,
                "mimeType": mime_type,
                "description": description,
                "attachmentTypeCd": attachment_type,
                "document": file_content
            }
        }
        
        return self.client.put(f"/merchant/{merchant_id}/attachment", payload)


# =============================================================================
# HELPER: Build a merchant payload from individual components
# =============================================================================

def build_merchant_payload(
    # Required business info
    dba_name: str,
    legal_name: str,
    tax_filing_name: str,
    tax_filing_method: str = "EIN",
    
    # Business address
    business_address: dict = None,
    mailing_address: dict = None,
    
    # Owner info
    owner_name: str = None,
    owner_email: str = None,
    owner_phone: str = None,
    owner_title: str = "Owner",
    owner_address: dict = None,
    owner_dob: str = None,
    owner_ssn: str = None,
    drivers_license_number: str = None,
    drivers_license_state: str = None,
    
    # Bank info
    deposit_bank: dict = None,
    withdrawal_bank: dict = None,
    
    # Site user (receives login)
    site_user_first_name: str = None,
    site_user_last_name: str = None,
    site_user_email: str = None,
    
    # Optional extras
    **kwargs
) -> dict:
    """
    Build a merchant payload from individual fields.
    
    This is a helper to make it easier to create merchants without 
    manually building the nested dictionary structure.
    
    Example:
        payload = build_merchant_payload(
            dba_name="My Store",
            legal_name="My Store LLC",
            tax_filing_name="My Store LLC",
            business_address={"address1": "123 Main St", "city": "Denver", ...},
            owner_name="John Doe",
            owner_email="john@example.com",
            ...
        )
        merchant_id = api.create_merchant(payload)
    
    Returns:
        dict: Properly structured merchant payload
    """
    payload = {
        "merchant": {
            "dbaName": dba_name,
            "legalBusinessName": legal_name,
            "taxFilingName": tax_filing_name,
            "taxFilingMethod": tax_filing_method,
        }
    }
    
    merchant = payload["merchant"]
    
    # Demographic
    if business_address or mailing_address:
        merchant["demographic"] = {}
        if business_address:
            merchant["demographic"]["businessAddress"] = business_address
        if mailing_address:
            merchant["demographic"]["mailingAddress"] = mailing_address or business_address
    
    # Ownership
    if owner_name:
        merchant["ownership"] = {
            "owner": {
                "ownerName": owner_name,
                "ownerEmail": owner_email,
                "ownerPhone": owner_phone,
                "ownerTitle": owner_title,
            }
        }
        if owner_address:
            merchant["ownership"]["owner"]["ownerAddress"] = owner_address
        if owner_dob:
            merchant["ownership"]["owner"]["ownerDob"] = owner_dob
        if owner_ssn:
            merchant["ownership"]["owner"]["ownerSSN"] = owner_ssn
        if drivers_license_number:
            merchant["ownership"]["driversLicenseNumber"] = drivers_license_number
        if drivers_license_state:
            merchant["ownership"]["driversLicenseStateCd"] = drivers_license_state
    
    # Bank details
    if deposit_bank or withdrawal_bank:
        merchant["bankDetail"] = {}
        if deposit_bank:
            merchant["bankDetail"]["depositBank"] = deposit_bank
        if withdrawal_bank:
            merchant["bankDetail"]["withdrawalBank"] = withdrawal_bank or deposit_bank
    
    # Site user
    if site_user_email:
        payload["ownerSiteUser"] = {
            "firstName": site_user_first_name or owner_name.split()[0] if owner_name else "",
            "lastName": site_user_last_name or owner_name.split()[-1] if owner_name else "",
            "email": site_user_email
        }
    
    # Add any extra fields
    for key, value in kwargs.items():
        if value is not None:
            merchant[key] = value
    
    return payload
