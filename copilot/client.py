"""
Base CoPilot API Client

Handles authentication headers and HTTP requests.
"""

import os
import time
import requests
from dotenv import load_dotenv
from .auth import TokenManager


class CoPilotClient:
    """
    Base client for CoPilot API requests.
    
    Handles:
    - Loading credentials from .env
    - Token management (auto-refresh)
    - Adding required headers to all requests
    - HTTP methods (GET, POST, PUT, DELETE)
    
    Usage:
        client = CoPilotClient()
        response = client.get("/merchant/12345")
    """
    
    def __init__(self, env_path: str = None):
        """
        Initialize the CoPilot client.
        
        Args:
            env_path: Optional path to .env file. If not provided,
                      looks for .env in current directory.
        """
        # Load environment variables
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()
        
        # Load configuration
        self.api_url = os.getenv("COPILOT_API_URL", "https://api-uat.cardconnect.com")
        self.sales_code = os.getenv("COPILOT_SALES_CODE")
        self.template_id = os.getenv("COPILOT_TEMPLATE_ID")
        self.timeout_seconds = float(os.getenv("COPILOT_TIMEOUT_SECONDS", "30"))
        self.max_retries = int(os.getenv("COPILOT_MAX_RETRIES", "4"))
        self.retry_backoff_seconds = float(os.getenv("COPILOT_RETRY_BACKOFF_SECONDS", "2"))
        
        # Initialize token manager
        self._token_manager = TokenManager(
            auth_url=os.getenv("COPILOT_AUTH_URL"),
            client_id=os.getenv("COPILOT_CLIENT_ID"),
            client_secret=os.getenv("COPILOT_CLIENT_SECRET"),
            username=os.getenv("COPILOT_USERNAME"),
            password=os.getenv("COPILOT_PASSWORD"),
        )
    
    def _get_headers(self) -> dict:
        """
        Build the headers required for API requests.
        
        Returns:
            dict: Headers including Authorization and API version
        """
        token = self._token_manager.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-CopilotAPI-Version": "1.0"
        }
    
    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """
        Make an HTTP request to the CoPilot API.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., "/merchant/12345")
            data: Optional JSON data for POST/PUT requests
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            Exception: If the request fails
        """
        url = f"{self.api_url}{endpoint}"
        retryable_statuses = {401, 429, 500, 502, 503, 504}

        for attempt in range(self.max_retries + 1):
            headers = self._get_headers()
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
                    raise Exception(f"CoPilot request failed after retries: {exc}") from exc
                time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                continue

            if response.status_code == 401:
                self._token_manager.clear_token()

            if response.status_code < 400:
                if response.text:
                    return response.json()
                return {}

            if response.status_code in retryable_statuses and attempt < self.max_retries:
                time.sleep(self.retry_backoff_seconds * (2 ** attempt))
                continue

            raise Exception(f"API Error {response.status_code}: {response.text}")
    
    def get(self, endpoint: str) -> dict:
        """Make a GET request."""
        return self._request("GET", endpoint)
    
    def post(self, endpoint: str, data: dict) -> dict:
        """Make a POST request with JSON data."""
        return self._request("POST", endpoint, data)
    
    def put(self, endpoint: str, data: dict) -> dict:
        """Make a PUT request with JSON data."""
        return self._request("PUT", endpoint, data)
    
    def delete(self, endpoint: str) -> dict:
        """Make a DELETE request."""
        return self._request("DELETE", endpoint)
