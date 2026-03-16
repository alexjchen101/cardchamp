"""
Token Manager for CoPilot API OAuth2 Authentication

Handles token fetching and auto-refresh when expired.
"""

import requests
import time


class TokenManager:
    """
    Manages OAuth2 access tokens for the CoPilot API.
    
    Automatically fetches a new token when:
    - No token exists yet
    - Current token has expired (with 60-second buffer)
    """
    
    def __init__(self, auth_url: str, client_id: str, client_secret: str, 
                 username: str, password: str):
        """
        Initialize the token manager.
        
        Args:
            auth_url: The OAuth2 token endpoint URL
            client_id: Your API client ID (e.g., 'merchapi')
            client_secret: Your API client secret
            username: Your API username
            password: Your API password
        """
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        
        # Token storage
        self._access_token = None
        self._token_expiry = 0  # Unix timestamp when token expires
    
    def get_token(self) -> str:
        """
        Get a valid access token.
        
        Returns a cached token if still valid, otherwise fetches a new one.
        
        Returns:
            str: A valid access token
            
        Raises:
            Exception: If token fetch fails
        """
        # Check if we have a valid token (with 60-second buffer)
        if self._access_token and time.time() < (self._token_expiry - 60):
            return self._access_token
        
        # Need to fetch a new token
        self._fetch_new_token()
        return self._access_token
    
    def _fetch_new_token(self):
        """
        Fetch a new access token from the OAuth2 endpoint.
        """
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = requests.post(self.auth_url, data=payload, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get token: {response.status_code} - {response.text}")
        
        data = response.json()
        
        self._access_token = data["access_token"]
        # Calculate expiry time (expires_in is in seconds)
        expires_in = data.get("expires_in", 300)  # Default 5 minutes
        self._token_expiry = time.time() + expires_in
        
        print(f"[TokenManager] New token acquired, expires in {expires_in} seconds")
    
    def clear_token(self):
        """
        Clear the cached token (useful if you get a 401 error).
        """
        self._access_token = None
        self._token_expiry = 0
