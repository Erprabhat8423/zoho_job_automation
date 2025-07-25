import requests
from .auth import get_access_token

class ZohoClient:
    def __init__(self):
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self.headers = {
            "Authorization": f"Zoho-oauthtoken {get_access_token()}",
            "Content-Type": "application/json"
        }

    def get_paginated_data(self, module, fields):
        url = f"{self.base_url}/{module}"
        params = {
            "fields": ",".join(fields),
            "page": 1,
            "per_page": 200
        }
        all_data = []

        while True:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json().get('data', [])
            if not data:
                break
            all_data.extend(data)
            if not response.json().get('info', {}).get('more_records'):
                break
            params["page"] += 1

        return all_data
