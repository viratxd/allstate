import requests
import json
import sys
import os
from typing import Dict, List, Optional
import time
import re

# Configuration
BASE_URL = "https://gateway-voters.eci.gov.in/api/v1"
STATES_ENDPOINT = f"{BASE_URL}/common/states/"
DISTRICTS_ENDPOINT = f"{BASE_URL}/common/districts/{{stateCd}}"
ACS_ENDPOINT = f"{BASE_URL}/common/acs/{{districtCd}}"
PARTS_ENDPOINT = f"{BASE_URL}/printing-publish/get-part-list"
PARTS_PAGE_SIZE = 10  # Default page size for Parts API
DATA_DIR = "data"

# Headers for Parts API
PARTS_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,es;q=0.6",
    "applicationname": "VSP",
    "atkn_bnd": "null",
    "channelidobo": "VSP",
    "content-type": "application/json",
    "platform-type": "ECIWEB",
    "rtkn_bnd": "null",
    "sec-ch-ua": '"Opera GX";v="120", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site"
}

def sanitize_filename(name: str) -> str:
    """Sanitize names for use in folder/file names."""
    # Replace spaces and special characters with underscores, remove invalid chars
    return re.sub(r'[^a-zA-Z0-9]', '', name.replace(' ', '').replace('.', ''))

def ensure_directory(path: str):
    """Create directory if it doesn't exist."""
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        print(f"Error creating directory {path}: {e}")

def save_to_json(data: List[Dict], filename: str):
    """Save data to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {filename}")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def fetch_states() -> Optional[List[Dict]]:
    """Fetch all states from the States API."""
    try:
        response = requests.get(STATES_ENDPOINT, timeout=10)
        response.raise_for_status()
        states = response.json()
        return [
            {
                "stateCd": state["stateCd"],
                "stateName": state["stateName"],
                "stateNameHindi": state["stateNameHindi"]
            }
            for state in states if state.get("isActive") == "Y"
        ]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching states: {e}")
        return None

def fetch_districts(state_cd: str) -> Optional[List[Dict]]:
    """Fetch districts for a given state code."""
    try:
        url = DISTRICTS_ENDPOINT.format(stateCd=state_cd)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        districts = response.json()
        return [
            {
                "districtCd": district["districtCd"],
                "districtValue": district["districtValue"],
                "districtValueHindi": district["districtValueHindi"]
            }
            for district in districts if district.get("isActive") == "Y"
        ]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching districts for state {state_cd}: {e}")
        return None

def fetch_assemblies(district_cd: str) -> Optional[List[Dict]]:
    """Fetch assembly constituencies for a given district code."""
    try:
        url = ACS_ENDPOINT.format(districtCd=district_cd)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        assemblies = response.json()
        return [
            {
                "acNumber": assembly["asmblyNo"],
                "asmblyName": assembly["asmblyName"],
                "asmblyNameL1": assembly["asmblyNameL1"]
            }
            for assembly in assemblies if assembly.get("isActive") == "Y"
        ]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching assemblies for district {district_cd}: {e}")
        return None

def fetch_parts(state_cd: str, district_cd: str, ac_number: int) -> Optional[List[Dict]]:
    """Fetch all parts (polling stations) for a given AC, handling pagination."""
    parts = []
    page_number = 0
    while True:
        try:
            payload = {
                "stateCd": state_cd,
                "districtCd": district_cd,
                "acNumber": ac_number,
                "pageNumber": page_number,
                "pageSize": PARTS_PAGE_SIZE
            }
            response = requests.post(PARTS_ENDPOINT, headers=PARTS_HEADERS, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("status") != "Success" or not data.get("payload"):
                print(f"No more parts for AC {ac_number}, district {district_cd}")
                break
            parts.extend([
                {
                    "partNumber": part["partNumber"],
                    "partName": part["partName"]
                }
                for part in data["payload"]
            ])
            page_number += 1
            if len(data["payload"]) < PARTS_PAGE_SIZE:
                break
            time.sleep(1)  # Avoid rate-limiting
        except requests.exceptions.RequestException as e:
            print(f"Error fetching parts for AC {ac_number}, district {district_cd}, page {page_number}: {e}")
            break
    return parts if parts else None

def build_election_data(state_cd: str):
    """Build and save election data for a given state code in folder structure."""
    # Fetch and save states
    states = fetch_states()
    if not states:
        print("Failed to fetch states. Exiting.")
        return
    ensure_directory(DATA_DIR)
    save_to_json(states, f"{DATA_DIR}/allstates.json")
    
    # Find the state
    state = next((s for s in states if s["stateCd"] == state_cd), None)
    if not state:
        print(f"State code {state_cd} not found.")
        return
    state_name = sanitize_filename(state["stateName"])
    state_dir = f"{DATA_DIR}/{state_name}"
    ensure_directory(state_dir)
    
    # Fetch and save districts
    districts = fetch_districts(state_cd)
    if districts is None:
        print(f"No districts found for state {state_cd}.")
        return
    save_to_json(districts, f"{state_dir}/districts.json")
    
    # Fetch assemblies and parts for each district
    for district in districts:
        district_name = sanitize_filename(district["districtValue"])
        district_dir = f"{state_dir}/{district_name}"
        ensure_directory(district_dir)
        
        assemblies = fetch_assemblies(district["districtCd"])
        if assemblies is None:
            print(f"No assemblies found for district {district['districtCd']}.")
            continue
        save_to_json(assemblies, f"{district_dir}/assemblies.json")
        
        # Fetch parts for each assembly
        for assembly in assemblies:
            assembly_name = sanitize_filename(assembly["asmblyName"])
            assembly_dir = f"{district_dir}/{assembly_name}"
            ensure_directory(assembly_dir)
            
            parts = fetch_parts(state_cd, district["districtCd"], assembly["acNumber"])
            if parts is None:
                print(f"No parts found for AC {assembly['acNumber']}, district {district['districtCd']}.")
                continue
            save_to_json(parts, f"{assembly_dir}/assemblies-part.json")

def main():
    """Main function to run the data collection."""
    if len(sys.argv) != 2:
        print("Usage: python fetch_election_data.py <stateCd>")
        sys.exit(1)
    
    state_cd = sys.argv[1].upper()
    print(f"Fetching data for state: {state_cd}")
    build_election_data(state_cd)

if __name__ == "__main__":
    main()