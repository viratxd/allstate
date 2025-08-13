import requests
import json
import sys
import os
import logging
from typing import Dict, List, Optional
import time
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('election_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://gateway-voters.eci.gov.in/api/v1"
STATES_ENDPOINT = f"{BASE_URL}/common/states/"
DISTRICTS_ENDPOINT = f"{BASE_URL}/common/districts/{{stateCd}}"
ACS_ENDPOINT = f"{BASE_URL}/common/acs/{{districtCd}}"
PARTS_ENDPOINT = f"{BASE_URL}/printing-publish/get-part-list"
PARTS_PAGE_SIZE = 10  # Kept for consistency with curl payload
DATA_DIR = "data"

# Headers to match curl command exactly
PARTS_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,es;q=0.6",
    "Connection": "keep-alive",
    "Origin": "https://voters.eci.gov.in",
    "Referer": "https://voters.eci.gov.in/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
    "applicationname": "VSP",
    "channelidobo": "VSP",
    "content-type": "application/json",
    "platform-type": "ECIWEB",
    "rtkn_bnd": "null",
    "sec-ch-ua": "\"Opera GX\";v=\"120\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site"
}

def sanitize_filename(name: str) -> str:
    """Sanitize names for use in folder/file names."""
    logger.debug(f"Sanitizing filename: {name}")
    sanitized = re.sub(r'[^a-zA-Z0-9]', '', name.replace(' ', '').replace('.', ''))
    logger.debug(f"Sanitized to: {sanitized}")
    return sanitized

def ensure_directory(path: str):
    """Create directory if it doesn't exist."""
    logger.info(f"Ensuring directory exists: {path}")
    try:
        os.makedirs(path, exist_ok=True)
        logger.debug(f"Directory {path} created or already exists")
    except Exception as e:
        logger.error(f"Error creating directory {path}: {e}")

def save_to_json(data: List[Dict], filename: str):
    """Save data to a JSON file."""
    logger.info(f"Saving data to {filename}")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully saved data to {filename}")
    except Exception as e:
        logger.error(f"Error saving to {filename}: {e}")

def create_session() -> requests.Session:
    """Create and configure a requests Session with retry logic."""
    logger.info("Creating HTTP session")
    session = requests.Session()
    session.headers.update(PARTS_HEADERS)
    # Add retry logic for transient errors
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    logger.debug(f"Session headers: {session.headers}")
    return session

def fetch_states(session: requests.Session) -> Optional[List[Dict]]:
    """Fetch all states from the States API."""
    logger.info(f"Fetching states from {STATES_ENDPOINT}")
    try:
        response = session.get(STATES_ENDPOINT, timeout=10)
        logger.debug(f"States API response status: {response.status_code}")
        response.raise_for_status()
        states = response.json()
        logger.debug(f"Received {len(states)} states")
        filtered_states = [
            {
                "stateCd": state["stateCd"],
                "stateName": state["stateName"],
                "stateNameHindi": state["stateNameHindi"]
            }
            for state in states if state.get("isActive") == "Y"
        ]
        logger.info(f"Filtered {len(filtered_states)} active states")
        return filtered_states
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching states: {e}")
        return None

def fetch_districts(session: requests.Session, state_cd: str) -> Optional[List[Dict]]:
    """Fetch districts for a given state code."""
    url = DISTRICTS_ENDPOINT.format(stateCd=state_cd)
    logger.info(f"Fetching districts for state {state_cd} from {url}")
    try:
        response = session.get(url, timeout=10)
        logger.debug(f"Districts API response status: {response.status_code}")
        response.raise_for_status()
        districts = response.json()
        logger.debug(f"Received {len(districts)} districts for state {state_cd}")
        filtered_districts = [
            {
                "districtCd": district["districtCd"],
                "districtValue": district["districtValue"],
                "districtValueHindi": district["districtValueHindi"]
            }
            for district in districts if district.get("isActive") == "Y"
        ]
        logger.info(f"Filtered {len(filtered_districts)} active districts for state {state_cd}")
        return filtered_districts
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching districts for state {state_cd}: {e}")
        return None

def fetch_assemblies(session: requests.Session, district_cd: str) -> Optional[List[Dict]]:
    """Fetch assembly constituencies for a given district code."""
    url = ACS_ENDPOINT.format(districtCd=district_cd)
    logger.info(f"Fetching assemblies for district {district_cd} from {url}")
    try:
        response = session.get(url, timeout=10)
        logger.debug(f"Assemblies API response status: {response.status_code}")
        response.raise_for_status()
        assemblies = response.json()
        if not assemblies:
            logger.warning(f"No assemblies found for district {district_cd}")
            return []
        logger.debug(f"Received {len(assemblies)} assemblies for district {district_cd}")
        filtered_assemblies = []
        for assembly in assemblies:
            if assembly.get("isActive") != "Y":
                logger.warning(f"Skipping inactive assembly: {assembly.get('asmblyName', 'Unknown')}")
                continue
            if not assembly.get("asmblyName") or not assembly.get("asmblyNo"):
                logger.warning(f"Missing required fields in assembly: {assembly}")
                continue
            filtered_assemblies.append({
                "acNumber": assembly["asmblyNo"],
                "asmblyName": assembly["asmblyName"],
                "asmblyNameL1": assembly["asmblyNameL1"]
            })
        logger.info(f"Filtered {len(filtered_assemblies)} active assemblies for district {district_cd}")
        return filtered_assemblies
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching assemblies for district {district_cd}: {e}")
        return None
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Invalid JSON response for district {district_cd}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching assemblies for district {district_cd}: {e}")
        return None

def fetch_parts(session: requests.Session, state_cd: str, district_cd: str, ac_number: int) -> Optional[List[Dict]]:
    """Fetch all parts (polling stations) for a given AC with a single POST request."""
    logger.info(f"Fetching parts for AC {ac_number}, district {district_cd}, state {state_cd}")
    try:
        payload = {
            "stateCd": str(state_cd),  # Ensure string
            "districtCd": str(district_cd),  # Ensure string
            "acNumber": int(ac_number),  # Ensure int
            "pageNumber": 0,  # Kept for consistency with curl
            "pageSize": PARTS_PAGE_SIZE  # Kept for consistency
        }
        logger.debug(f"Sending POST request to {PARTS_ENDPOINT} with headers: {session.headers}, payload: {payload}")
        logger.debug(f"Session cookies: {session.cookies.get_dict()}")
        response = session.post(PARTS_ENDPOINT, json=payload, timeout=15)
        logger.debug(f"Parts API response status: {response.status_code}")
        if response.status_code == 401:
            logger.error("Unauthorized: API key or token may be required")
            return None
        if response.status_code == 403:
            logger.error(f"Forbidden: Check headers or Origin. Response: {response.text}")
            return None
        if response.status_code == 429:
            logger.warning("Rate limit exceeded. Retrying after 5 seconds...")
            time.sleep(5)
            response = session.post(PARTS_ENDPOINT, json=payload, timeout=15)
            logger.debug(f"Retry response status: {response.status_code}")
        response.raise_for_status()
        try:
            data = response.json()
            logger.debug(f"Parts API response: {data}")
        except ValueError as e:
            logger.error(f"Invalid JSON response for AC {ac_number}, district {district_cd}: {e}, Response: {response.text}")
            return None
        if data.get("status") != "Success" or not data.get("payload"):
            logger.warning(f"No parts found for AC {ac_number}, district {district_cd}. Response: {data}")
            return []
        parts = [
            {
                "partNumber": part["partNumber"],
                "partName": part["partName"]
            }
            for part in data["payload"] if part.get("partName")
        ]
        logger.info(f"Fetched {len(parts)} parts for AC {ac_number}, district {district_cd}")
        return parts if parts else []
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching parts for AC {ac_number}, district {district_cd}: {e}, Response: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching parts for AC {ac_number}, district {district_cd}: {e}")
        return None

def build_election_data(state_cd: str):
    """Build and save election data for a given state code in folder structure."""
    logger.info(f"Starting data collection for state {state_cd}")
    
    # Create session
    session = create_session()
    
    # Fetch and save states
    logger.info("Fetching all states")
    states = fetch_states(session)
    if not states:
        logger.error("Failed to fetch states. Exiting.")
        return
    ensure_directory(DATA_DIR)
    save_to_json(states, f"{DATA_DIR}/allstates.json")
    
    # Find the state
    state = next((s for s in states if s["stateCd"] == state_cd), None)
    if not state:
        logger.error(f"State code {state_cd} not found.")
        return
    state_name = sanitize_filename(state["stateName"])
    state_dir = f"{DATA_DIR}/{state_name}"
    ensure_directory(state_dir)
    
    # Fetch and save districts
    logger.info(f"Fetching districts for state {state_cd}")
    districts = fetch_districts(session, state_cd)
    if districts is None:
        logger.error(f"No districts found for state {state_cd}.")
        return
    save_to_json(districts, f"{state_dir}/districts.json")
    
    # Fetch assemblies and parts for each district
    for district in districts:
        district_name = sanitize_filename(district["districtValue"])
        district_dir = f"{state_dir}/{district_name}"
        ensure_directory(district_dir)
        
        logger.info(f"Fetching assemblies for district {district['districtCd']}")
        assemblies = fetch_assemblies(session, district["districtCd"])
        if assemblies is None:
            logger.error(f"No assemblies found for district {district['districtCd']}.")
            continue
        save_to_json(assemblies, f"{district_dir}/assemblies.json")
        
        # Fetch parts for each assembly
        for assembly in assemblies:
            assembly_name = sanitize_filename(assembly["asmblyName"])
            assembly_dir = f"{district_dir}/{assembly_name}"
            ensure_directory(assembly_dir)
            
            logger.info(f"Fetching parts for AC {assembly['acNumber']}, district {district['districtCd']}")
            parts = fetch_parts(session, state_cd, district["districtCd"], assembly["acNumber"])
            if parts is None:
                logger.error(f"No parts found for AC {assembly['acNumber']}, district {district['districtCd']}.")
                continue
            save_to_json(parts, f"{assembly_dir}/assemblies-part.json")
    
    session.close()
    logger.info("HTTP session closed")

def main():
    """Main function to run the data collection."""
    if len(sys.argv) != 2:
        logger.error("Invalid usage. Usage: python fetch_election_data.py <stateCd>")
        sys.exit(1)
    
    state_cd = sys.argv[1].upper()
    logger.info(f"Starting election data collection for state: {state_cd}")
    build_election_data(state_cd)
    logger.info(f"Completed data collection for state: {state_cd}")

if __name__ == "__main__":
    main()