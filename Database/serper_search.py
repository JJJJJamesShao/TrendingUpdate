import http.client
import json
import argparse
from dotenv import load_dotenv
import os
import sys

# Determine the correct path to .env file
# When imported as module from parent: use parent directory
# When run directly: use current directory or parent directory
if __name__ == '__main__':
    # Running directly from Database folder
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
else:
    # Imported as module from parent
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')

# Load environment variables from .env
load_dotenv(dotenv_path=env_path)

SERPER_CONFIG = {
    'SERPER_API_KEY': os.getenv('SERPER_API_KEY')
}


def search(query: str, tbs: str = "qdr:d", page: int = 1) -> dict:
    """
    Search using Serper API.
    
    Args:
        query: Search query string
        tbs: Time-based search filter (default: "qdr:d" for past day)
        page: Page number (default: 1)
    
    Returns:
        dict: JSON response from Serper API
    """
    conn = http.client.HTTPSConnection("google.serper.dev")
    
    payload = json.dumps({
        "q": query,
        "tbs": tbs,
        "page": page
    })
    
    headers = {
        'X-API-KEY': SERPER_CONFIG['SERPER_API_KEY'],
        'Content-Type': 'application/json'
    }
    
    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    data = res.read()
    
    return json.loads(data.decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description='Serper Search Tool')
    parser.add_argument('--test', type=str, metavar='QUERY', help='Test search with a query string')
    
    args = parser.parse_args()
    
    if args.test:
        print(f"Searching for: {args.test}\n")
        result = search(args.test)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
