#Imports 
import os
import requests
import datetime
import json
import time
from pathlib import Path

# --- Configuration & Setup ---

# Read the API key securely from the environment
API_KEY = os.environ.get("TMDB_API_KEY")

# Set all URLs
BASE_API_URL = "https://api.themoviedb.org/3" #Api base url 
DISCOVER_URL = f"{BASE_API_URL}/discover/movie" # To get trending movies
DETAILS_URL_TEMPLATE = f"{BASE_API_URL}/movie/{{movie_id}}" #to get IDs
CREDITS_URL_TEMPLATE = f"{BASE_API_URL}/movie/{{movie_id}}/credits" # To get actors

# Define file paths for saving data
RAW_DATA_PATH = Path("/usr/local/airflow/data/raw")
OUTPUT_FILE = RAW_DATA_PATH / "raw_movies.json"

def validate_api_key():
    """Helper function to check the API key."""
    if not API_KEY:
        print("Error: TMDB_API_KEY environment variable not set.")
        raise ValueError("TMDB_API_KEY environment variable not set.")
    
def fetch_movie_ids():
    """
    Step 1: Fetches the IDs of top-grossing movies from the last 90 days.
    """
    today = datetime.date.today()
    ninety_days_ago = today - datetime.timedelta(days=90)
    
    discover_params = {
        "api_key": API_KEY,
        "sort_by": "revenue.desc",
        "primary_release_date.gte": ninety_days_ago.isoformat(),
        "primary_release_date.lte": today.isoformat(),
        "region": "US",            
        "with_original_language": "en",
        "page": 1,
        "include_adult": False,
    }

    print(f"Fetching movie list released between {ninety_days_ago.isoformat()} and {today.isoformat()}...")
    try:
        response = requests.get(DISCOVER_URL, params=discover_params)
        response.raise_for_status() # Check for HTTP errors
        discover_data = response.json()
        
        movie_ids = [movie['id'] for movie in discover_data.get('results', [])]
        return movie_ids
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movie list from TMDB: {e}")
        return []
    
def fetch_data_for_movie(movie_id):
    """
    Step 2: Fetches both Details (budget, revenue) and Credits (actors)
    for a single movie ID.
    """
    details_params = {"api_key": API_KEY, "language": "en-US"}
    credits_params = {"api_key": API_KEY}
    
    details_url = DETAILS_URL_TEMPLATE.format(movie_id=movie_id)
    credits_url = CREDITS_URL_TEMPLATE.format(movie_id=movie_id)

    try:
        # Fetch Details
        details_response = requests.get(details_url, params=details_params)
        details_response.raise_for_status()
        details_data = details_response.json()
        
        # Fetch Credits
        credits_response = requests.get(credits_url, params=credits_params)
        credits_response.raise_for_status()
        credits_data = credits_response.json()
        
        # Merge the two JSON objects into one
        # We take everything from 'details' and add the 'cast' from 'credits'
        details_data['cast'] = credits_data.get('cast', [])
        
        return details_data
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for movie ID {movie_id}: {e}")
        return None
    
def run_extraction():

    validate_api_key()
    
    # Get all movie IDs
    movie_ids = fetch_movie_ids()
    
    all_movie_data = []
    for i, movie_id in enumerate(movie_ids):
        movie_data = fetch_data_for_movie(movie_id)
        if movie_data:
            all_movie_data.append(movie_data)
        # IMPORTANT: Respect API rate limits
        time.sleep(0.3) 

    if not all_movie_data:
        print("No detailed movie data was fetched.")
        return

    print(f"Successfully fetched full data for {len(all_movie_data)} movies.")
    
    # Step 3: Save the data to a file
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_movie_data, f, indent=4)
        
    print(f"Data successfully saved to {OUTPUT_FILE}")

# --- 4. Main function for testing ---
if __name__ == "__main__":
    """
    This block runs only when you execute the script directly
    (e.g., 'python include/scripts/extract.py')
    """
    print("--- Testing extraction script ---")
    run_extraction()
    print("--- End of test ---")
