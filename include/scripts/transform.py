import pandas as pd
import json
from pathlib import Path

# Define file paths
# /usr/local/airflow/ is the working directory inside the Astro container
BASE_DATA_PATH = Path("/usr/local/airflow/data")
RAW_FILE_PATH = BASE_DATA_PATH / "raw" / "raw_movies.json"
PROCESSED_PATH = BASE_DATA_PATH / "processed"
PROCESSED_FILE_PATH = PROCESSED_PATH / "processed_movies.csv"

# Define image base URLs
# w342 is a good size for posters, w185 is good for actor profiles
POSTER_BASE_URL = "https://image.tmdb.org/t/p/w342"
PROFILE_BASE_URL = "https://image.tmdb.org/t/p/w185"

def load_data(file_path):
    """Loads the raw JSON data from the file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except FileNotFoundError:
        print(f"Error: Raw data file not found at {file_path}")
        return None

def clean_and_filter(df):
    """Cleans data, filters for valid entries, and calculates ROI."""
    
    # Keep only the columns we need for the dashboard
    cols_to_keep = [
        'id', 'title', 'budget', 'revenue', 'release_date', 'vote_average',
        'overview', 'poster_path', 'genres', 'cast'
    ]
    df = df[cols_to_keep]

    # Filter for movies that are "analyzable" for ROI
    # We can't use movies where budget or revenue is 0

    df = df[(df['budget'] > 0) & (df['revenue'] > 0)]

    if df.empty:
        print("Warning: No movies left after filtering.")
        return df

    # Create our key business metric: ROI
    # Formula: (Revenue - Budget) / Budget
    df['ROI'] = (df['revenue'] - df['budget']) / df['budget']
    
    return df

def extract_top_actors(cast_list):
    """
    Helper function to extract top 3 actors from the 'cast' list
    and return them as a flat Series.
    """
    actor_data = {
        'actor_1_name': None, 'actor_1_image_url': None,
        'actor_2_name': None, 'actor_2_image_url': None,
        'actor_3_name': None, 'actor_3_image_url': None
    }
    
    if not isinstance(cast_list, list):
        return pd.Series(actor_data)

    # Sort cast by 'order' (their rank in the credits)
    sorted_cast = sorted(cast_list, key=lambda x: x.get('order', 99))

    # Get top 3
    if len(sorted_cast) > 0:
        actor_data['actor_1_name'] = sorted_cast[0].get('name')
        if sorted_cast[0].get('profile_path'):
            actor_data['actor_1_image_url'] = f"{PROFILE_BASE_URL}{sorted_cast[0]['profile_path']}"
            
    if len(sorted_cast) > 1:
        actor_data['actor_2_name'] = sorted_cast[1].get('name')
        if sorted_cast[1].get('profile_path'):
            actor_data['actor_2_image_url'] = f"{PROFILE_BASE_URL}{sorted_cast[1]['profile_path']}"
            
    if len(sorted_cast) > 2:
        actor_data['actor_3_name'] = sorted_cast[2].get('name')
        if sorted_cast[2].get('profile_path'):
            actor_data['actor_3_image_url'] = f"{PROFILE_BASE_URL}{sorted_cast[2]['profile_path']}"
            
    return pd.Series(actor_data)

def enhance_and_flatten(df):
    """Creates full URLs, flattens genres, and flattens cast lists."""

    # 1. Create full poster URL
    df['poster_url'] = df['poster_path'].apply(
        lambda x: f"{POSTER_BASE_URL}{x}" if x else None
    )

    # 2. Flatten genres list (e.g., [{'name': 'Action'}] -> "Action")
    df['genres'] = df['genres'].apply(
        lambda x: ', '.join([g['name'] for g in x]) if isinstance(x, list) else None
    )
    
    # 3. Flatten cast list
    # .apply() on the 'cast' column will call 'extract_top_actors' for each row.
    # The result is a new DataFrame with the 6 actor columns.
    actor_df = df['cast'].apply(extract_top_actors)
    
    # We now join this new actor DataFrame back to our main DataFrame
    df = pd.concat([df, actor_df], axis=1)
    
    return df

def save_data(df, file_path):
    """Saves the final, processed DataFrame to a CSV file."""

    # Ensure the output directory exists
    PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
    
    # Define the final column order for the CSV
    final_columns = [
        'id', 'title', 'budget', 'revenue', 'release_date', 'vote_average', 'overview',
        'poster_url', 'genres', 'ROI',
        'actor_1_name', 'actor_1_image_url',
        'actor_2_name', 'actor_2_image_url',
        'actor_3_name', 'actor_3_image_url'
    ]
    
    # Drop the original "helper" columns we don't need anymore
    df = df.drop(columns=['poster_path', 'cast'], errors='ignore')
    
    # Reorder columns and save
    df = df[final_columns]
    df.to_csv(file_path, index=False)
    print(f"Data saved successfully to {file_path}")

def run_transformation():
    """Main orchestration function for the transformation script."""
    df = load_data(RAW_FILE_PATH)
    
    if df is not None:
        df_cleaned = clean_and_filter(df)
        
        if not df_cleaned.empty:
            df_enhanced = enhance_and_flatten(df_cleaned)
            save_data(df_enhanced, PROCESSED_FILE_PATH)
        else:
            print("No data to process after cleaning. Exiting.")
    else:
        print("Failed to load data. Exiting.")

# --- Main function for testing ---
if __name__ == "__main__":
    """
    This block runs only when you execute the script directly
    (e.g., 'python include/scripts/transform.py')
    """
    print("--- Testing transformation script ---")
    run_transformation()
    print("--- End of test ---")