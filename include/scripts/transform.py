import pandas as pd
import json
from pathlib import Path
import boto3
from io import StringIO
import os  

# --- Configuration ---

# Get AWS Credentials from Environment Variables
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET")
S3_FILE_KEY = "processed_movies.csv" # The name of the file in S3

# Define file paths
# /usr/local/airflow/ is the working directory inside the Astro container
BASE_DATA_PATH = Path("/usr/local/airflow/data")
RAW_FILE_PATH = BASE_DATA_PATH / "raw" / "raw_movies.json"

# Define image base URLs
POSTER_BASE_URL = "https://image.tmdb.org/t/p/w342"
PROFILE_BASE_URL = "https://image.tmdb.org/t/p/w185"

def load_data(file_path):
    """Loads the raw JSON data from the file."""
    print(f"Loading raw data from {file_path}...")
    with open(file_path, 'r') as f:
        data = json.load(f)
    print(f"Successfully loaded {len(data)} records.")
    return pd.DataFrame(data)

def clean_and_filter(df):
    """Cleans data, filters for valid entries, and calculates ROI."""
    print("Cleaning and filtering data...")
    
    # Keep only the columns we need for the dashboard
    cols_to_keep = [
        'id', 'title', 'budget', 'revenue', 'release_date', 'vote_average',
        'overview', 'poster_path', 'genres', 'cast'
    ]
    
    # Ensure all columns exist to prevent errors
    for col in cols_to_keep:
        if col not in df.columns:
            df[col] = None
            
    df = df[cols_to_keep]

    # Filter for movies that are "analyzable" for ROI
    # We can't use movies where budget or revenue is 0
    initial_count = len(df)
    df = df[(df['budget'] > 0) & (df['revenue'] > 0)].copy() 
    print(f"Filtered {initial_count - len(df)} movies. {len(df)} analyzable movies remaining.")

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

    # Sort cast by 'order' (their rank in the credits), filtering out any without 'order'
    sorted_cast = sorted([c for c in cast_list if 'order' in c], key=lambda x: x['order'])

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
    print("Enhancing data (URLs, genres, actors)...")

    # 1. Create full poster URL
    df['poster_url'] = df['poster_path'].apply(
        lambda x: f"{POSTER_BASE_URL}{x}" if x else None
    )

    # 2. Flatten genres list (e.g., [{'name': 'Action'}] -> "Action")
    df['genres'] = df['genres'].apply(
        lambda x: ', '.join([g['name'] for g in x]) if isinstance(x, list) and x else None
    )
    
    # 3. Flatten cast list
    actor_df = df['cast'].apply(extract_top_actors)
    
    # We now join this new actor DataFrame back to our main DataFrame
    df = pd.concat([df, actor_df], axis=1)
    
    return df

def save_to_s3(df):
    """
    Saves the final, processed DataFrame to an S3 bucket.
    """
    print(f"Saving processed data to S3 Bucket: {AWS_S3_BUCKET}, Key: {S3_FILE_KEY}")

    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET]):
        print("Error: AWS credentials or bucket name not set. Cannot upload to S3.")
        return

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
    
    # Reorder columns and ensure they all exist
    df = df.reindex(columns=final_columns)
    
    # Create an in-memory CSV file
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Connect to S3 using boto3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # Upload the CSV buffer
    try:
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=S3_FILE_KEY,
            Body=csv_buffer.getvalue()
        )
        print(f"--- Transformation Complete: File uploaded to s3://{AWS_S3_BUCKET}/{S3_FILE_KEY} ---")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def run_transformation():
    """Main orchestration function for the transformation script."""
    df = load_data(RAW_FILE_PATH)
    
    if df is not None:
        df_cleaned = clean_and_filter(df)
        
        if df_cleaned is not None and not df_cleaned.empty:
            df_enhanced = enhance_and_flatten(df_cleaned)
            save_to_s3(df_enhanced)  
        else:
            print("No data to process after cleaning. Exiting.")
    else:
        print("Failed to load data. Exiting.")

# --- Main function for testing ---
if __name__ == "__main__":
    
    print("--- Testing transformation script ---")
    run_transformation()
    print("--- End of test ---")
