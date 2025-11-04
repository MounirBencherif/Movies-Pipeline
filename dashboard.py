import streamlit as st
import pandas as pd
import boto3
from io import StringIO

# --- Configuration ---
st.set_page_config(layout="wide", page_title="Movie ROI Dashboard")

# --- AWS S3 Configuration ---
# These will be read from Streamlit's "Secrets Management"
AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = st.secrets.get("AWS_S3_BUCKET")
S3_FILE_KEY = "processed_movies.csv"

@st.cache_data(ttl=3600) # Cache the data for 1 hour
def load_data_from_s3():
    """
    Connects to S3 using boto3 and st.secrets, then loads the CSV
    file into a Pandas DataFrame.
    """
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET]):
        st.error("AWS credentials or bucket name not found in Streamlit Secrets.")
        return None

    try:
        # Connect to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        # Get the object from S3
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=S3_FILE_KEY)
        
        # Read the object's body into a string
        csv_string = response.get("Body").read().decode('utf-8')
        
        # Read the string into a Pandas DataFrame
        df = pd.read_csv(StringIO(csv_string))
        
        # Convert release_date to datetime objects for better handling
        df['release_date'] = pd.to_datetime(df['release_date'])
        return df
        
    except Exception as e:
        st.error(f"Error loading data from S3: {e}")
        return None

# Load the data
df = load_data_from_s3()

# --- Build the Dashboard ---
if df is not None and not df.empty:

    # --- 1. Main Title & KPIs ---
    st.title("Movie ROI & Talent Dashboard 🎬")
    st.markdown("Business insights from the top-grossing films of the last 90 days.")

    # Sort data for KPIs
    df_sorted_roi = df.sort_values("ROI", ascending=False)
    df_sorted_revenue = df.sort_values("revenue", ascending=False)

    # Get the top performers
    best_roi_movie = df_sorted_roi.iloc[0]
    highest_gross_movie = df_sorted_revenue.iloc[0]
    average_roi = df['ROI'].mean()

    st.divider()

    # Display KPIs
    kpi_cols = st.columns(3)
    kpi_cols[0].metric(
        label="🏆 Best ROI",
        value=best_roi_movie['title'],
        help=f"This film had an ROI of {best_roi_movie['ROI']:.1%}",
        delta=f"{best_roi_movie['ROI']:.1%}",
    )
    kpi_cols[1].metric(
        label="💰 Highest Gross Revenue",
        value=highest_gross_movie['title'],
        help=f"This film grossed ${highest_gross_movie['revenue']:,}",
        delta=f"${highest_gross_movie['revenue'] / 1_000_000:.1f} M",
    )
    kpi_cols[2].metric(
        label="📊 Average ROI",
        value=f"{average_roi:.1%}",
        help="The average ROI for all analyzed movies.",
    )

    st.divider()

    # --- 2. The "Why" - Business Analysis Charts ---
    st.header("Why Are These Films Successful?")
    chart_cols = st.columns(2)

    with chart_cols[0]:
        st.subheader("Budget vs. ROI")
        st.scatter_chart(
            df,
            x="budget",
            y="ROI",
            color="title",  # Color-code by genre
            size="revenue"  # Make bigger revenue bubbles larger
        )
        st.caption("Does a bigger budget guarantee a better return? This chart helps answer that.")

    with chart_cols[1]:
        st.subheader("Which Genres Are Most Profitable?")
        
        try:
            # Create a "long" dataframe by exploding the genres list
            df_genres = df.assign(genre=df['genres'].str.split(', ')).explode('genre')
            # Calculate mean ROI for each genre
            df_genre_roi = df_genres.groupby('genre')['ROI'].mean().sort_values(ascending=False)
            
            st.bar_chart(df_genre_roi)
            st.caption("Average ROI for each individual genre.")
        except Exception as e:
            st.warning(f"Could not generate genre chart: {e}")

    st.divider()

    # --- 3. The "What & Who" - Top 5 Movie Deep-Dive ---
    st.header("Top 5 Most Profitable Films (by ROI)")

    # Get top 5 movies by ROI
    top_5_roi = df_sorted_roi.head(5)

    for i, row in top_5_roi.iterrows():
        st.subheader(f"{(df_sorted_roi.index.get_loc(i))+1}. {row['title']}")
        
        # Use columns for a clean layout: [Poster | Details | Cast]
        detail_cols = st.columns([1, 2, 1])
        
        with detail_cols[0]:
            if row['poster_url']:
                st.image(row['poster_url'], caption="Poster", use_column_width="auto")

        with detail_cols[1]:
            st.markdown(f"**ROI: {row['ROI']:.1%}**")
            st.markdown(f"**Budget:** ${row['budget']:,}")
            st.markdown(f"**Revenue:** ${row['revenue']:,}")
            st.markdown(f"**Genres:** {row['genres']}")
            st.caption(f"**Overview:** {row['overview']}")

        with detail_cols[2]:
            st.markdown("**Top 3 Cast**")
            if row['actor_1_name']:
                st.image(row['actor_1_image_url'], caption=row['actor_1_name'], use_column_width="auto")
            if row['actor_2_name']:
                st.image(row['actor_2_image_url'], caption=row['actor_2_name'], use_column_width="auto")
            if row['actor_3_name']:
                st.image(row['actor_3_image_url'], caption=row['actor_3_name'], use_column_width="auto")
        
        st.divider()

    # --- 4. The "Raw Data" - Full Data Table (UPDATED) ---
    with st.expander("Explore All Movie ROI Rankings"):
        # Select, sort, and reset index
        df_table = df[['title', 'ROI']].sort_values("ROI", ascending=False)
        
        # Format the ROI column to look like a percentage
        df_table['ROI'] = df_table['ROI'].map('{:,.1%}'.format)
        
        # Hide the index for a cleaner look
        st.dataframe(df_table, use_container_width=True, hide_index=True)

else:
    # This message shows if data loading failed
    st.error("Could not load data from S3.")
    st.info("Please ensure AWS credentials are set in Streamlit Secrets and the bucket/file key are correct.")



