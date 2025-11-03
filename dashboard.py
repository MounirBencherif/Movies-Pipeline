import streamlit as st
import pandas as pd
from pathlib import Path

# --- Configuration ---
# Set the page to "wide" mode to use the full screen
st.set_page_config(layout="wide", page_title="Movie ROI Dashboard")

# --- Data Loading ---
# Define the path to the CSV file
# This dashboard.py file is at the root, so we look inside the 'data' folder
CSV_PATH = "https://raw.githubusercontent.com/MounirBencherif/Movies-Pipeline/178671c149afd93f4bc4a2305d91c7202dd1e2c1/data/processed/processed_movies.csv"

@st.cache_data
def load_data(path):
    """
    Loads the processed CSV data.
    Uses st.cache_data to avoid reloading on every interaction.
    """
    try:
        df = pd.read_csv(path)
        # Convert release_date to datetime objects for better handling
        df['release_date'] = pd.to_datetime(df['release_date'])
        return df
    except FileNotFoundError:
        st.error(f"Error: Processed data file not found at {path}")
        st.info("Please run the Airflow DAG 'movie_roi_pipeline' to generate the data.")
        return None

# Load the data
df = load_data(CSV_PATH)

# Check if data loading was successful
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
            color="genres",  # Color-code by genre
            size="revenue"  # Make bigger revenue bubbles larger
            # Removed 'tooltip' argument to fix the TypeError
        )
        st.caption("Does a bigger budget guarantee a better return? This chart helps answer that.")

    with chart_cols[1]:
        st.subheader("Which Genres Are Most Profitable?")
        
        # We must "explode" the genres column to analyze them individually
        # 'Action, Sci-Fi' -> row 1: 'Action', row 2: 'Sci-Fi'
        try:
            df_genres = df.assign(genre=df['genres'].str.split(', ')).explode('genre')
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
                st.image(row['poster_url'], caption="Poster")

        with detail_cols[1]:
            st.markdown(f"**ROI: {row['ROI']:.1%}**")
            st.markdown(f"**Budget:** ${row['budget']:,}")
            st.markdown(f"**Revenue:** ${row['revenue']:,}")
            st.markdown(f"**Genres:** {row['genres']}")
            st.caption(f"**Overview:** {row['overview']}")

        with detail_cols[2]:
            st.markdown("**Top 3 Cast**")
            if row['actor_1_name']:
                st.image(row['actor_1_image_url'], caption=row['actor_1_name'], width=100)
            if row['actor_2_name']:
                st.image(row['actor_2_image_url'], caption=row['actor_2_name'], width=100)
            if row['actor_3_name']:
                st.image(row['actor_3_image_url'], caption=row['actor_3_name'], width=100)
        
        st.divider()

    # --- 4. The "Raw Data" - Full Data Table ---
    with st.expander("Explore All Processed Data"):
        st.dataframe(df)

else:
    # This message shows if the CSV file hasn't been created yet
    st.info("Waiting for data...")
    st.image("https://placehold.co/1200x400/f8f8f8/c0c0c0?text=Run+Your+Airflow+DAG+to+See+Data+Here", use_column_width=True)


