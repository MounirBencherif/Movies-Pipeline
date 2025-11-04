# 🎬 Movie ROI Data Pipeline

An end-to-end data pipeline built to provide actionable **business insights for movie producers**. This project automates the process of fetching, transforming, and visualizing movie financial data, focusing on Return on Investment (ROI) rather than just gross revenue.

Check out the live dashboard here: https://movies-roi.streamlit.app

## 🚀 Project Architecture

<p align="center">
  <img src="Assets/Project Architecture.png" alt="App Preview"width="600"//>
</p>

This project ingests raw data, processes it in an automated pipeline, stores it in cloud storage, and serves it to a business-facing dashboard.

- **Orchestration:** Apache Airflow (running via the Astro CLI in Docker) manages the entire workflow.

- **Extract:** A Python script calls the TMDB API to fetch the top-grossing US films from the last 90 days.

- **Transform:** A Pandas script cleans the data, filters for analyzable films, calculates ROI, and flattens nested JSON (for genres and cast) into a clean, flat table.

- **Load:** The final, processed CSV is uploaded directly to a private AWS S3 bucket.

- **Visualize:** A Streamlit app reads the data directly and securely from S3, providing an interactive dashboard for producers.

## ✨ Key Features

- **Automated ETL Pipeline:** Uses Apache Airflow to schedule and trigger the data pipeline on demand.

- **Business-Centric Transformation:** Leverages Pandas to calculate the key business metric **ROI** and extracts poster images and top-3 cast members for a rich UI.

- **Cloud-Native Storage:** Uses **AWS S3** as a decoupled, scalable data-storage layer.

- **Secure & Interactive Dashboard:** The Streamlit app uses st.secrets to securely connect to the private S3 bucket, ensuring credentials are never exposed.

## 📊 Dashboard Preview

<p align="center">
  <img src="Assets/dash 1.png" alt="App Preview"width="700"//>
</p>

<p align="center">
  <img src="Assets/dash 2.png" alt="App Preview"width="700"//>
</p>
