### **Project Description: Building a Cost-Free Data Engineering Pipeline for Trading Insights**

This project aims to create a comprehensive and fully automated data engineering pipeline to extract, process, and analyze trading data from the **PredictIt API**, identify significant trading patterns or market swings, and present these insights through a visually engaging, interactive dashboard. The pipeline also enriches these insights by integrating relevant news articles, giving users additional context about trading events.

The development process focuses on **local testing** and eventual deployment to **Google Cloud Platform (GCP)** using its **Always Free Tier**, ensuring that no costs are incurred during the project. All components of the pipeline will be orchestrated with modern data engineering tools and best practices.

#### **Project Objectives**
1. **Data Ingestion**: 
   - Schedule and automate daily data collection from the Finnhub API using Airflow.
   - Save raw data in Google Cloud Storage (GCS) for storage and processing.

2. **Data Processing**:
   - Clean and transform the raw data using **Pandas** to identify trends, anomalies, or patterns in trading activity.
   - Calculate important metrics such as percentage changes, trading swings, and volume patterns.

3. **News Enrichment**:
   - Integrate a **news API** to fetch articles related to trading patterns, ensuring the data insights are contextually meaningful.
   - Match articles with trading data based on keywords or timestamps.

4. **Dashboard Development**:
   - Build a user-friendly dashboard using tools like **Streamlit**, **Dash**, or **Flask** to display data insights and enriched news articles.
   - Design visualizations like trend charts, tables, and filters for dynamic exploration.

5. **Cloud Deployment**:
   - Automate infrastructure provisioning on GCP with **Terraform**, including:
     - A virtual machine (**e2-micro**) to run the Airflow pipeline.
     - **Cloud SQL** (PostgreSQL) for processed data storage.
     - **Firebase Hosting** or **Cloud Run** for deploying the dashboard.
   - Ensure all components operate seamlessly within GCP’s free-tier limits.

#### **Technology Stack**
- **Orchestration**: Airflow for scheduling and managing tasks.
- **Data Storage**: Google Cloud Storage (raw data) and Cloud SQL (processed data).
- **Data Processing**: Python, Pandas, SQLAlchemy.
- **Visualization**: Streamlit or Dash for the dashboard.
- **Infrastructure Management**: Terraform for GCP resource provisioning.
- **APIs**: Finnhub API for trading data, Bing News Search for news enrichment.

#### **Key Highlights**
This project is designed to be an end-to-end solution for ingesting, analyzing, and presenting trading data insights without incurring any costs. It leverages free-tier cloud resources and modern data tools, making it an excellent demonstration of scalable, real-world data engineering practices.

The result will be a fully operational dashboard hosted on GCP, capable of providing dynamic insights into trading patterns with enriched contextual news. This project is ideal for showcasing expertise in data engineering, cloud platforms, and modern development workflows.
