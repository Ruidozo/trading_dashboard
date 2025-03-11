To deploy your **Streamlit dashboard** on **Cloud Run**, follow these steps:

---

## **1️⃣ Update the Dockerfile for Cloud Run**
Ensure your **Dockerfile** for the Streamlit app is structured correctly. Modify or create a `Dockerfile` inside your **dashboard** directory:

```dockerfile
# Use official Streamlit image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . .

# Expose the default Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

---

## **2️⃣ Build & Push the Docker Image to Google Container Registry (GCR)**

Authenticate to **Google Cloud**:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

Enable **Container Registry** and **Cloud Run**:

```bash
gcloud services enable artifactregistry.googleapis.com run.googleapis.com
```

Build the Docker image:

```bash
docker build -t gcr.io/YOUR_PROJECT_ID/streamlit-app .
```

Push the image to **GCR**:

```bash
docker push gcr.io/YOUR_PROJECT_ID/streamlit-app
```

---

## **3️⃣ Deploy to Cloud Run**

Deploy the containerized Streamlit app:

```bash
gcloud run deploy streamlit-app \
  --image gcr.io/YOUR_PROJECT_ID/streamlit-app \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

💡 **Flags Explanation:**
- `--image`: Specifies the Docker image.
- `--platform managed`: Deploys on **fully managed Cloud Run**.
- `--region`: Adjust this to your preferred **GCP region**.
- `--allow-unauthenticated`: Allows anyone to access the dashboard (you can modify this for authentication).

---

## **4️⃣ Get the Cloud Run URL**
Once deployed, Google Cloud Run will return a **public URL** (e.g., `https://streamlit-app-xyz.a.run.app`). Open it in your browser to verify the deployment.

---