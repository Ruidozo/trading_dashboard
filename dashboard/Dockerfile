FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app files
COPY . .

# Copy the .env file to the container
COPY .env .env

# Expose the port specified by the PORT environment variable
ENV PORT 8080
EXPOSE 8080

# Run the Streamlit app
CMD ["sh", "-c", "streamlit run app.py --server.port=$PORT --server.address=0.0.0.0"]