FROM python:3.9-slim

# Install Java and other system dependencies required by Spark
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable for Spark to find Java
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Set the working directory
WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY transform.py .

# Define the default command
CMD ["python", "transform.py"]