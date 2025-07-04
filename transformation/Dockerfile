FROM python:3.9-slim

# Install Java 11 (compatible with Spark 3.x) and dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Fix hostname resolution (for Spark to not fail)
RUN echo "127.0.0.1 $(hostname)" >> /etc/hosts

# Set working directory
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application script
COPY transform.py .

# Set hostname override for Spark
ENV SPARK_LOCAL_HOSTNAME=localhost

# Run the transformation script
CMD ["python", "transform.py"]
