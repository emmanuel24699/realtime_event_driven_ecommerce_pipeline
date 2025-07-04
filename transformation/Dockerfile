FROM python:3.9-slim

# Install Java (OpenJDK 17) and dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 17
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

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
