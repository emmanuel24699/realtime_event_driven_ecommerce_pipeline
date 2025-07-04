FROM python:3.9-slim

# Install Java (OpenJDK 17) and other system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Set the working directory
WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY transform.py .

CMD ["python", "transform.py"]
