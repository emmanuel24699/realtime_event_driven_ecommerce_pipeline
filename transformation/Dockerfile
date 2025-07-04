FROM python:3.9-slim

# Install a newer version of Java available in this OS and other dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set the correct JAVA_HOME path for OpenJDK 17
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Set the working directory
WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY transform.py .

# Define the default command
CMD ["python", "transform.py"]