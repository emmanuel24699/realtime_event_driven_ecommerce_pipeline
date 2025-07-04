# FROM python:3.9-slim

# # Install Java (OpenJDK 17) and other system dependencies
# RUN apt-get update && apt-get install -y \
#     openjdk-17-jre-headless \
#     procps \
#     && rm -rf /var/lib/apt/lists/*

# # Set the JAVA_HOME environment variable
# ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# # Set the working directory
# WORKDIR /app

# # Copy and install Python requirements
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy your application code
# COPY transform.py .

# CMD ["python", "transform.py"]


FROM python:3.9-slim

# Install Java (OpenJDK 17), wget, and other system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Download Delta Lake JAR (adjust version as needed)
ENV DELTA_VERSION=2.4.0
RUN wget -O /opt/delta-core_2.12-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar

# Make sure Spark loads the Delta Lake JAR
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/delta-core_2.12-2.4.0.jar pyspark-shell"

# Set the working directory
WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY transform.py .

# Default command
CMD ["python", "transform.py"]
