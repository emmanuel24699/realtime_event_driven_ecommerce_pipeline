FROM python:3.9-slim

# --- Install Java ---
# 1. Update package lists
RUN apt-get update

# 2. Install OpenJDK 11 (a common choice for Spark) without interactive prompts
RUN apt-get install -y openjdk-11-jre-headless

# 3. Set the JAVA_HOME environment variable for PySpark to find it
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# --- Install Python Dependencies ---
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
