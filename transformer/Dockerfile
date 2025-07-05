# transformer/Dockerfile

# 1. Use the official Bitnami Spark image as a base.
FROM bitnami/spark:3.5

# 2. Switch to the 'root' user to install packages.
USER root

# 3. Copy our application files into the container.
WORKDIR /app
COPY . .

# 4. Install Python packages from requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# 5. Switch back to the non-root 'spark' user for security.
USER 1001

# 6. *** THE FINAL FIX ***
# Set the default command to run our Python script using spark-submit.
# --packages: Tells Spark to download the required Java libraries (JARs) for Delta Lake and S3.
# --conf: Sets the driver host to prevent networking errors in Fargate.
CMD [ \
  "spark-submit", \
  "--packages", \
  "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
  "--conf", \
  "spark.driver.host=127.0.0.1", \
  "/app/main.py" \
]
