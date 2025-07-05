# 1. Use the official Bitnami Spark image as a base.
FROM bitnami/spark:3.5

# 2. The Bitnami image runs as the 'spark' user (ID 1001).
# We need to switch to the 'root' user temporarily to install our packages.
USER root

# 3. Copy our application files to a directory in the container.
WORKDIR /app
COPY . .

# 4. Install the Python packages listed in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# 5. *** THE FIX ***
# Set the Spark driver host to localhost to prevent networking lookup errors in Fargate.
ENV SPARK_DRIVER_HOST=127.0.0.1

# 6. Switch back to the non-root 'spark' user for security.
USER 1001

# 7. Set the default command to run our Python script using spark-submit.
CMD ["spark-submit", "/app/main.py"]
