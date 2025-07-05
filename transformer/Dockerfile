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

# 6. *** THE FIX ***
# Set the default command to run our Python script using spark-submit,
# and pass the driver host configuration directly to the command.
CMD ["spark-submit", "--conf", "spark.driver.host=127.0.0.1", "/app/main.py"]
