
FROM bitnami/spark:3.5

# We need to switch to the 'root' user temporarily to install our packages.
USER root

# Copy our application files to a directory in the container.
WORKDIR /app
COPY . .

# Install the Python packages listed in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the non-root 'spark' user for security.
USER 1001

CMD ["spark-submit", "/app/main.py"]
