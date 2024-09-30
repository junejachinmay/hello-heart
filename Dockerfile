# Use the official Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install Java (for PySpark) and any required packages
RUN apt-get update && apt-get install -y openjdk-11-jdk curl && apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables for AWS (LocalStack uses these)
ENV AWS_ACCESS_KEY_ID=test
ENV AWS_SECRET_ACCESS_KEY=test
ENV AWS_DEFAULT_REGION=us-east-1

# Expose any necessary ports (if needed)
# EXPOSE 8080

# Command to run your application
CMD ["python", "etl_pipeline.py"]
