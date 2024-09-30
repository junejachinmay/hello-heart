# Use the official Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install Java (for PySpark) and required packages
# Slim images may not have certain tools, so we add them first (e.g. gnupg, apt-transport-https)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    apt-transport-https \
    ca-certificates \
    build-essential \
    curl \
    python3-dev

# Add Adoptium GPG key and repository
RUN mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb buster main" | tee /etc/apt/sources.list.d/adoptium.list

# Install Temurin JDK
RUN apt-get update && \
    apt-get install -y temurin-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Hadoop and related AWS libraries for S3 support
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz && \
    tar -xzf hadoop-3.2.0.tar.gz && \
    mv hadoop-3.2.0 /usr/local/hadoop && \
    rm hadoop-3.2.0.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Install the necessary Hadoop AWS and AWS SDK dependencies
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P $HADOOP_HOME/share/hadoop/tools/lib/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.888/aws-java-sdk-bundle-1.11.888.jar -P $HADOOP_HOME/share/hadoop/tools/lib/

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables for AWS (LocalStack uses these)
ENV AWS_ACCESS_KEY_ID=test
ENV AWS_SECRET_ACCESS_KEY=test
ENV AWS_DEFAULT_REGION=us-east-1

# Command to run your application
CMD ["python", "etl_pipeline.py"]
