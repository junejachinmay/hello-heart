# Use an official Python runtime as a parent image
FROM python:3.9

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Set PATH to include Java
ENV PATH $PATH:$JAVA_HOME/bin

# Install PySpark and other Python dependencies
RUN pip install pyspark pandas pyarrow boto3

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Run etl_pipeline.py when the container launches
CMD ["python", "etl_pipeline.py"]