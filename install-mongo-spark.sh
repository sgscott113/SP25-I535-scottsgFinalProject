#!/bin/bash
set -eux

# Define the target JAR path
JAR_URL="gs://fbi_nibrs/mongo-spark-connector_2.12-10.4.1.jar"
TARGET_DIR="/usr/lib/spark/jars/"

# Download the MongoDB Spark Connector
gsutil cp "$JAR_URL" "$TARGET_DIR"

echo "MongoDB Spark Connector installed successfully!"