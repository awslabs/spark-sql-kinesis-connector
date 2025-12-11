#!/bin/bash

# Run script for Kinesis Connector Smoke Tests
# Executes all smoke tests against a Kinesis stream

set -e

# Parse arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <stream-name> <region> [consumer-name]"
    echo
    echo "Arguments:"
    echo "  stream-name    - Name of the Kinesis stream to test"
    echo "  region         - AWS region (e.g., us-east-2)"
    echo "  consumer-name  - EFO consumer name (default: smoke-test-consumer)"
    echo
    echo "Example:"
    echo "  $0 spark4-smoke-test us-east-2 my-consumer"
    exit 1
fi

STREAM_NAME=$1
REGION=$2
CONSUMER_NAME=${3:-smoke-test-consumer}

echo "Kinesis Connector Smoke Tests"
echo "=============================="
echo
echo "Configuration:"
echo "  Stream Name: $STREAM_NAME"
echo "  Region: $REGION"
echo "  Consumer Name: $CONSUMER_NAME"
echo

# Check prerequisites
if [ -z "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME is not set"
    echo "Please set SPARK_HOME to your Spark 4.0.0 installation"
    exit 1
fi

if [ ! -f "target/smoke-test.jar" ]; then
    echo "ERROR: smoke-test.jar not found"
    echo "Please build the smoke tests first:"
    echo "  ./build-smoke-tests.sh"
    exit 1
fi

CONNECTOR_JAR="../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar"
if [ ! -f "$CONNECTOR_JAR" ]; then
    echo "ERROR: Connector JAR not found at $CONNECTOR_JAR"
    echo "Please build the connector first:"
    echo "  cd .."
    echo "  mvn clean package -DskipTests"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS credentials not configured"
    echo "Please configure AWS credentials via:"
    echo "  - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
    echo "  - AWS credentials file (~/.aws/credentials)"
    echo "  - IAM role (if running on EC2/ECS/EKS)"
    exit 1
fi

echo "Prerequisites check:"
echo "  ✓ SPARK_HOME: $SPARK_HOME"
echo "  ✓ Smoke test JAR: target/smoke-test.jar"
echo "  ✓ Connector JAR: $CONNECTOR_JAR"
echo "  ✓ AWS credentials configured"
echo

# Check if stream exists
echo "Checking if stream exists..."
if ! aws kinesis describe-stream --stream-name "$STREAM_NAME" --region "$REGION" &> /dev/null; then
    echo "WARNING: Stream '$STREAM_NAME' not found in region '$REGION'"
    echo
    read -p "Create stream now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Creating stream..."
        aws kinesis create-stream \
            --stream-name "$STREAM_NAME" \
            --shard-count 2 \
            --region "$REGION"
        
        echo "Waiting for stream to become ACTIVE..."
        aws kinesis wait stream-exists \
            --stream-name "$STREAM_NAME" \
            --region "$REGION"
        
        echo "✓ Stream created and active"
    else
        echo "Exiting. Please create the stream manually:"
        echo "  aws kinesis create-stream --stream-name $STREAM_NAME --shard-count 2 --region $REGION"
        exit 1
    fi
else
    echo "✓ Stream exists"
fi

echo
echo "=============================="
echo "Running Smoke Tests"
echo "=============================="
echo

# Run smoke tests
$SPARK_HOME/bin/spark-submit \
    --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestRunner \
    --master local[2] \
    --jars "$CONNECTOR_JAR" \
    target/smoke-test.jar \
    "$STREAM_NAME" \
    "$REGION" \
    "$CONSUMER_NAME"

EXIT_CODE=$?

echo
echo "=============================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "Smoke Tests: PASSED"
else
    echo "Smoke Tests: FAILED"
fi
echo "=============================="

exit $EXIT_CODE
