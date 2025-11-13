#!/bin/bash

# Build script for Kinesis Connector Smoke Tests
# Compiles the smoke test Scala files into a JAR

set -e

echo "Building Kinesis Connector Smoke Tests"
echo "======================================="
echo

# Check prerequisites
if [ -z "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME is not set"
    echo "Please set SPARK_HOME to your Spark 4.0.0 installation"
    exit 1
fi

if ! command -v scalac &> /dev/null; then
    echo "ERROR: scalac not found"
    echo "Please install Scala 2.13"
    exit 1
fi

# Check if connector JAR exists
CONNECTOR_JAR="../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar"
if [ ! -f "$CONNECTOR_JAR" ]; then
    echo "ERROR: Connector JAR not found at $CONNECTOR_JAR"
    echo "Please build the connector first:"
    echo "  cd .."
    echo "  mvn clean package -DskipTests"
    exit 1
fi

echo "Prerequisites check:"
echo "  ✓ SPARK_HOME: $SPARK_HOME"
echo "  ✓ Scala compiler: $(scalac -version 2>&1)"
echo "  ✓ Connector JAR: $CONNECTOR_JAR"
echo

# Create output directory
mkdir -p target/classes

echo "Compiling smoke tests..."
scalac \
  -classpath "$CONNECTOR_JAR:$SPARK_HOME/jars/*" \
  -d target/classes \
  *.scala

if [ $? -ne 0 ]; then
    echo "ERROR: Compilation failed"
    exit 1
fi

echo "✓ Compilation successful"
echo

# Create JAR
echo "Creating smoke-test.jar..."
cd target/classes
jar cf ../smoke-test.jar org/
cd ../..

if [ ! -f "target/smoke-test.jar" ]; then
    echo "ERROR: Failed to create JAR"
    exit 1
fi

echo "✓ JAR created: target/smoke-test.jar"
echo

# Show JAR contents
echo "JAR contents:"
jar tf target/smoke-test.jar | grep "\.class$" | sed 's/\.class$//' | sed 's/\//./g'
echo

echo "======================================="
echo "Build complete!"
echo
echo "To run smoke tests:"
echo "  ./run-smoke-tests.sh <stream-name> <region> <consumer-name>"
echo
echo "Or run individual tests:"
echo "  \$SPARK_HOME/bin/spark-submit \\"
echo "    --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestRunner \\"
echo "    --master local[2] \\"
echo "    --jars $CONNECTOR_JAR \\"
echo "    target/smoke-test.jar \\"
echo "    <stream-name> <region> <consumer-name>"
