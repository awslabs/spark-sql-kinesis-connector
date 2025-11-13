# Smoke Tests for Spark 4.0.0 Migration

This directory contains smoke tests to validate the Spark 4.0.0 migration of the Kinesis connector.

## Overview

The smoke tests verify:
1. **Write functionality** - Writing records to Kinesis
2. **GetRecords consumer** - Reading with polling-based consumer
3. **SubscribeToShard (EFO) consumer** - Reading with enhanced fan-out
4. **Graceful shutdown** - Proper cleanup and consumer deregistration

## Prerequisites

### 1. Build the Connector
```bash
cd ..
mvn clean package -DskipTests
```

This creates: `target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar`

### 2. Build the Smoke Tests
```bash
cd smoke-test
scalac -classpath "../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar:$SPARK_HOME/jars/*" \
  -d smoke-test.jar \
  *.scala
```

Or use the provided build script:
```bash
./build-smoke-tests.sh
```

### 3. AWS Setup

#### Create a Test Stream
```bash
aws kinesis create-stream \
  --stream-name spark4-smoke-test \
  --shard-count 2 \
  --region us-east-2
```

Wait for stream to become ACTIVE:
```bash
aws kinesis describe-stream \
  --stream-name spark4-smoke-test \
  --region us-east-2
```

#### Configure AWS Credentials
Ensure AWS credentials are configured via:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM role (if running on EC2/ECS/EKS)

Required IAM permissions:
- `kinesis:PutRecord`
- `kinesis:GetRecords`
- `kinesis:GetShardIterator`
- `kinesis:DescribeStream`
- `kinesis:ListShards`
- `kinesis:RegisterStreamConsumer`
- `kinesis:DeregisterStreamConsumer`
- `kinesis:SubscribeToShard`

### 4. Environment Requirements
- **Java 17** - Required for Spark 4.0.0
- **Spark 4.0.0** - Set `SPARK_HOME` environment variable
- **Scala 2.13** - For compiling smoke tests

Verify versions:
```bash
java -version    # Should show Java 17
scala -version   # Should show Scala 2.13
$SPARK_HOME/bin/spark-submit --version  # Should show Spark 4.0.0
```

## Running Individual Tests

### Test 1: Write to Kinesis
```bash
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestWrite \
  --master local[2] \
  --jars ../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
  smoke-test.jar \
  spark4-smoke-test \
  us-east-2 \
  10
```

Arguments: `<stream-name> <region> [num-records]`

### Test 2: Read with GetRecords
```bash
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestGetRecords \
  --master local[2] \
  --jars ../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
  smoke-test.jar \
  spark4-smoke-test \
  us-east-2 \
  30
```

Arguments: `<stream-name> <region> [duration-seconds]`

### Test 3: Read with EFO
```bash
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestEFO \
  --master local[2] \
  --jars ../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
  smoke-test.jar \
  spark4-smoke-test \
  us-east-2 \
  smoke-test-efo-consumer \
  30
```

Arguments: `<stream-name> <region> <consumer-name> [duration-seconds]`

### Test 4: Graceful Shutdown
```bash
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestGracefulShutdown \
  --master local[2] \
  --jars ../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
  smoke-test.jar \
  spark4-smoke-test \
  us-east-2 \
  smoke-test-shutdown-consumer \
  20
```

Arguments: `<stream-name> <region> <consumer-name> [duration-seconds]`

You can press Ctrl+C during execution to test the shutdown hook.

## Running All Tests

Use the test runner to execute all tests in sequence:

```bash
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestRunner \
  --master local[2] \
  --jars ../target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
  smoke-test.jar \
  spark4-smoke-test \
  us-east-2 \
  smoke-test-consumer
```

Arguments: `<stream-name> <region> <consumer-name>`

The runner will:
1. Write test records to Kinesis
2. Read with GetRecords consumer
3. Read with EFO consumer
4. Test graceful shutdown
5. Print a summary of results

## Expected Output

### Successful Test Output
```
Starting smoke test: GetRecords consumer
  Stream: spark4-smoke-test
  Region: us-east-2
  Duration: 30 seconds
  Spark Version: 4.0.0
  Scala Version: version 2.13.15
  Java Version: 17.0.x

✓ Stream reader created successfully
✓ Query started successfully
Running for 30 seconds...

-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+-------------+--------------------+----------------------------+
|message             |partitionKey |sequenceNumber      |approximateArrivalTimestamp |
+--------------------+-------------+--------------------+----------------------------+
|Test message 1...   |test-key-1   |49590338...         |2024-01-15 10:30:45.123     |
+--------------------+-------------+--------------------+----------------------------+

Stopping query...
✓ Query stopped successfully
✓ Spark session stopped

=== Smoke Test GetRecords: PASSED ===
```

### Verification Steps

After running tests, verify:

1. **EFO Consumer Deregistration**
```bash
aws kinesis list-stream-consumers \
  --stream-name spark4-smoke-test \
  --region us-east-2
```
Should show no consumers (or only active ones).

2. **Records in Stream**
```bash
# Get shard iterator
aws kinesis get-shard-iterator \
  --stream-name spark4-smoke-test \
  --shard-id shardId-000000000000 \
  --shard-iterator-type TRIM_HORIZON \
  --region us-east-2

# Get records
aws kinesis get-records \
  --shard-iterator <iterator-from-above> \
  --region us-east-2
```

3. **Check Logs**
Review Spark logs for:
- No `ClassNotFoundException`
- No `NoSuchMethodError`
- No Scala 2.12 references
- Proper Java 17 usage

## Cleanup

### Delete Test Stream
```bash
aws kinesis delete-stream \
  --stream-name spark4-smoke-test \
  --region us-east-2
```

### Deregister Leftover Consumers
```bash
# List consumers
aws kinesis list-stream-consumers \
  --stream-name spark4-smoke-test \
  --region us-east-2

# Deregister each consumer
aws kinesis deregister-stream-consumer \
  --stream-arn <stream-arn> \
  --consumer-name <consumer-name> \
  --region us-east-2
```

### Clean Checkpoint Directories
```bash
rm -rf /tmp/kinesis-smoke-test-*
```

## Troubleshooting

### ClassNotFoundException
**Problem**: `java.lang.ClassNotFoundException: org.apache.spark.sql.connector.kinesis...`

**Solution**:
- Verify JAR path is correct
- Ensure connector JAR is in `--jars` parameter
- Check Scala version matches (2.13)

### AWS Credentials Error
**Problem**: `Unable to load credentials from any provider`

**Solution**:
- Set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Or configure `~/.aws/credentials`
- Verify credentials with: `aws sts get-caller-identity`

### Stream Not Found
**Problem**: `ResourceNotFoundException: Stream spark4-smoke-test not found`

**Solution**:
- Create stream: `aws kinesis create-stream --stream-name spark4-smoke-test --shard-count 2`
- Wait for ACTIVE status: `aws kinesis describe-stream --stream-name spark4-smoke-test`

### EFO Consumer Registration Fails
**Problem**: `LimitExceededException: Consumer limit exceeded`

**Solution**:
- List consumers: `aws kinesis list-stream-consumers --stream-name spark4-smoke-test`
- Deregister unused consumers
- Wait 5 minutes after deregistration before retrying

### Java Version Mismatch
**Problem**: `UnsupportedClassVersionError` or version warnings

**Solution**:
- Verify Java 17: `java -version`
- Set `JAVA_HOME` to Java 17 installation
- Ensure Spark is using Java 17: `$SPARK_HOME/bin/spark-submit --version`

### Spark Version Mismatch
**Problem**: API incompatibility errors

**Solution**:
- Verify Spark 4.0.0: `$SPARK_HOME/bin/spark-submit --version`
- Rebuild connector with correct Spark version
- Check `pom.xml` has `<spark.version>4.0.0</spark.version>`

## Success Criteria

All tests should:
- ✅ Complete without exceptions
- ✅ Show "PASSED" in output
- ✅ Display correct version information (Spark 4.0.0, Scala 2.13, Java 17)
- ✅ Read/write data correctly
- ✅ Deregister EFO consumers on shutdown
- ✅ Exit cleanly with status code 0

## Notes

- Tests use `/tmp/kinesis-smoke-test-*` for checkpoints
- Each test runs for a limited duration (default 20-60 seconds)
- Tests are designed to be run in a development/test environment
- Monitor AWS costs during testing (minimal for short tests)
- Tests can be run multiple times safely

## Integration with CI/CD

To integrate smoke tests into CI/CD:

1. Set up AWS credentials as secrets
2. Create test stream in setup phase
3. Run smoke test suite
4. Clean up resources in teardown phase

Example GitHub Actions workflow:
```yaml
- name: Run Smoke Tests
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
  run: |
    aws kinesis create-stream --stream-name ci-smoke-test --shard-count 1
    ./smoke-test/run-smoke-tests.sh ci-smoke-test us-east-2
    aws kinesis delete-stream --stream-name ci-smoke-test
```

## Additional Resources

- [Spark 4.0.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [AWS Kinesis Documentation](https://docs.aws.amazon.com/kinesis/)
- [Connector README](../README.md)
- [Testing with AWS Guide](../TESTING_WITH_AWS.md)
