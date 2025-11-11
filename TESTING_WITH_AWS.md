# Testing Spark Kinesis Connector with AWS Resources

This guide explains how to run integration tests for the Spark Kinesis Connector with real AWS resources.

## Prerequisites

### 1. AWS Account Setup
You need an AWS account with permissions to create and manage:
- **Kinesis Data Streams**
- **DynamoDB tables** (for metadata storage)
- **IAM roles/policies** (for permissions)

### 2. AWS Credentials Configuration

The connector uses the AWS Default Credentials Provider Chain. Configure credentials using one of these methods:

#### Option A: AWS CLI Configuration (Recommended)
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Enter default region (e.g., us-east-2)
```

This creates `~/.aws/credentials` and `~/.aws/config` files.

#### Option B: Environment Variables
```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-east-2"
```

#### Option C: IAM Role (if running on EC2/EMR)
If running tests on EC2 or EMR, attach an IAM role with appropriate permissions.

### 3. Required IAM Permissions

Create an IAM policy with these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KinesisTestPermissions",
            "Effect": "Allow",
            "Action": [
                "kinesis:CreateStream",
                "kinesis:DeleteStream",
                "kinesis:DescribeStream",
                "kinesis:DescribeStreamSummary",
                "kinesis:ListShards",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:PutRecord",
                "kinesis:PutRecords",
                "kinesis:SplitShard",
                "kinesis:MergeShards",
                "kinesis:SubscribeToShard",
                "kinesis:DescribeStreamConsumer",
                "kinesis:ListStreamConsumers",
                "kinesis:RegisterStreamConsumer",
                "kinesis:DeregisterStreamConsumer"
            ],
            "Resource": "*"
        },
        {
            "Sid": "DynamoDBTestPermissions",
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:DeleteTable",
                "dynamodb:DescribeTable",
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:DeleteItem",
                "dynamodb:Scan",
                "dynamodb:Query"
            ],
            "Resource": "*"
        }
    ]
}
```

**Note:** For production, restrict the `Resource` field to specific ARNs instead of using `*`.

## Running Integration Tests

### 1. Configure Test Endpoint (Optional)

By default, tests use `us-east-2` region. To use a different region:

```bash
export KINESIS_TEST_ENDPOINT_URL="https://kinesis.us-west-2.amazonaws.com"
```

### 2. Run All Tests

```bash
# Run all tests (unit + integration)
mvn clean test

# This will:
# - Create temporary Kinesis streams
# - Create temporary DynamoDB tables
# - Run tests
# - Clean up resources automatically
```

### 3. Run Specific Test Suites

#### Unit Tests Only (No AWS Resources Required)
```bash
mvn test -Dtest=ShardSyncerSuite,KinesisOptionsSuite,PackageSuite,HDFSMetaDataCommitterSuite,DynamodbMetadataCommitterSuite
```

#### Integration Tests with Polling Consumer
```bash
mvn test -Dtest=NoAggPollingKinesisSourceDDBItSuite
```

#### Integration Tests with EFO Consumer
```bash
mvn test -Dtest=NoAggEfoKinesisSourceHDFSItSuite
```

#### Sink Tests
```bash
mvn test -Dtest=KinesisSinkItSuite
```

#### Cross-Account Tests
```bash
mvn test -Dtest=KinesisSourceCrossAccountItSuite
```

### 4. Run Tests with Specific Configuration

```bash
# Run with custom endpoint
KINESIS_TEST_ENDPOINT_URL="https://kinesis.eu-west-1.amazonaws.com" mvn test

# Run with verbose logging
mvn test -X

# Run with specific test and show output
mvn test -Dtest=NoAggPollingKinesisSourceDDBItSuite -DfailIfNoTests=false
```

## Test Categories

### Unit Tests (No AWS Resources)
These tests use mocks and don't require AWS:
- `ShardSyncerSuite`
- `KinesisOptionsSuite`
- `PackageSuite`
- `HDFSMetaDataCommitterSuite` (uses local filesystem)
- `DynamodbMetadataCommitterSuite` (uses mocks)
- `EfoRecordBatchPublisherSuite` (uses mocks)
- `EfoShardSubscriberSuite` (uses mocks)

### Integration Tests (Require AWS Resources)
These tests create real AWS resources:

#### Polling Consumer Tests
- `NoAggPollingKinesisSourceDDBItSuite` - Tests with DynamoDB metadata storage
- `NoAggPollingKinesisSourceHDFSItSuite` - Tests with HDFS metadata storage
- `NoAggPollingKinesisSourceReshardDDBItSuite` - Tests shard splitting/merging

#### EFO Consumer Tests
- `NoAggEfoKinesisSourceHDFSItSuite` - EFO with HDFS metadata
- `NoAggEfoKinesisSourceDDBItSuite` - EFO with DynamoDB metadata
- `NoAggEfoKinesisSourceReshardHDFSItSuite` - EFO with resharding
- `AggEfoShardConsumerItSuite` - Tests with aggregated records
- `NoAggEfoShardConsumerItSuite` - Tests without aggregation

#### Sink Tests
- `KinesisSinkItSuite` - Tests writing to Kinesis

#### Cross-Account Tests
- `KinesisSourceCrossAccountItSuite` - Tests cross-account access

## AWS Resources Created During Tests

### Kinesis Streams
- **Naming Pattern:** `KinesisTestUtils-<random-number>`
- **Shard Count:** 2 (default)
- **Lifecycle:** Created at test start, deleted at test end
- **Cost:** Minimal (tests run quickly, streams are deleted)

### DynamoDB Tables
- **Naming Pattern:** Based on checkpoint location (e.g., `work_checkpoint_<timestamp>`)
- **Purpose:** Store Kinesis connector metadata
- **Lifecycle:** Created during test, deleted in cleanup
- **Cost:** Minimal (on-demand billing, small data)

### EFO Consumers (for EFO tests only)
- **Name:** `EFO_POC_INTEGRATION_TEST_CONSUMER`
- **Purpose:** Enhanced Fan-Out consumer registration
- **Lifecycle:** Registered at test start, deregistered at test end
- **Cost:** EFO consumers incur additional charges (~$0.015/hour per consumer)

## Cost Considerations

Running the full integration test suite typically costs **less than $1** because:
- Tests run for only a few minutes
- Resources are cleaned up immediately
- Kinesis streams use minimal shards (2)
- DynamoDB uses on-demand billing with minimal data

**Estimated costs per full test run:**
- Kinesis Data Streams: ~$0.10 (shard hours)
- DynamoDB: ~$0.01 (read/write requests)
- EFO Consumers: ~$0.05 (if running EFO tests)
- **Total: ~$0.15 - $0.20**

## Troubleshooting

### Issue: Tests fail with "Could not find AWS credentials"
**Solution:** Configure AWS credentials using one of the methods above.

### Issue: Tests fail with "Access Denied" or permission errors
**Solution:** Ensure your IAM user/role has the required permissions listed above.

### Issue: Tests timeout or hang
**Solution:** 
- Check your network connectivity to AWS
- Verify the region is accessible
- Check AWS service health status

### Issue: Resources not cleaned up
**Solution:**
- Manually delete test streams: `aws kinesis delete-stream --stream-name KinesisTestUtils-<number>`
- Manually delete test tables: `aws dynamodb delete-table --table-name work_checkpoint_<timestamp>`
- Deregister consumers: `aws kinesis deregister-stream-consumer --stream-arn <arn> --consumer-name EFO_POC_INTEGRATION_TEST_CONSUMER`

### Issue: Rate limiting errors
**Solution:** AWS has API rate limits. If you see throttling errors:
- Wait a few minutes between test runs
- Request limit increases from AWS Support if needed

## Running Tests in CI/CD

For automated testing in CI/CD pipelines:

```bash
# GitHub Actions / GitLab CI example
- name: Run Integration Tests
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_DEFAULT_REGION: us-east-2
  run: |
    mvn clean test
```

## Best Practices

1. **Use a dedicated AWS account or sandbox** for testing to avoid affecting production resources
2. **Set up billing alerts** to monitor test costs
3. **Run unit tests first** before integration tests to catch issues early
4. **Clean up manually** if tests fail unexpectedly to avoid lingering resources
5. **Use specific test suites** during development instead of running all tests
6. **Monitor AWS costs** in the AWS Cost Explorer after running tests

## Quick Start Example

```bash
# 1. Configure AWS credentials
aws configure

# 2. Verify credentials work
aws kinesis list-streams --region us-east-2

# 3. Run a simple integration test
mvn test -Dtest=NoAggPollingKinesisSourceDDBItSuite

# 4. Check for any leftover resources
aws kinesis list-streams --region us-east-2 | grep KinesisTestUtils
aws dynamodb list-tables --region us-east-2 | grep work_checkpoint
```

## Additional Resources

- [AWS SDK for Java Credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)
- [Kinesis Data Streams Pricing](https://aws.amazon.com/kinesis/data-streams/pricing/)
- [DynamoDB Pricing](https://aws.amazon.com/dynamodb/pricing/)
- [AWS Free Tier](https://aws.amazon.com/free/) - Some resources may be covered under free tier

## Support

If you encounter issues:
1. Check the test logs in `target/unittest-reports/`
2. Enable verbose logging: `mvn test -X`
3. Review AWS CloudTrail logs for API call details
4. Open an issue on the GitHub repository with logs and error messages
