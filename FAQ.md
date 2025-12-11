# Spark Kinesis Connector - Frequently Asked Questions

## Compatibility Questions

### Q: Does this connector work with Spark 4.0 and Java 17?

**A: Yes!** The connector is fully compatible with Spark 4.0+ and Java 17+. However, we recommend using **DynamoDB for metadata storage** instead of HDFS for the best experience.

### Q: Why do HDFS tests fail with Java 17+?

**A:** This is due to Java 17's deprecation of the Security Manager APIs that Hadoop uses. The Hadoop version comes from your Spark distribution (not from this connector), so we cannot directly fix Hadoop's Java 17 compatibility issues.

**For detailed explanation, see [HDFS_COMPATIBILITY.md](HDFS_COMPATIBILITY.md).**

### Q: Is this a bug in the connector?

**A: No.** The connector correctly uses Hadoop APIs. The issue is that Hadoop 3.3.x (bundled with most Spark distributions) uses deprecated Java APIs that throw exceptions in Java 17+. This is an ecosystem compatibility issue, not a connector bug.

### Q: Which metadata storage should I use?

**A: For Spark 4.0+ with Java 17+, we strongly recommend DynamoDB:**

```scala
.option("kinesis.metadataCommitterType", "DYNAMODB")
.option("kinesis.dynamodb.tableName", "kinesis_metadata")
```

**Why DynamoDB?**
- ✅ Fully compatible with Java 17+
- ✅ No Hadoop dependency
- ✅ Managed service (no servers to maintain)
- ✅ Automatic scaling
- ✅ Built for cloud-native deployments

### Q: Can I still use HDFS for metadata storage?

**A: Yes, but with caveats:**

For Java 17+, you'll need to add JVM flags:

```bash
spark-submit \
  --conf "spark.driver.extraJavaOptions=--add-opens java.base/javax.security.auth=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens java.base/javax.security.auth=ALL-UNNAMED" \
  --jars spark-streaming-sql-kinesis-connector_2.13-1.4.3.jar \
  your-app.jar
```

This is a workaround, not a permanent solution. We recommend DynamoDB for production.

### Q: Who controls the Hadoop version used at runtime?

**A:** Your Spark distribution controls the Hadoop version, not this connector. 

When you download Spark (e.g., `spark-4.0.0-bin-hadoop3.tgz`), it comes pre-bundled with Hadoop libraries. This connector uses whatever Hadoop version Spark brought along (via Maven's `provided` scope).

### Q: Will this be fixed in the future?

**A:** The proper fix requires:
1. Hadoop 3.4+ with better Java 17+ support
2. Spark distributions bundling the newer Hadoop version

Monitor Spark release notes for Hadoop version updates. In the meantime, DynamoDB metadata storage works perfectly.

## Configuration Questions

### Q: How do I configure DynamoDB metadata storage?

**A:** Add these options to your Spark streaming configuration:

```scala
val kinesis = spark
  .readStream
  .format("aws-kinesis")
  .option("kinesis.region", "us-east-1")
  .option("kinesis.streamName", "my-stream")
  .option("kinesis.metadataCommitterType", "DYNAMODB")
  .option("kinesis.dynamodb.tableName", "kinesis_metadata")
  .load()
```

**Required IAM permissions:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable",
        "dynamodb:DeleteItem",
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Scan",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/kinesis_metadata"
    }
  ]
}
```

### Q: What's the difference between GetRecords and SubscribeToShard?

**A:**

**GetRecords (Default):**
- Standard Kinesis API
- Shared throughput (2 MB/sec per shard)
- Lower cost
- Good for most use cases

**SubscribeToShard (Enhanced Fan-Out):**
- Dedicated throughput (2 MB/sec per consumer per shard)
- Lower latency (~70ms vs ~200ms)
- Higher cost (additional charges apply)
- Better for multiple consumers or low-latency requirements

```scala
// GetRecords (default)
.option("kinesis.consumerType", "GetRecords")

// SubscribeToShard (EFO)
.option("kinesis.consumerType", "SubscribeToShard")
.option("kinesis.consumerName", "my-unique-consumer-name")
```

### Q: How do I handle cross-account access?

**A:** Use STS AssumeRole:

```scala
.option("kinesis.stsRoleArn", "arn:aws:iam::123456789012:role/KinesisReadRole")
.option("kinesis.stsSessionName", "spark-kinesis-session")
```

See the [Cross Account Access](#cross-account-access-using-assumerole) section in README.md for detailed setup.

## Performance Questions

### Q: How do I optimize throughput?

**A:** Key configuration options:

```scala
// Increase records per fetch
.option("kinesis.maxFetchRecordsPerShard", "100000")

// Add time-based limit (optional)
.option("kinesis.maxFetchTimePerShardSec", "30")

// For GetRecords: adjust fetch parameters
.option("kinesis.getRecords.numberOfRecordsPerFetch", "10000")
.option("kinesis.getRecords.fetchIntervalMs", "200")
```

### Q: Should I use speculative execution?

**A: No.** Speculative execution is not supported and should be disabled (it's off by default on EMR). Running two tasks for the same shard creates race conditions.

### Q: How do I avoid reading from Kinesis multiple times in the same job?

**A:** Cache the DataFrame:

```scala
val batchProcessor: (DataFrame, Long) => Unit = (batchDF: DataFrame, batchId: Long) => {
  batchDF.persist()
  
  // Multiple operations on the same batch
  batchDF.write.format("parquet").save("s3://bucket/path1")
  batchDF.write.format("json").save("s3://bucket/path2")
  
  batchDF.unpersist()
}

inputDf.writeStream.foreachBatch(batchProcessor).start()
```

## Troubleshooting Questions

### Q: I'm getting "UnsupportedOperationException: getSubject is not supported"

**A:** This is the Java 17 + Hadoop compatibility issue. Solutions:

1. **Recommended:** Switch to DynamoDB metadata storage
2. **Workaround:** Add JVM flags (see above)
3. **Alternative:** Use Java 11 if possible

See [HDFS_COMPATIBILITY.md](HDFS_COMPATIBILITY.md) for details.

### Q: My Enhanced Fan-Out consumer isn't being deregistered

**A:** EFO consumers are only deregistered when the application shuts down gracefully with `query.stop()`. If your application crashes or is killed, the consumer remains registered.

**Solution:** Implement graceful shutdown:

```scala
sys.addShutdownHook {
  query.stop()
}
```

**Note:** Registered consumers may incur AWS costs even when not actively reading.

### Q: I'm seeing "failOnDataLoss" errors

**A:** This happens when shards are missing or expired. Options:

```scala
// Option 1: Disable (not recommended for production)
.option("kinesis.failOnDataLoss", "false")

// Option 2: Adjust retention and checkpoint frequency
// Ensure checkpoints happen more frequently than shard retention
```

### Q: How do I test with AWS resources?

**A:** See [TESTING_WITH_AWS.md](TESTING_WITH_AWS.md) for:
- Setting up AWS credentials
- Running integration tests
- Required IAM permissions
- LocalStack for local testing

## Migration Questions

### Q: I'm upgrading from Spark 3.x to 4.0. What should I change?

**A:**

1. **Update Java:** Spark 4.0 requires Java 17+
2. **Update connector:** Use the latest version built for Spark 4.0
3. **Review metadata storage:** Consider switching to DynamoDB if using HDFS
4. **Test thoroughly:** Run integration tests with your workload

### Q: Can I migrate from HDFS to DynamoDB metadata storage?

**A:** Yes, but you'll need to handle the migration:

1. Stop your streaming job
2. Note the last processed sequence numbers from HDFS metadata
3. Update configuration to use DynamoDB
4. Optionally: manually populate DynamoDB with last sequence numbers
5. Restart the job (it will start from LATEST or TRIM_HORIZON if no metadata exists)

**Note:** There's no automatic migration tool. Plan for potential data reprocessing.

## Cost Questions

### Q: What are the AWS costs?

**A:** Main cost factors:

1. **Kinesis Data Streams:**
   - Shard hours
   - PUT payload units
   - Enhanced Fan-Out (if used): consumer-shard hours + data retrieval

2. **DynamoDB (if used for metadata):**
   - Read/write capacity units (minimal - only for checkpoints)
   - Storage (minimal - only metadata)

3. **Data Transfer:**
   - Cross-region data transfer (if applicable)

**Cost optimization:**
- Use GetRecords instead of Enhanced Fan-Out if latency isn't critical
- Right-size your shard count
- Use DynamoDB on-demand pricing for variable workloads

### Q: Is DynamoDB metadata storage expensive?

**A: No.** Metadata storage uses minimal DynamoDB capacity:
- Small items (just checkpoint information)
- Infrequent writes (once per microbatch)
- Minimal reads (only on startup/recovery)

Typical cost: **< $1/month** for most workloads.

## Support Questions

### Q: Where can I report issues?

**A:** GitHub Issues: [spark-sql-kinesis-connector/issues](https://github.com/awslabs/spark-sql-kinesis-connector/issues)

### Q: How do I contribute?

**A:** See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Code contribution guidelines
- Development setup
- Testing requirements
- Security issue reporting

### Q: Where can I find more examples?

**A:** Check:
- [README.md](README.md) - Configuration examples
- Integration tests in `src/test/scala/.../it/`
- AWS Big Data Blog posts about Spark and Kinesis

## Additional Resources

- **Detailed HDFS Compatibility:** [HDFS_COMPATIBILITY.md](HDFS_COMPATIBILITY.md)
- **AWS Testing Guide:** [TESTING_WITH_AWS.md](TESTING_WITH_AWS.md)
- **Main Documentation:** [README.md](README.md)
- **AWS Kinesis Documentation:** [AWS Kinesis Data Streams](https://docs.aws.amazon.com/kinesis/)
- **Apache Spark Documentation:** [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

---

**Still have questions?** Open an issue on GitHub or check the AWS Big Data forums.
