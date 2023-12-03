# Kinesis Connector for Spark Structured Streaming 

Implementation of Kinesis connector in Spark Structured Streaming with support to both GetRecords and SubscribeToShard (Enhanced Fan-Out, EFO) consumer types.



## Developer Setup

Clone SparkSqlKinesisConnector from the source repository on GitHub.

```sh
git clone https://github.com/awslabs/spark-sql-kinesis-connector.git
cd spark-sql-kinesis-connector

mvn clean install -DskipTests
```

This will create `target/spark-streaming-sql-kinesis-connector_2.12-<kineisis-connector-version>-SNAPSHOT.jar` file which contains the connector and its shaded dependencies. The jar file will also be installed to local maven repository.



After the jar file is installed in local Maven repository, configure your project pom.xml (use Maven as an example):

```xml
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-sql-kinesis-connector_2.12</artifactId>
            <version>${kinesis-connector-version}</version>
        </dependency>
```



Current version is tested with Spark 3.2 and above.

### Public jar file

For easier access, there is a public jar file available at S3.  For example, for version 1.0.0, the file path to jar file is `s3://awslabs-code-us-east-1/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar`.

To run with `spark-submit`,  include the jar file as below (version 1.0.0 as an example)
```
--jars s3://awslabs-code-us-east-1/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar
```

## How to use it



### Code Examples

##### Configure Kinesis Source with GetRecords consumer type

Consume data from Kinesis using GetRecords consumer type which is default consumer type.
```scala
val kinesis = spark
              .readStream
              .format("aws-kinesis")
              .option("kinesis.region", "us-east-2")
              .option("kinesis.streamName", "teststream")
              .option("kinesis.consumerType", "GetRecords")
              .option("kinesis.endpointUrl", endpointUrl)
              .option("kinesis.startingposition", "LATEST")
              .load
```

Following policy definition should be added to the IAM role
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KdsStreamSubscribeToShardPolicy",
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStreamSummary",
                "kinesis:ListShards",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords"
            ],
            "Resource": [
                "arn:aws:kinesis:*:<account-id>:stream/<kinesis-stream-name>",
                "arn:aws:kinesis:*:<account-id>:stream/<Kinesis-stream-name>/*"
            ]
        }
    ]
}
```

##### Configure Kinesis Source with SubscribeToShard consumer type
Consume data from Kinesis using SubscribeToShard(EFO) consumer type (**Please be aware that EFO may incur extra AWS costs**)
```scala
val kinesis = spark
              .readStream
              .format("aws-kinesis")
              .option("kinesis.region", "us-east-2")
              .option("kinesis.streamName", "teststream")
              .option("kinesis.consumerType", "SubscribeToShard")
              .option("kinesis.endpointUrl", endpointUrl)
              .option("kinesis.startingposition", "LATEST")
              .option("kinesis.consumerName", "TestConsumer")
              .load()
```

Following policy definition should be added to the IAM role
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KdsStreamSubscribeToShardPolicy",
            "Effect": "Allow",
            "Action": [
                "kinesis:SubscribeToShard",
                "kinesis:DescribeStreamSummary",
                "kinesis:ListShards",
                "kinesis:DescribeStreamConsumer",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListStreamConsumers",
                "kinesis:RegisterStreamConsumer",
                "kinesis:DeregisterStreamConsumer"
            ],
            "Resource": [
                "arn:aws:kinesis:*:<account-id>:stream/<kinesis-stream-name>",
                "arn:aws:kinesis:*:<account-id>:stream/<Kinesis-stream-name>/*"
            ]
        }
    ]
}
```

##### Check Schema

```scala
kinesis.printSchema
root
|-- data: binary (nullable = true)
|-- streamName: string (nullable = true)
|-- partitionKey: string (nullable = true)
|-- sequenceNumber: string (nullable = true)
|-- approximateArrivalTimestamp: timestamp (nullable = true)
```

##### Start Query

```scala
 // Cast data into string and group by data column
 val query = kinesis
            .selectExpr("CAST(data AS STRING)").as[(String)]
            .groupBy("data").count()
            .writeStream
            .format("console")
            .outputMode("complete") 
            .start()
```

##### Gracefully Shutdown Query

Below is an example on how to ensure the query is gracefully shutdown before the streaming driver process is stopped.

```scala
// add query stop to system shutdown hook
sys.addShutdownHook {
  query.stop()
}

// wait for the signal to stop query
waitForQueryStop(query, writeToDir)

def waitForQueryStop(query: StreamingQuery, path: String): Unit = {
    val stopLockPath = new Path(path, "STOP_LOCK")
    val fileContext = FileContext.getFileContext(stopLockPath.toUri, new Configuration())

    while (query.isActive) {
      // Stop the query when "STOP_LOCK" file is found
      if (fileContext.util().exists(stopLockPath)) {
        query.stop()
        fileContext.delete(stopLockPath, false)
      }

      Thread.sleep(500)
    }
}
```



##### Using the Kinesis Sink

```scala
// Cast data into string and group by data column
kinesis
  .selectExpr("CAST(rand() AS STRING) as partitionKey","CAST(data AS STRING)").as[(String,String)]
  .groupBy("data").count()
  .writeStream
  .format("aws-kinesis")
  .outputMode("append")
  .option("kinesis.region", "us-east-1")
  .option("kinesis.streamName", "sparkSinkTest")
  .option("kinesis.endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
  .option("checkpointLocation", "/path/to/checkpoint")
  .start()
```



### Kinesis Connector Metadata storage

By default, Kinesis Connector's metadata is stored under the same HDFS/S3 folder of checkpoint location .  



It is also possible to save the metadata in DynamoDB by specifying the options as below:

```scala
      .option("kinesis.metadataCommitterType", "DYNAMODB")
      .option("kinesis.dynamodb.tableName", "kinesisTestMetadata")
```



To use DynamoDB, following policy definition should be added to the IAM role running the job

```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KDSConnectorAccess",
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
            "Resource": "arn:aws:dynamodb:<region>:<account-number>:table/<tablename>"
        }
    ]
}
```



### EFO Consumer Registration/Deregistration

The Spark application need to provide a `kinesis.consumerName` when it is using Kinesis Enhanced Fan Out. **Each application must have a unique stream consumer name.** Kinesis Connector registers the stream consumer automatically when the application starts.  If a consumer with the same `kinesis.consumerName` already exists, the connector reuses it. 

The Stream consumer is deregistered when the application is **shutdown gracefully** with query `stop()` called. There is no garuantee of deregistration success, especially in the event that an application is terminated brutally. The stream consumer will be reused when the application restarts. Note that The stream consumers remain registrated may **incur extra AWS costs**.



### Avoid race conditions

1. Speculative execution should to be disabled (by default, *spark.speculation* is turned off on EMR) to avoid Spark running two tasks for the same shard at the same time which will create race conditions. 

2. For the same reason, If two jobs need to read from the same Kinesis stream at the same time, the Spark application should cache the dataframe. Here is an example of caching dataframe in scala. Although  `batchDF.count` and `batchDF.write` will start two jobs,  `batchDF.persist()` ensures the application will only read from Kinesis stream once. `batchDF.unpersist()` releases the cache once the processing is done.

```scala
    val batchProcessor: (DataFrame, Long) => Unit = (batchDF: DataFrame, batchId: Long) => {
      val now = System.currentTimeMillis()
      val writeToDirNow = s"${writeToDir}/${now}"
      batchDF.persist()
      if (batchDF.count() > 0) {
        batchDF.write
          .format("csv")
          .mode(SaveMode.Append)
          .save(writeToDirNow)
      }
      batchDF.unpersist()
    }

    val inputDf = reader.load()
      .selectExpr("CAST(data AS STRING)")

    val query = inputDf
      .writeStream
      .queryName("KinesisDataConsumerForeachBatch")
      .foreachBatch {batchProcessor}
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()
```



The same applies if you want to write the output of a streaming query to multiple locations

```scala
    val query = inputDf
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        batchDF.write.format(...).save(...)  // location 1
        batchDF.write.format(...).save(...)  // location 2
        batchDF.unpersist()
      }
```



### Credential Provider

Kinesis Connector uses the default credentials provider chain to supply credentials that are used in your application.It looks for credentials in this order:

1. Java System Properties - aws.accessKeyId and aws.secretAccessKey
2. Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
3. Web Identity Token credentials from system properties or environment variables
4. Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
5. Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable,
6. Instance profile credentials delivered through the Amazon EC2 metadata service

Refer to [AWS SDK for Java 2.x developer Guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) for details.



### Cross Account Access

There are scenarios where customers follow a multi-account approach resulting in Kinesis Data Streams and Spark consumer applications operating in different accounts. 

The steps to access a Kinesis data stream in one account from a Spark structured streaming application in another account are:
* Step 1 – Create AWS Identity and Access Management (IAM) role in Account A to access the Kinesis data stream with trust relationship with Account B.

Attach below policy to the role in Account A

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KdsStreamPolicy",
            "Effect": "Allow",
            "Action": [
                "kinesis:SubscribeToShard",
                "kinesis:DescribeStreamSummary",
                "kinesis:ListShards",
                "kinesis:DescribeStreamConsumer",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListStreamConsumers",
                "kinesis:RegisterStreamConsumer",
                "kinesis:DeregisterStreamConsumer"
            ],
            "Resource": [
                "arn:aws:kinesis:*:<AccountA-id>:stream/<SparkConnectorTestStream>",
                "arn:aws:kinesis:*:<AccountA-id>:stream/<SparkConnectorTestStream>/*"
            ]
        }
    ]
}

```



Trust policy of the role in Account A

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AccountATrust",
			"Effect": "Allow",
			"Principal": {"AWS":["arn:aws:iam::<AccountB-id>:root"]},
			"Action": "sts:AssumeRole"
		}
	]
}
```



* Step 2 – Create IAM role in Account B to assume the role in Account A. This role is used to run the Spark application.

add below permission

```json
{
  "Sid": "AssumeRoleInSourceAccount",
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Resource": "<RoleArnInAccountA>"
}
```



* Step 3 – Configure Kinesis connector to assume the role in Account A to read Kinesis data stream in Account A as below

```scala
  .option("kinesis.stsRoleArn", "RoleArnInAccountA")
  .option("kinesis.stsSessionName", "StsSessionName")
```



## Kinesis Source Configuration

| Name                                       | Default Value                                                | Description                                                  |
| ------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| kinesis.endpointUrl                        | required, no default value                                   | Endpoint URL for Kinesis Stream                              |
| kinesis.region                             | inferred value from `kinesis.endpointUrl`                    | Region running the Kinesis connector                         |
| kinesis.streamName                         | required, no default value                                   | Name of the stream                                           |
| kinesis.consumerType                       | GetRecords                                                   | Consumer type. Possible values are "GetRecords", "SubscribeToShard" |
| kinesis.failOnDataLoss                     | false                                                        | Fail the streaming job if any active shard is missing or expired |
| kinesis.maxFetchRecordsPerShard            | 100,000                                                      | Maximum number of records to fetch per shard per microbatch  |
| kinesis.startingPosition                   | LATEST                                                       | Starting Position in Kinesis to fetch data from. Possible values are "LATEST", "TRIM_HORIZON", "EARLIEST" (alias for TRIM_HORIZON), or "AT_TIMESTAMP YYYY-MM-DDTHH:MM:SSZ" (e.g. 2023-08-30T19:00:05Z, 2023-08-30T19:00:05-08:00) |
| kinesis.describeShardInterval              | 1s                                                           | Minimum Interval between two ListShards API calls to get latest shards. Possible values are time values such as 50s, 100ms. |
| kinesis.minBatchesToRetain                 | same as `spark.sql.streaming.minBatchesToRetain`             | The minimum number of batches of kinesis metadata that must be retained and made recoverable. |
| kinesis.checkNewRecordThreads              | 8                                                            | Number of threads in Spark driver to check if there are new records in Kinesis stream. |
| kinesis.metadataCommitterType              | HDFS                                                         | Where to save Kinesis connector metadata. Possible values are "HDFS", "DYNAMODB" |
| kinesis.metadataPath                       | Same as `checkpointLocation`                                 | a path to HDFS or S3. Only valid when  `kinesis.metadataCommitterType` is HDFS. |
| kinesis.metadataNumRetries                 | 5                                                            | Maximum Number of retries for metadata requests              |
| kinesis.metadataRetryIntervalsMs           | 1000 (milliseconds)                                          | Wait time before retrying metadata requests                  |
| kinesis.metadataMaxRetryIntervalMs         | 10000 (milliseconds)                                         | Max wait time between 2 retries of metadata requests         |
| kinesis.clientNumRetries                   | 5                                                            | Maximum Number of retries for Kinesis API requests           |
| kinesis.clientRetryIntervalsMs             | 1000 (milliseconds)                                          | Wait time before retrying Kinesis requests                   |
| kinesis.clientMaxRetryIntervalMs           | 10000 (milliseconds)                                         | Max wait time between 2 retries of Kinesis requests          |
| kinesis.consumerName                       | Required when `kinesis.consumerType` is "SubscribeToShard"   | Kinesis stream Enhance Fan Out consumer name                 |
| kinesis.stsRoleArn                         | -                                                            | AWS STS Role ARN for Kinesis operations describe, read record, etc. |
| kinesis.stsSessionName                     | -                                                            | AWS STS Session name                                         |
| kinesis.stsEndpointUrl                     | -                                                            | AWS STS Endpoint URL                                         |
| kinesis.kinesisRegion                      | inferred value from `kinesis.endpointUrl`                    | Region the Kinesis stream belongs to                         |
| kinesis.dynamodb.tableName                 | Required when when  `kinesis.metadataCommitterType` is "DYNAMODB" | Dynamodb tableName                                           |
| kinesis.subscribeToShard.timeoutSec        | 60 (seconds)                                                 | Timeout waiting for subscribeToShard finish                  |
| kinesis.subscribeToShard.maxRetries        | 10                                                           | Max retries of subscribeToShard request                      |
| kinesis.getRecords.numberOfRecordsPerFetch | 10,000                                                       | Maximum Number of records to fetch per getRecords API call   |
| kinesis.getRecords.fetchIntervalMs         | 200 (milliseconds)                                           | Minimum interval of two getRecords API calls                 |



## Kinesis Sink Configuration

| Name                                 | Default Value                    | Description                                                  |
| ------------------------------------ | -------------------------------- | ------------------------------------------------------------ |
| kinesis.endpointUrl                  | required, no default value       | Endpoint URL for Kinesis Stream                              |
| kinesis.region                       | inferred value from endpoint url | Region running the Kinesis connector                         |
| kinesis.streamName                   | required, no default value       | Name of the stream                                           |
| kinesis.sink.flushWaitTimeMs         | 100 (milliseconds)               | Wait time while flushing records to Kinesis on Task End      |
| kinesis.sink.recordMaxBufferedTimeMs | 1000 (milliseconds)              | Specify the maximum buffered time of a record                |
| kinesis.sink.maxConnections          | 1                                | Specify the maximum connections to Kinesis                   |
| kinesis.sink.aggregationEnabled      | True                             | Specify if records should be aggregated before sending them to Kinesis |



## Security

See [CONTRIBUTING](*CONTRIBUTING.md#security-issue-notifications*) for more information.



## Acknowledgement

This connector would not have been possible without reference implemetation of [Kinesis Connector](https://github.com/qubole/kinesis-sql), [Apache Flink AWS Connectors](https://github.com/apache/flink-connector-aws), [spark-streaming-sql-s3-connector](https://github.com/aws-samples/spark-streaming-sql-s3-connector) and [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client). Structure of some part of the code is influenced by the excellent work done by various Apache Spark Contributors.
