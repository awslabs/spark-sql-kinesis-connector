/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connector.kinesis

import java.time.Duration

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisOptionsSuite._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils


class KinesisOptionsSuite extends KinesisTestBase {
  test("option uses default values POLLING_CONSUMER_TYPE") {
    val options = KinesisOptions(defaultPollingOptionCaseInsensitiveMap)

    options.region shouldBe TESTBASE_DEFAULT_REGION
    options.endpointUrl shouldBe TESTBASE_DEFAULT_ENDPOINT_URL
    options.streamName shouldBe TESTBASE_DEFAULT_STREAM_NAME
    options.consumerType shouldBe POLLING_CONSUMER_TYPE
    options.failOnDataLoss shouldBe DEFAULT_FAIL_ON_DATA_LOSS
    options.avoidEmptyBatches shouldBe DEFAULT_AVOID_EMPTY_BATCHES
    options.maxFetchRecordsPerShard shouldBe DEFAULT_MAX_FETCH_RECORDS_PER_SHARD
    options.maxFetchTimePerShardSec shouldBe None
    options.startingPosition shouldBe DEFAULT_STARTING_POSITION
    options.describeShardIntervalMs shouldBe Utils.timeStringAsMs(DEFAULT_DESCRIBE_SHARD_INTERVAL)
    options.minBatchesToRetain shouldBe None
    options.checkNewRecordThreads shouldBe DEFAULT_CHECK_NEW_RECORD_THREADS
    options.metadataCommitterType shouldBe HDFS_COMMITTER_TYPE
    options.metadataPath shouldBe None
    options.metadataNumRetries shouldBe DEFAULT_METADATA_NUMBER_RETRIES
    options.metadataRetryIntervalsMs shouldBe DEFAULT_METADATA_RETRY_INTERVALS_MS
    options.metadataMaxRetryIntervalMs shouldBe DEFAULT_METADATA_MAX_RETRY_INTERVALS_MS
    options.clientNumRetries shouldBe DEFAULT_CLIENT_NUMBER_RETRIES
    options.clientRetryIntervalsMs shouldBe DEFAULT_CLIENT_RETRY_INTERVALS_MS
    options.clientMaxRetryIntervalMs shouldBe DEFAULT_CLIENT_MAX_RETRY_INTERVALS_MS
    options.dataQueueCapacity shouldBe DEFAULT_DATA_QUEUE_CAPACITY
    options.maxDataQueueEmptyCount shouldBe DEFAULT_MAX_DATA_QUEUE_EMPTY_COUNT
    options.dataQueueWaitTimeout shouldBe Duration.ofSeconds(DEFAULT_DATA_QUEUE_WAIT_TIME_SEC)
    options.consumerName shouldBe None
    options.efoSubscribeToShardTimeout shouldBe Duration.ofSeconds(DEFAULT_EFO_SUBSCRIBE_TO_SHARD_TIMEOUT)
    options.efoSubscribeToShardMaxRetries shouldBe DEFAULT_EFO_SUBSCRIBE_TO_SHARD_MAX_RETRIES
    options.pollingNumberOfRecordsPerFetch shouldBe DEFAULT_POLLING_MAX_NUMBER_OF_RECORDS_PER_FETCH
    options.pollingFetchIntervalMs shouldBe DEFAULT_POLLING_FETCH_INTERVAL_MILLIS
    options.dynamodbTableName shouldBe s"kdsc_${options.region}_${options.streamName}"
    options.maintenanceTaskIntervalSec shouldBe DEFAULT_MAINTENANCE_TASK_INTERVAL_SEC
    options.stsRoleArn shouldBe None
    options.stsSessionName shouldBe None
    options.stsEndpointUrl shouldBe None
    options.awsAccessKeyId shouldBe None
    options.awsSecretKey shouldBe None
    options.sessionToken shouldBe None
    options.customCredentialsProviderClass shouldBe None
    options.customCredentialsProviderParam shouldBe None
    options.kinesisRegion shouldBe TESTBASE_DEFAULT_REGION
  }

  test("option uses default values EFO_CONSUMER_TYPE") {
    val options = KinesisOptions(defaultEfoOptionCaseInsensitiveMap)

    options.region shouldBe TESTBASE_DEFAULT_REGION
    options.endpointUrl shouldBe TESTBASE_DEFAULT_ENDPOINT_URL
    options.streamName shouldBe TESTBASE_DEFAULT_STREAM_NAME
    options.consumerType shouldBe EFO_CONSUMER_TYPE
    options.failOnDataLoss shouldBe DEFAULT_FAIL_ON_DATA_LOSS
    options.avoidEmptyBatches shouldBe DEFAULT_AVOID_EMPTY_BATCHES
    options.maxFetchRecordsPerShard shouldBe DEFAULT_MAX_FETCH_RECORDS_PER_SHARD
    options.maxFetchTimePerShardSec shouldBe None
    options.startingPosition shouldBe DEFAULT_STARTING_POSITION
    options.describeShardIntervalMs shouldBe Utils.timeStringAsMs(DEFAULT_DESCRIBE_SHARD_INTERVAL)
    options.minBatchesToRetain shouldBe None
    options.checkNewRecordThreads shouldBe DEFAULT_CHECK_NEW_RECORD_THREADS
    options.metadataCommitterType shouldBe HDFS_COMMITTER_TYPE
    options.metadataPath shouldBe Some(TESTBASE_DEFAULT_METADATA_PATH)
    options.metadataNumRetries shouldBe DEFAULT_METADATA_NUMBER_RETRIES
    options.metadataRetryIntervalsMs shouldBe DEFAULT_METADATA_RETRY_INTERVALS_MS
    options.metadataMaxRetryIntervalMs shouldBe DEFAULT_METADATA_MAX_RETRY_INTERVALS_MS
    options.clientNumRetries shouldBe DEFAULT_CLIENT_NUMBER_RETRIES
    options.clientRetryIntervalsMs shouldBe DEFAULT_CLIENT_RETRY_INTERVALS_MS
    options.clientMaxRetryIntervalMs shouldBe DEFAULT_CLIENT_MAX_RETRY_INTERVALS_MS
    options.dataQueueCapacity shouldBe DEFAULT_DATA_QUEUE_CAPACITY
    options.maxDataQueueEmptyCount shouldBe DEFAULT_MAX_DATA_QUEUE_EMPTY_COUNT
    options.dataQueueWaitTimeout shouldBe Duration.ofSeconds(DEFAULT_DATA_QUEUE_WAIT_TIME_SEC)
    options.consumerName shouldBe Some(TESTBASE_DEFAULT_EFO_CONSUMER_NAME)
    options.efoSubscribeToShardTimeout shouldBe Duration.ofSeconds(DEFAULT_EFO_SUBSCRIBE_TO_SHARD_TIMEOUT)
    options.efoSubscribeToShardMaxRetries shouldBe DEFAULT_EFO_SUBSCRIBE_TO_SHARD_MAX_RETRIES
    options.pollingNumberOfRecordsPerFetch shouldBe DEFAULT_POLLING_MAX_NUMBER_OF_RECORDS_PER_FETCH
    options.pollingFetchIntervalMs shouldBe DEFAULT_POLLING_FETCH_INTERVAL_MILLIS
    options.dynamodbTableName shouldBe s"kdsc_${options.region}_${options.streamName}_${options.consumerName.get}"
    options.maintenanceTaskIntervalSec shouldBe DEFAULT_MAINTENANCE_TASK_INTERVAL_SEC
    options.stsRoleArn shouldBe None
    options.stsSessionName shouldBe None
    options.stsEndpointUrl shouldBe None
    options.awsAccessKeyId shouldBe None
    options.awsSecretKey shouldBe None
    options.sessionToken shouldBe None
    options.customCredentialsProviderClass shouldBe None
    options.customCredentialsProviderParam shouldBe None
    options.kinesisRegion shouldBe TESTBASE_DEFAULT_REGION
  }
  
  test("option uses names as published in README") {
    val testRegion = "us-west-2"
    val testKafkaRegion = "us-east-1"
    val testEndpointUrl = s"https://kinesis.${testKafkaRegion}.amazonaws.com"
    val testConsumerType = "SubscribeToShard"
    val testStreamName = "nameTestStreamName"
    val testConsumerName = "nameTestConsumerName"
    val testMetaPath = "/new/path"
    val testStsRoleArn = "nameTestStsRoleArn"
    val testStsSessionName = "nameTestStsSessionName"
    val testStsEndpointUrl = "nameTestStsEndpointUrl"
    val testAwsAccessKeyId = "nameTestAwsAccessKeyId"
    val testAwsSecretKey = "nameTestAwsSecretKey"
    val testSessionToken = "nameTestSessionToken"
    val testCredentialProviderClass = "customCredentialProviderClass"
    val testCredentialProviderParam = "customCredentialProviderParam"
    val testDynamoDBTableName = "nameTestDynamoDBTableName"

    val params = Map(
      "kinesis.endpointUrl" -> testEndpointUrl,
      "kinesis.region" -> testRegion,
      "kinesis.consumerType" -> testConsumerType,
      "kinesis.streamName" -> testStreamName,
      "kinesis.consumerName" -> testConsumerName,
      "kinesis.failOnDataLoss" -> "true",
      "kinesis.maxFetchRecordsPerShard" -> "500",
      "kinesis.maxFetchTimePerShardSec" -> "60",
      "kinesis.startingPosition" -> "EARLIEST",
      "kinesis.describeShardInterval" -> "5s",
      "kinesis.minBatchesToRetain" -> "50",
      "kinesis.checkNewRecordThreads" -> "5",
      "kinesis.metadataCommitterType" -> "DYNAMODB",
      "kinesis.metadataPath" -> testMetaPath,
      "kinesis.metadataNumRetries" -> "3",
      "kinesis.metadataRetryIntervalsMs" -> "100",
      "kinesis.metadataMaxRetryIntervalMs" -> "1000",
      "kinesis.clientNumRetries" -> "1",
      "kinesis.clientRetryIntervalsMs" -> "300",
      "kinesis.clientMaxRetryIntervalMs" -> "3000",
      "kinesis.stsRoleArn" -> testStsRoleArn,
      "kinesis.stsSessionName" -> testStsSessionName,
      "kinesis.stsEndpointUrl" -> testStsEndpointUrl,
      "kinesis.awsAccessKeyId" -> testAwsAccessKeyId,
      "kinesis.awsSecretKey" -> testAwsSecretKey,
      "kinesis.sessionToken" -> testSessionToken,
      "kinesis.credentialProviderClass" -> testCredentialProviderClass,
      "kinesis.credentialProviderParam" -> testCredentialProviderParam,
      "kinesis.kinesisRegion" -> testKafkaRegion,
      "kinesis.dynamodb.tableName" -> testDynamoDBTableName,
      "kinesis.subscribeToShard.timeoutSec" -> "10",
      "kinesis.subscribeToShard.maxRetries" -> "3",
      "kinesis.getRecords.numberOfRecordsPerFetch" -> "500",
      "kinesis.getRecords.fetchIntervalMs" -> "50"
    )

    val options = KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    options.endpointUrl shouldBe testEndpointUrl
    options.consumerType shouldBe testConsumerType
    options.region shouldBe testRegion
    options.streamName shouldBe testStreamName
    options.consumerName shouldBe Some(testConsumerName)
    options.failOnDataLoss shouldBe true
    options.maxFetchRecordsPerShard shouldBe 500
    options.maxFetchTimePerShardSec shouldBe Some(60)
    options.startingPosition shouldBe "EARLIEST"
    options.describeShardIntervalMs shouldBe 5000
    options.minBatchesToRetain shouldBe Some(50)
    options.checkNewRecordThreads shouldBe 5
    options.metadataCommitterType shouldBe "DYNAMODB"
    options.metadataPath shouldBe Some(testMetaPath)
    options.metadataNumRetries shouldBe 3
    options.metadataRetryIntervalsMs shouldBe 100
    options.metadataMaxRetryIntervalMs shouldBe 1000
    options.clientNumRetries shouldBe 1
    options.clientRetryIntervalsMs shouldBe 300
    options.clientMaxRetryIntervalMs shouldBe 3000
    options.stsRoleArn shouldBe Some(testStsRoleArn)
    options.stsSessionName shouldBe Some(testStsSessionName)
    options.stsEndpointUrl shouldBe Some(testStsEndpointUrl)
    options.awsAccessKeyId shouldBe Some(testAwsAccessKeyId)
    options.awsSecretKey shouldBe Some(testAwsSecretKey)
    options.sessionToken shouldBe Some(testSessionToken)
    options.customCredentialsProviderClass shouldBe Some(testCredentialProviderClass)
    options.customCredentialsProviderParam shouldBe Some(testCredentialProviderParam)
    options.kinesisRegion shouldBe testKafkaRegion
    options.dynamodbTableName shouldBe testDynamoDBTableName
    options.efoSubscribeToShardTimeout shouldBe Duration.ofSeconds(10)
    options.efoSubscribeToShardMaxRetries shouldBe 3
    options.pollingNumberOfRecordsPerFetch shouldBe 500
    options.pollingFetchIntervalMs shouldBe 50
  }

  test ("awsAccessKeyId and awsSecretKey must be both define or none") {

    val params = collection.mutable.Map(defaultPollingOptionMap.toSeq: _*) + (AWS_ACCESS_KEY_ID -> "testAccessKeyId")
    intercept[IllegalArgumentException] {
      KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    }
    
    params +=(AWS_SECRET_KEY -> "testSecretKey")

    val options = KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    options.awsAccessKeyId.get shouldBe "testAccessKeyId"
    options.awsSecretKey.get shouldBe "testSecretKey"
  }
  
  test("DESCRIBE_SHARD_INTERVAL must be >=0") {
    val params = collection.mutable.Map(defaultPollingOptionMap.toSeq: _*) + (DESCRIBE_SHARD_INTERVAL -> "-5s")

    intercept[IllegalArgumentException] {
      KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    }
  }

  test("MAX_FETCH_TIME_PER_SHARD_SEC must be >= 10") {
    val params = collection.mutable.Map(defaultPollingOptionMap.toSeq: _*) + (MAX_FETCH_TIME_PER_SHARD_SEC -> "5")

    intercept[IllegalArgumentException] {
      KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    }

    val params2 = collection.mutable.Map(defaultPollingOptionMap.toSeq: _*) + (MAX_FETCH_TIME_PER_SHARD_SEC -> "10")
    
    val options2 = KinesisOptions(new CaseInsensitiveStringMap(params2.asJava))

    options2.maxFetchTimePerShardSec shouldBe Some(10)
  }

  test("get region from endpoint url") {

    val testRegion = "us-west-2"
    
    val params = Map(
      ENDPOINT_URL -> s"https://kinesis.${testRegion}.amazonaws.com",
      CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
      STREAM_NAME -> TESTBASE_DEFAULT_STREAM_NAME
    )
    
    val options = KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    options.region shouldBe testRegion
    options.kinesisRegion shouldBe testRegion
  }

  test("stsRoleArn and stsSessionName must be both define or none") {

    val params = collection.mutable.Map(defaultPollingOptionMap.toSeq: _*) + (STS_ROLE_ARN -> "assumeRoleTest")

    intercept[IllegalArgumentException] {
      KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    }

    params += (STS_SESSION_NAME -> "sessionNameTest")

    val options = KinesisOptions(new CaseInsensitiveStringMap(params.asJava))
    options.stsRoleArn.get shouldBe "assumeRoleTest"
    options.stsSessionName.get shouldBe "sessionNameTest"
  }

  test("sink option uses names as published in README") {
    ENDPOINT_URL shouldBe "kinesis.endpointUrl"
    REGION shouldBe "kinesis.region"
    STREAM_NAME shouldBe "kinesis.streamName"
    SINK_FLUSH_WAIT_TIME_MILLIS shouldBe "kinesis.sink.flushWaitTimeMs"
    SINK_RECORD_MAX_BUFFERED_TIME shouldBe "kinesis.sink.recordMaxBufferedTimeMs"
    SINK_MAX_CONNECTIONS shouldBe "kinesis.sink.maxConnections"
    SINK_AGGREGATION_ENABLED shouldBe "kinesis.sink.aggregationEnabled"
  }
}

object KinesisOptionsSuite {
  val TESTBASE_DEFAULT_REGION = "us-east-2"
  val TESTBASE_DEFAULT_STREAM_NAME = "teststream"
  val TESTBASE_DEFAULT_EFO_CONSUMER_NAME = "EFOTestConsumer"
  val TESTBASE_DEFAULT_ENDPOINT_URL = s"https://kinesis.${TESTBASE_DEFAULT_REGION}.amazonaws.com"
  val TESTBASE_DEFAULT_METADATA_PATH = "/test/path"
  val TESTBASE_DEFAULT_METADATA_DYNAMODB_TABLE = "test_table"

  val defaultPollingOptionMap: Map[String, String] = Map(
    REGION -> TESTBASE_DEFAULT_REGION,
    ENDPOINT_URL -> TESTBASE_DEFAULT_ENDPOINT_URL,
    CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
    STREAM_NAME -> TESTBASE_DEFAULT_STREAM_NAME
  )

  val defaultPollingOptionCaseInsensitiveMap = new CaseInsensitiveStringMap(defaultPollingOptionMap.asJava)


  val defaultEfoOptionMap: Map[String, String] = Map(
    REGION -> TESTBASE_DEFAULT_REGION,
    ENDPOINT_URL -> TESTBASE_DEFAULT_ENDPOINT_URL,
    CONSUMER_TYPE -> EFO_CONSUMER_TYPE,
    STREAM_NAME -> TESTBASE_DEFAULT_STREAM_NAME,
    CONSUMER_NAME -> TESTBASE_DEFAULT_EFO_CONSUMER_NAME,
    METADATA_PATH -> TESTBASE_DEFAULT_METADATA_PATH
  )

  val defaultEfoOptionCaseInsensitiveMap = new CaseInsensitiveStringMap(defaultEfoOptionMap.asJava)
}
