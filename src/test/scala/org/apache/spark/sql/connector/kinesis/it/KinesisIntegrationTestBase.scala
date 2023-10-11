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
package org.apache.spark.sql.connector.kinesis.it

import java.util.concurrent.CompletionException

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.control.NonFatal

import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap



abstract class KinesisIntegrationTestBase(val consumerType: String, streamShardCount: Int = 2)
  extends StreamTest with SharedSparkSession with BeforeAndAfter {

  protected var testUtils: KinesisTestUtils = _

  override val streamingTimeout = 600.seconds

  val reshardWaitTime = 5000L // milliseconds
  val IT_CONSUMER_NAME = "EFO_POC_INTEGRATION_TEST_CONSUMER"

  var defaultKinesisOptions: KinesisOptions = _

  var currentDynamoTableName: Option[String] = None

  after {
    val tableName = currentDynamoTableName.getOrElse(defaultKinesisOptions.dynamodbTableName)

    try {
      val client = DynamoDbAsyncClient.builder()
        .region(Region.of(defaultKinesisOptions.region))
        .build();

      client.deleteTable(
        DeleteTableRequest.builder.tableName(tableName).build
      ).join()

      logInfo(s"Dynamodb table ${tableName} deleted.")
    } catch {
      case ce: CompletionException if ce.getCause.isInstanceOf[ResourceNotFoundException] =>
        logInfo(s"Dynamodb table ${tableName} doesn't exist. Skip delete table.")
      case NonFatal(e) => // ignore errors
        logWarning(s"Delete dynamodb table ${tableName} ended with exception.", e)
    } finally {
      currentDynamoTableName = None
    }
  }
  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new EnhancedKinesisTestUtils(streamShardCount)
    testUtils.createStream()

    defaultKinesisOptions = KinesisOptions(new CaseInsensitiveStringMap(Map(
      REGION -> testUtils.regionName,
      ENDPOINT_URL -> testUtils.endpointUrl,
      CONSUMER_TYPE -> consumerType,
      STREAM_NAME -> testUtils.streamName,
      CONSUMER_NAME -> IT_CONSUMER_NAME,
      MAX_DATA_QUEUE_EMPTY_COUNT-> "6",
      DATA_QUEUE_WAIT_TIME_SEC -> "5"
    ).asJava))
  }

  override def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.deleteStream()
      }
    } finally {
      super.afterAll()
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.ui.explainMode", "extended")
          .set("spark.driver.bindAddress", "127.0.0.1") // for VPN
          .set("spark.driver.host", "127.0.0.1") // for VPN
  }

  def createSparkReadStream(consumerType: String,
                            utils: KinesisTestUtils,
                            checkpoint: String,
                            metadataCommitterType: String): DataStreamReader = {
    val reader = spark
      .readStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option(REGION, utils.regionName)
      .option(STREAM_NAME, utils.streamName)
      .option(ENDPOINT_URL, utils.endpointUrl)
      .option(CONSUMER_TYPE, consumerType)
      .option(METADATA_COMMITTER_TYPE, metadataCommitterType)
      .option(MAX_DATA_QUEUE_EMPTY_COUNT, defaultKinesisOptions.maxDataQueueEmptyCount.toString)
      .option(DATA_QUEUE_WAIT_TIME_SEC, defaultKinesisOptions.dataQueueWaitTimeout.getSeconds.toString)

    if (consumerType == EFO_CONSUMER_TYPE) {
      reader.option(CONSUMER_NAME, defaultKinesisOptions.consumerName.get)
    }

    val tableName = checkpoint.replaceAll("/", "_")
    reader.option(DYNAMODB_TABLE_NAME, tableName)
    currentDynamoTableName = Some(tableName)

    reader
  }
  def waitUntilBatchProcessed(clock: StreamManualClock): AssertOnQuery = Execute { q =>
    logInfo(s"waitUntilBatchProcessed start with timeout ${streamingTimeout}")
    Thread.sleep(1000.toLong)
    eventually(Timeout(streamingTimeout)) {
      if (q.exception.isEmpty) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      logInfo(s"waitUntilBatchProcessed ended with exception.", q.exception.get)
      throw q.exception.get
    }
    logInfo(s"waitUntilBatchProcessed ended")
  }

  def getRandomCheckpointDir: String = {
    val now = System.currentTimeMillis()
    val checkpointDir = s"work/checkpoint_${now}${randomAlphabetic(2)}/"
    checkpointDir

  }
}
