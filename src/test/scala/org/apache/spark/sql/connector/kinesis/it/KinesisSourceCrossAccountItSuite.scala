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

import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.ConnectorAwsCredentialsProvider
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.connector.kinesis.getRegionNameByEndpoint
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.util.StreamManualClock

abstract class KinesisSourceCrossAccountItSuite(
  consumerType: String
)
  extends KinesisIntegrationTestBase(consumerType)
  with Matchers
  with Logging {

  import testImplicits._

  private val CROSS_ACCOUNT_KINESIS_END_POINT = "CROSS_ACCOUNT_KINESIS_END_POINT"
  private val CROSS_ACCOUNT_KINESIS_STREAM_NAME = "CROSS_ACCOUNT_KINESIS_STREAM_NAME"
  private val KINESIS_STS_ROLE_ARN = "KINESIS_STS_ROLE_ARN"

  private val crossAccountEndpointUrl = sys.env.get(CROSS_ACCOUNT_KINESIS_END_POINT)
  private val crossAccountRegion = crossAccountEndpointUrl.map(getRegionNameByEndpoint)
  private val crossAccountStreamName = sys.env.get(CROSS_ACCOUNT_KINESIS_STREAM_NAME)
  private val stsRoleArn = sys.env.get(KINESIS_STS_ROLE_ARN)
  private val stsSessionName = "testsession"

  private lazy val credentialsProvider = ConnectorAwsCredentialsProvider.builder
      .stsCredentials(
        stsRoleArn,
        Some(stsSessionName),
        crossAccountRegion.get
      )
     .build()
    .provider

  private lazy val kinesisClient = KinesisClient.builder
    .httpClientBuilder(ApacheHttpClient.builder)
    .credentialsProvider(credentialsProvider)
    .region(Region.of(crossAccountRegion.get))
    .build()

  def pushDataToKinesis(data: Array[String]): Map[String, Seq[(String, String)]] = {
    val shardIdToSeqNumbers =
      new mutable.HashMap[String, ArrayBuffer[(String, String)]]()
    data.foreach { entry =>
      val data = SdkBytes.fromByteArray(entry.getBytes(StandardCharsets.UTF_8))
      val putRecordRequest = PutRecordRequest.builder()
        .streamName(crossAccountStreamName.get)
        .data(data)
        .partitionKey(entry)
        .build()

      val putRecordResult = kinesisClient.putRecord(putRecordRequest)
      val shardId = putRecordResult.shardId
      val seqNumber = putRecordResult.sequenceNumber
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(String, String)]())
      sentSeqNumbers += ((entry, seqNumber))
    }

    logInfo(s"Pushed data ${data.mkString("Array(", ", ", ")")}:\n\t${shardIdToSeqNumbers.mkString("\n\t")}")
    shardIdToSeqNumbers.toMap
  }

  def createCrossAccountSparkReadStream(consumerType: String,
                            utils: KinesisTestUtils): DataStreamReader = {
    val reader = spark
      .readStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option(REGION, defaultKinesisOptions.region)
      .option(STREAM_NAME, crossAccountStreamName.get)
      .option(ENDPOINT_URL, crossAccountEndpointUrl.get)
      .option(CONSUMER_TYPE, consumerType)
      .option(MAX_DATA_QUEUE_EMPTY_COUNT, defaultKinesisOptions.maxDataQueueEmptyCount.toString)
      .option(DATA_QUEUE_WAIT_TIME_SEC, defaultKinesisOptions.dataQueueWaitTimeout.getSeconds.toString)
      .option(STS_ROLE_ARN, stsRoleArn.get)
      .option(STS_SESSION_NAME, stsSessionName)

    if (consumerType == EFO_CONSUMER_TYPE) {
      reader.option(CONSUMER_NAME, defaultKinesisOptions.consumerName.get)
    }

    reader
  }


  private def testIfEnabled(testName: String)(testBody: => Unit): Unit = {
    if (crossAccountEndpointUrl.isDefined
      && crossAccountStreamName.isDefined
      && stsRoleArn.isDefined
    ) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $CROSS_ACCOUNT_KINESIS_END_POINT, $CROSS_ACCOUNT_KINESIS_STREAM_NAME, and " +
        s"$KINESIS_STS_ROLE_ARN]")(testBody)
    }
  }

  testIfEnabled("Kinesis connector reads from another account") {
    pushDataToKinesis(Array("0"))

    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createCrossAccountSparkReadStream(consumerType, testUtils)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { query =>
        pushDataToKinesis(testData.map(_.toString).toArray)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5), // should not have 0
      StopStream
    )
  }
}


@IntegrationTestSuite
class EfoKinesisSourceCrossAccountItSuite
  extends KinesisSourceCrossAccountItSuite(
    consumerType = EFO_CONSUMER_TYPE,
  )

@IntegrationTestSuite
class PollingKinesisSourceCrossAccountItSuite
  extends KinesisSourceCrossAccountItSuite(
    consumerType = POLLING_CONSUMER_TYPE,
  )
