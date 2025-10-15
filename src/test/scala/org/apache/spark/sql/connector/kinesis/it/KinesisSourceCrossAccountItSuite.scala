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
  private val KINESIS_ACCESS_KEY_ID = "KINESIS_ACCESS_KEY_ID"
  private val KINESIS_SECRET_KEY = "KINESIS_SECRET_KEY"
  private val KINESIS_SESSION_TOKEN = "KINESIS_SESSION_TOKEN"

  private val crossAccountEndpointUrl = sys.env.get(CROSS_ACCOUNT_KINESIS_END_POINT)
  private val crossAccountRegion = crossAccountEndpointUrl.map(getRegionNameByEndpoint)
  private val crossAccountStreamName = sys.env.get(CROSS_ACCOUNT_KINESIS_STREAM_NAME)
  private val stsRoleArnOpt = sys.env.get(KINESIS_STS_ROLE_ARN)
  private val stsSessionName = "testsession"
  private val awsAccessKeyIdOpt = sys.env.get(KINESIS_ACCESS_KEY_ID)
  private val awsSecretKeyOpt = sys.env.get(KINESIS_SECRET_KEY)
  private val sessionTokenOpt = sys.env.get(KINESIS_SESSION_TOKEN)

  private lazy val credentialsProvider = ConnectorAwsCredentialsProvider.builder
      .stsCredentials(
        stsRoleArnOpt,
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

  def pushDataToKinesis(data: Array[String]): Map[String, ArrayBuffer[(String, String)]] = {
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

  def createCrossAccountSparkCustomReadStream(consumerType: String,
                                           utils: KinesisTestUtils): DataStreamReader = {
    
    val customClassName = "org.apache.spark.sql.connector.kinesis.it.CustomTestCredentialsProvider"
    
    val reader = spark
      .readStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option(REGION, defaultKinesisOptions.region)
      .option(STREAM_NAME, crossAccountStreamName.get)
      .option(ENDPOINT_URL, crossAccountEndpointUrl.get)
      .option(CONSUMER_TYPE, consumerType)
      .option(MAX_DATA_QUEUE_EMPTY_COUNT, defaultKinesisOptions.maxDataQueueEmptyCount.toString)
      .option(DATA_QUEUE_WAIT_TIME_SEC, defaultKinesisOptions.dataQueueWaitTimeout.getSeconds.toString)
      .option(CUSTOM_CREDENTIAL_PROVIDER_CLASS, customClassName)
      .option(CUSTOM_CREDENTIAL_PROVIDER_PARAM, s"${stsRoleArnOpt.get},${stsSessionName}")

    if (consumerType == EFO_CONSUMER_TYPE) {
      reader.option(CONSUMER_NAME, defaultKinesisOptions.consumerName.get)
    }

    reader
  }
  
  def createCrossAccountSparkStsReadStream(consumerType: String,
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
      .option(STS_ROLE_ARN, stsRoleArnOpt.get)
      .option(STS_SESSION_NAME, stsSessionName)

    if (consumerType == EFO_CONSUMER_TYPE) {
      reader.option(CONSUMER_NAME, defaultKinesisOptions.consumerName.get)
    }

    reader
  }


  private def testIfStsEnabled(testName: String)(testBody: => Unit): Unit = {
    if (crossAccountEndpointUrl.isDefined
      && crossAccountStreamName.isDefined
      && stsRoleArnOpt.isDefined
    ) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $CROSS_ACCOUNT_KINESIS_END_POINT, $CROSS_ACCOUNT_KINESIS_STREAM_NAME, and " +
        s"$KINESIS_STS_ROLE_ARN]")(testBody)
    }
  }

  def createCrossAccountSparkReadStream(consumerType: String,
                                        awsAccessKeyId: String,
                                        awsSecretKey: String,
                                        sessionTokenOpt: Option[String],
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
     .option(AWS_ACCESS_KEY_ID, awsAccessKeyId)
     .option(AWS_SECRET_KEY, awsSecretKey)
    
      if (consumerType == EFO_CONSUMER_TYPE) {
        reader.option(CONSUMER_NAME, defaultKinesisOptions.consumerName.get)
      }
    
     if (sessionTokenOpt.isDefined) {
       reader.option(AWS_SESSION_TOKEN, sessionTokenOpt.get)
     }
  
    reader
  }
  
  private def testIfAccessKeyEnabled(testName: String)(testBody: => Unit): Unit = {
    if (crossAccountEndpointUrl.isDefined
     && crossAccountStreamName.isDefined
     && stsRoleArnOpt.isDefined
     && awsAccessKeyIdOpt.isDefined
     && awsSecretKeyOpt.isDefined
   ) {
       test(testName)(testBody)
     } else {
       ignore(s"$testName [enable by setting env var $CROSS_ACCOUNT_KINESIS_END_POINT, $CROSS_ACCOUNT_KINESIS_STREAM_NAME, " +
           s"$KINESIS_STS_ROLE_ARN, $KINESIS_ACCESS_KEY_ID, $KINESIS_SECRET_KEY, $KINESIS_SESSION_TOKEN]")(testBody)
     }
  }
  
  testIfStsEnabled("Kinesis connector reads from another account using STS") {
   val reader = createCrossAccountSparkStsReadStream(consumerType, testUtils)
    runTestWithSparkRead(reader)
  }
  
  testIfStsEnabled("Kinesis connector reads from another account using custom credentials provider") {
    val reader = createCrossAccountSparkCustomReadStream(consumerType, testUtils)
    runTestWithSparkRead(reader)
  }
  
  testIfAccessKeyEnabled("Kinesis connector reads from another account using access key") {
   val reader = createCrossAccountSparkReadStream(consumerType,
         awsAccessKeyIdOpt.get,
         awsSecretKeyOpt.get,
         sessionTokenOpt,
         testUtils)
  
        runTestWithSparkRead(reader)
  }
  
  private def runTestWithSparkRead(reader: DataStreamReader): Unit = {
    
      pushDataToKinesis(Array("0"))

    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

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
