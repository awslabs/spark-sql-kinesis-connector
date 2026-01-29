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

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import software.amazon.awssdk.auth.credentials.{AwsCredentials, DefaultCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.agg.RecordAggregator
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatch
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber




class KinesisTestUtils(streamShardCount: Int = 2) extends Logging {

  val endpointUrl: String = KinesisTestUtils.endpointUrl
  val regionName: String = getRegionNameByEndpoint(endpointUrl)

  private val createStreamTimeoutSeconds = 300
  private val describeStreamPollTimeSeconds = 1

  @volatile
  private var streamCreated = false

  @volatile
  private var _streamName: String = _

  protected lazy val kinesisClient: KinesisClient = {
    KinesisClient.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .endpointOverride(URI.create(endpointUrl))
      .build()
  }

  protected def getProducer(aggregate: Boolean): KinesisDataGenerator = {
    if (!aggregate) {
      new SimpleDataGenerator(kinesisClient)
    } else {
      throw new UnsupportedOperationException("Aggregation is not supported through this code path")
    }
  }


  def streamName: String = {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    _streamName
  }

  def setStreamName(streamName: String): Unit = {
    _streamName = streamName
    waitForStreamToBeActive(_streamName)
    streamCreated = true
    logInfo(s"set stream ${_streamName}")
  }

  def createStream(): Unit = {
    require(!streamCreated, "Stream already created")
    _streamName = findNonExistentStreamName()

    // Create a stream. The number of shards determines the provisioned throughput.
    logInfo(s"Creating stream ${_streamName}")
    val createStreamRequest = CreateStreamRequest.builder()
      .streamName(_streamName)
      .shardCount(streamShardCount)
      .build()
    kinesisClient.createStream(createStreamRequest)

    // The stream is now being created. Wait for it to become active.
    waitForStreamToBeActive(_streamName)
    streamCreated = true
    logInfo(s"Created stream ${_streamName}")
  }

  def getShards(): Seq[Shard] = {
    val describeStreamRequest = DescribeStreamRequest.builder()
      .streamName(_streamName)
      .build()
    kinesisClient.describeStream(describeStreamRequest).streamDescription().shards().asScala.toSeq
  }

  def splitShard(shardId: String): Unit = {
    // Set a half of the max hash value
    val splitShardRequest = SplitShardRequest.builder()
      .streamName(_streamName)
      .shardToSplit(shardId).newStartingHashKey("170141183460469231731687303715884105728")
      .build()
    kinesisClient.splitShard(splitShardRequest)
    // Wait for the shards to become active
    waitForStreamToBeActive(_streamName)
  }

  def splitShard : (Integer, Integer) = {
    val shardToSplit = getShards().head
    splitShard(shardToSplit.shardId())
    val (splitOpenShards, splitCloseShards) = getShards().partition {
      shard => shard.sequenceNumberRange().endingSequenceNumber() == null
    }
    (splitOpenShards.size, splitCloseShards.size)
  }

  def mergeShard(shardToMerge: String, adjacentShardToMerge: String): Unit = {
    val mergeShardRequest = MergeShardsRequest.builder()
      .streamName(_streamName)
      .shardToMerge(shardToMerge)
      .adjacentShardToMerge(adjacentShardToMerge)
      .build()
    kinesisClient.mergeShards(mergeShardRequest)
    // Wait for the shards to become active
    waitForStreamToBeActive(_streamName)
  }


  def mergeShard: (Integer, Integer) = {
    val (openShard, _) = getShards().partition {
      shard => shard.sequenceNumberRange().endingSequenceNumber() == null
    }
    val Seq(shardToMerge, adjShard) = openShard
    mergeShard(shardToMerge.shardId(), adjShard.shardId())

    val (mergedOpenShards, mergedCloseShards) =
      getShards().partition {
        shard => shard.sequenceNumberRange().endingSequenceNumber() == null
      }
    (mergedOpenShards.size, mergedCloseShards.size)
  }

  /**
   * Push data to Kinesis stream and return a map of
   * shardId -> seq of (data, seq number) pushed to corresponding shard
   */
  def pushData(testData: Array[String], aggregate: Boolean,
               pkOption: Option[String] = None): Map[String, ArrayBuffer[(String, String)]] = {
    require(streamCreated, "Stream not yet created, call createStream() to create one")
    logInfo(s"Push data aggregate ${aggregate}")
    val producer = getProducer(aggregate)
    val shardIdToSeqNumbers = producer.sendData(streamName, testData, pkOption)
    logDebug(s"Pushed data ${testData.mkString("Array(", ", ", ")")}:\n\t ${shardIdToSeqNumbers.mkString("\n\t")}")
    shardIdToSeqNumbers
  }

  def pushData(testData: java.util.List[String]): Unit = {
    pushData(testData.asScala.toArray, aggregate = false)
  }

  def deleteStream(): Unit = {
    try {
      if (streamCreated) {
        val deleteStreamRequest = DeleteStreamRequest.builder().streamName(streamName).build()
        kinesisClient.deleteStream(deleteStreamRequest)
      }
    } catch {
      case e: Exception =>
        logWarning(s"Could not delete stream $streamName. Exception ${e.getMessage}", e)
    }
  }

  private def describeStream(streamNameToDescribe: String): Option[StreamDescription] = {
    try {
      val describeStreamRequest = DescribeStreamRequest.builder().streamName(streamNameToDescribe).build()
      val desc = kinesisClient.describeStream(describeStreamRequest).streamDescription()
      Some(desc)
    } catch {
      case _: ResourceNotFoundException =>
        None
    }
  }

  private def findNonExistentStreamName(): String = {
    var testStreamName: String = null
    do {
      Thread.sleep(TimeUnit.SECONDS.toMillis(describeStreamPollTimeSeconds))
      testStreamName = s"KinesisTestUtils-${math.abs(Random.nextLong())}"
    } while (describeStream(testStreamName).nonEmpty)
    testStreamName
  }

  private def waitForStreamToBeActive(streamNameToWaitFor: String): Unit = {
    val startTime = System.currentTimeMillis()
    val endTime = startTime + TimeUnit.SECONDS.toMillis(createStreamTimeoutSeconds)
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(describeStreamPollTimeSeconds))
      describeStream(streamNameToWaitFor).foreach { description =>
        val streamStatus = description.streamStatus()
        logInfo(s"\t $streamNameToWaitFor - current state: $streamStatus\n")
        if (streamStatus == "ACTIVE") {
          Thread.sleep(TimeUnit.SECONDS.toMillis(10)) // Wait for extra time to ensure the status is stable
          return
        }
      }
    }
    require(false, s"Stream $streamName never became active")
  }

}

object KinesisTestUtils {

  val endVarNameForEndpoint = "KINESIS_TEST_ENDPOINT_URL"
  val defaultEndpointUrl = "https://kinesis.us-east-2.amazonaws.com"
  val regionName: String = getRegionNameByEndpoint(endpointUrl)

  lazy val endpointUrl: String = {
    val url = sys.env.getOrElse(endVarNameForEndpoint, defaultEndpointUrl)
    // scalastyle:off println
    // Print this so that they are easily visible on the console and not hidden in the log4j logs.
    println(s"Using endpoint URL $url for creating Kinesis streams for tests.")
    // scalastyle:on println
    url
  }

  def isAWSCredentialsPresent: Boolean = {
    Try { DefaultCredentialsProvider.create().resolveCredentials() }.isSuccess
  }

  def getAWSCredentials: AwsCredentials = {
    Try { DefaultCredentialsProvider.create().resolveCredentials() } match {
      case Success(cred) =>
        cred
      case Failure(_) =>
        throw new Exception(
          s"""
             |Could not find AWS credentials. Please follow instructions in AWS documentation
             |to set the credentials in your system such that the DefaultAWSCredentialsProviderChain
             |can find the credentials.
           """.stripMargin)
    }
  }
}

class TestConsumer(private val initialStartingPosition: KinesisPosition) extends RecordBatchConsumer {
  private val recordBatches: util.List[RecordBatch] = new util.ArrayList[RecordBatch]
  private var latestSequenceNumber = SequenceNumber.toSequenceNumber(initialStartingPosition)

  override def accept(batch: RecordBatch): SequenceNumber = {
    recordBatches.add(batch)
    val records = batch.userRecords
    if (records.nonEmpty) {
      latestSequenceNumber = records.last.sequenceNumber
    }

    latestSequenceNumber
  }

  def getRecordBatches: util.List[RecordBatch] = recordBatches
}

/** A wrapper interface that will allow us to consolidate the code for synthetic data generation. */
trait KinesisDataGenerator {
  /** Sends the data to Kinesis and returns the metadata for everything that has been sent. */
  def sendData(streamName: String, data: Array[String], pkOption: Option[String]):
  Map[String, ArrayBuffer[(String, String)]]
}

class SimpleDataGenerator(client: KinesisClient) extends KinesisDataGenerator {
  override def sendData(streamName: String, data: Array[String], pkOption: Option[String]):
  Map[String, ArrayBuffer[(String, String)]] = {
    val shardIdToSeqNumbers =
      new mutable.HashMap[String, ArrayBuffer[(String, String)]]()
    data.foreach { num =>
      val data = ByteBuffer.wrap(num.getBytes(StandardCharsets.UTF_8))
      val putRecordRequest = PutRecordRequest.builder()
        .streamName(streamName)
        .data(SdkBytes.fromByteBuffer(data))
        .partitionKey(pkOption.getOrElse(num))
        .build()

      val putRecordResult = client.putRecord(putRecordRequest)
      val shardId = putRecordResult.shardId()
      val seqNumber = putRecordResult.sequenceNumber()
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(String, String)]())
      sentSeqNumbers += ((num, seqNumber))
    }

    shardIdToSeqNumbers.toMap
  }
}

// support both simple and aggregated data generator
class EnhancedKinesisTestUtils(streamShardCount: Int)
  extends KinesisTestUtils(streamShardCount) {
  override protected def getProducer(aggregate: Boolean): KinesisDataGenerator = {
    if (!aggregate) {
      logInfo("getProducer uses SimpleDataGenerator")
      new SimpleDataGenerator(kinesisClient)
    } else {
      logInfo("getProducer uses AggregateDataGenerator")
      new AggregateDataGenerator(kinesisClient)
    }
  }
}

/** A wrapper for the record aggregator. */
class AggregateDataGenerator( client: KinesisClient) extends KinesisDataGenerator {
  override def sendData(streamName: String, data: Array[String], pkOption: Option[String]):
  Map[String, ArrayBuffer[(String, String)]] = {

    val recordAggregator = new RecordAggregator

    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(String, String)]]()

    val pk = pkOption.getOrElse(randomAlphabetic(2))
    data.foreach { num =>
      recordAggregator.addUserRecord(pk, num.getBytes(UTF_8))
    }

    val aggregatedData = ByteBuffer.wrap(recordAggregator.clearAndGet.toRecordBytes)
    val putRecordRequest = PutRecordRequest.builder()
      .streamName(streamName)
      .data(SdkBytes.fromByteBuffer(aggregatedData))
      .partitionKey(pk).build()

    val putRecordResult = client.putRecord(putRecordRequest)
    val shardId = putRecordResult.shardId()
    val seqNumber = putRecordResult.sequenceNumber()
    val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId, new ArrayBuffer[(String, String)]())
    sentSeqNumbers += ((pk, seqNumber))

    shardIdToSeqNumbers.toMap
  }
}

