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

import java.time.Instant

import scala.util.control.NonFatal

import software.amazon.awssdk.services.kinesis.model.ConsumerStatus.ACTIVE
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus.DELETING
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StartingPosition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer

object KinesisUtils extends Logging{

  def registerStreamConsumer(kinesisReader: KinesisClientConsumer,
                             consumerName: String): String = {
    val stream = kinesisReader.kinesisStreamName
    logInfo(s"Registering stream consumer - ${stream}::${consumerName}")

    val (streamArn: String, describeStreamConsumerResponse: Option[DescribeStreamConsumerResponse]) =
      describeStreamConsumer(kinesisReader, stream, consumerName)

    if (describeStreamConsumerResponse.isEmpty) {
      try {
        kinesisReader.registerStreamConsumer(streamArn, consumerName)
      } catch {
        case _: ResourceInUseException => // ignore it
        case NonFatal(e) => throw e
      }
    }

    val streamConsumerArn: String = waitForConsumerToBecomeActive(kinesisReader,
      describeStreamConsumerResponse,
      streamArn,
      consumerName)
    logInfo(s"Using stream consumer - ${streamConsumerArn}")
    streamConsumerArn
  }

  private def describeStreamConsumer(
                                      kinesisReader: KinesisClientConsumer,
                                      stream: String,
                                      streamConsumerName: String): (String, Option[DescribeStreamConsumerResponse]) = {
    val describeStreamSummaryResponse = kinesisReader.describeStreamSummary(stream)
    val streamArn: String = describeStreamSummaryResponse.streamDescriptionSummary.streamARN
    logInfo(s"Found stream ARN - ${streamArn}")
    val describeStreamConsumerResponse = returnNoneForResourceNotFound {
      kinesisReader.describeStreamConsumer(streamArn, streamConsumerName)
    }

    (streamArn, describeStreamConsumerResponse)
  }

  private def waitForConsumerToBecomeActive(kinesisReader: KinesisClientConsumer,
                                            describeStreamConsumerResponse: Option[DescribeStreamConsumerResponse],
                                            streamArn: String,
                                            streamConsumerName: String): String = {
    var attempt = 0
    val MAX_ATTEMPT = 30
    var response = describeStreamConsumerResponse
    while (
      attempt < MAX_ATTEMPT &&
        (response.isEmpty || (response.get.consumerDescription.consumerStatus != ACTIVE))
    ) {
      logInfo(s"Waiting for stream consumer to become active, attempt ${attempt} - ${streamArn}::${streamConsumerName}")

      response = returnNoneForResourceNotFound {
        kinesisReader.describeStreamConsumer(streamArn, streamConsumerName)
      }

      Thread.sleep(1000)
      attempt += 1
    }

    if (response.isEmpty || (response.get.consumerDescription.consumerStatus != ACTIVE)) {
      throw new Exception(s"Timeout waiting for stream consumer to become active:${streamConsumerName} on ${streamArn}")
    }
    response.get.consumerDescription.consumerARN
  }

  def deregisterStreamConsumer(kinesisReader: KinesisClientConsumer,
                               consumerName: String): Unit = {
    val stream = kinesisReader.kinesisStreamName
    logInfo(s"Deregistering stream consumer - ${stream}::${consumerName}")

    val (streamArn: String, describeStreamConsumerResponse: Option[DescribeStreamConsumerResponse]) =
      describeStreamConsumer(kinesisReader, stream, consumerName)

    if (describeStreamConsumerResponse.isDefined
      && describeStreamConsumerResponse.get.consumerDescription().consumerStatus() != DELETING
    ) {
      try {
        Some(kinesisReader.deregisterStreamConsumer(streamArn, consumerName))
      } catch {
        case NonFatal(e) => throw e
      }
    }

    waitForConsumerToDeregister(kinesisReader, describeStreamConsumerResponse, streamArn, consumerName)
    logDebug(s"Deregistered stream consumer -  - ${stream}::${consumerName}")
  }

  private def waitForConsumerToDeregister(kinesisReader: KinesisClientConsumer,
                                          describeStreamConsumerResponse: Option[DescribeStreamConsumerResponse],
                                          streamArn: String,
                                          streamConsumerName: String): Unit = {
    var attempt = 0
    val MAX_ATTEMPT = 30
    var response = describeStreamConsumerResponse
    val backoffManager = new FullJitterBackoffManager()
    while (
      attempt < MAX_ATTEMPT
        && response.isDefined
    ) {
      logInfo(s"Wait for stream consumer to be deregistered, attempt ${attempt} - ${streamArn}::${streamConsumerName}")

      response = returnNoneForResourceNotFound {
        kinesisReader.describeStreamConsumer(streamArn, streamConsumerName)
      }

      deregistrationBackoff(backoffManager, attempt)
      attempt += 1
    }

    if (response.isDefined) {
      throw new Exception(s"Timeout waiting for deregistering stream consumer: ${streamConsumerName} on ${streamArn}")
    }
  }

  private def deregistrationBackoff(backoff: FullJitterBackoffManager, attempt: Int): Unit = {
    val backoffMillis = backoff.calculateFullJitterBackoff(attempt)
    backoff.sleep(backoffMillis)
  }

  def toSdkStartingPosition(shardInfo: ShardInfo): StartingPosition = {
    val builder = StartingPosition.builder().`type`(
      shardInfo.iteratorType match {
        case AfterSequenceNumber.iteratorType if (shardInfo.isAggregated && !shardInfo.isLast) =>
          AtSequenceNumber.iteratorType // Need to start from the same position for unprocessed subSequenceNumber
        case t => t
      }
    )

    shardInfo.iteratorType match {
      case AtTimeStamp.iteratorType =>
        builder.timestamp(Instant.ofEpochMilli(shardInfo.iteratorPosition.toLong))
      case AtSequenceNumber.iteratorType | AfterSequenceNumber.iteratorType =>
        builder.sequenceNumber(shardInfo.iteratorPosition)
      case _ =>
    }

    builder.build();
  }

  private def returnNoneForResourceNotFound[T](body: => T): Option[T] = {
    try {
      Some(body)
    } catch {
      case rne if throwableIsCausedBy[ResourceNotFoundException](rne) =>
        None
      case NonFatal(e) =>
        throw e
    }
  }

}
