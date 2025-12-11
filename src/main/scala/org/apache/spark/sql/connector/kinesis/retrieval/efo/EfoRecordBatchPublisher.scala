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
package org.apache.spark.sql.connector.kinesis.retrieval.efo

import java.util.function.Consumer
import scala.collection.JavaConverters._
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.FullJitterBackoffManager
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatch
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisher
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus._
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRecordBatchPublisher.RECOVERABLE_RETRY_MULTIPLIER

class EfoRecordBatchPublisher (
                                override val initialStartingPosition: KinesisPosition,
                                val consumerArn: String,
                                override val streamShard: StreamShard,
                                val kinesisConsumer: KinesisClientConsumer,
                                val kinesisOptions: KinesisOptions,
                                runningSupplier: => Boolean,
                                backoff: Option[FullJitterBackoffManager] = None
                              ) extends RecordBatchPublisher with Logging {

  logInfo(s"Initializing EfoRecordBatchPublisher ${streamShard}")
  
  val backoffManager: FullJitterBackoffManager = backoff.getOrElse(new FullJitterBackoffManager())
  var nextStartingPosition: KinesisPosition = initialStartingPosition

  /** The current attempt in the case of subsequent recoverable errors. */
  private var attempt = 0

  override def runProcessLoop(recordBatchConsumer: RecordBatchConsumer): RecordsPublisherRunStatus = {
    logDebug(s"EfoRecordBatchPublisher runProcessLoop on ${streamShard}, startingPosition: ${nextStartingPosition}")
    val eventConsumer = new Consumer[SubscribeToShardEvent]() {
      override def accept(event: SubscribeToShardEvent): Unit = {
        val recordBatch = RecordBatch (event.records.asScala.toSeq, streamShard, event.millisBehindLatest)
        val sequenceNumber = recordBatchConsumer.accept (recordBatch)
        nextStartingPosition = getNextStartingPosition (sequenceNumber, nextStartingPosition)
      }}

    val result = runWithBackoff(eventConsumer)

    logInfo(s"EfoRecordBatchPublisher.runProcessLoop on ${streamShard} finished with status ${result}")

    result
  }

  private def runWithBackoff(eventConsumer: Consumer[SubscribeToShardEvent]): RecordsPublisherRunStatus = {

    val fanOutShardSubscriber = EfoShardSubscriber(consumerArn,
      streamShard,
      kinesisConsumer,
      kinesisOptions.efoSubscribeToShardTimeout,
      runningSupplier)

    try {
      val res = fanOutShardSubscriber.subscribeToShardAndConsumeRecords(nextStartingPosition, eventConsumer)
      attempt = 0
      res
    } catch {
      case ex: EfoSubscriberInterruptedException =>
        logInfo(s"Thread interrupted, closing record publisher for shard ${streamShard}.", ex)
        CANCELLED
      case ex: EfoSubscriberException =>
        if (ex.getCause.isInstanceOf[ResourceNotFoundException]) {
          logWarning(s"Received ResourceNotFoundException. " +
            s"Either the shard does not exist, or the stream subscriber has been deregistered. " +
            s"Marking this shard as incomplete ${consumerArn}::${streamShard}")
          // mark it as incomplete for the safe side so that the upper layer can retry
          INCOMPLETE
        }
        else if (attempt >=
          (if (ex.isInstanceOf[EfoRecoverableSubscriberException])
            kinesisOptions.efoSubscribeToShardMaxRetries * RECOVERABLE_RETRY_MULTIPLIER // Recoverable errors retry more times
          else kinesisOptions.efoSubscribeToShardMaxRetries)
        ) {
          val errorMessage = s"Maximum retries exceeded for SubscribeToShard: failed ${attempt} times."
          logError(errorMessage, ex)
          throw new RuntimeException(errorMessage, ex.getCause)
        }
        else {
          logWarning("runWithBackoff gets error, backoff retrying...", ex)
          attempt += 1
          backoff(ex)
          INCOMPLETE
        }
    } finally {
      fanOutShardSubscriber.close()
    }
  }

  private def backoff(ex: Throwable): Unit = {
    val backoffMillis = backoffManager.calculateFullJitterBackoff(attempt)

    logWarning(s"Encountered error ${ex.getCause.getClass.getSimpleName}." +
      s" Backing off for ${backoffMillis} millis ${consumerArn}:${streamShard})")
    backoffManager.sleep(backoffMillis)
  }
}

object EfoRecordBatchPublisher {
  val RECOVERABLE_RETRY_MULTIPLIER = 2
}
