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
package org.apache.spark.sql.connector.kinesis.retrieval.polling

import scala.collection.JavaConverters._

import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.AfterSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtSequenceNumber
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatch
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisher
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus._
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard

class PollingRecordBatchPublisher(
      override val initialStartingPosition: KinesisPosition,
      override val streamShard: StreamShard,
      val kinesisConsumer: KinesisClientConsumer,
      val kinesisOptions: KinesisOptions,
      runningSupplier: => Boolean
  ) extends RecordBatchPublisher with Logging {

  logInfo(s"Initializing PollingRecordBatchPublisher for ${streamShard}")

  private val maxNumberOfRecordsPerFetch: Int = kinesisOptions.pollingNumberOfRecordsPerFetch
  private val fetchIntervalMillis: Long = kinesisOptions.pollingFetchIntervalMs

  // visible for test only
  var nextStartingPosition: KinesisPosition = initialStartingPosition

  private var nextShardItr: String = getShardIterator
  private var processingStartTimeNanos = System.nanoTime

  private var lastRecordBatchSize = 0
  private var lastRecordBatchSizeInBytes = 0


  override def runProcessLoop(recordBatchConsumer: RecordBatchConsumer): RecordsPublisherRunStatus = {
    logDebug(s"PollingRecordBatchPublisher runProcessLoop on ${streamShard}, startingPosition: ${nextStartingPosition}")
    
    val result = run((batch: RecordBatch) => {
      val latestSequenceNumber = recordBatchConsumer.accept(batch)
      lastRecordBatchSize = batch.numberOfDeaggregatedRecord
      lastRecordBatchSizeInBytes = batch.totalSizeInBytes
      latestSequenceNumber
    }, maxNumberOfRecordsPerFetch)
    adjustToFetchInterval(processingStartTimeNanos, System.nanoTime)
    processingStartTimeNanos = System.nanoTime
    result

  }

  private def run(consumer: RecordBatchConsumer,
          maxNumberOfRecords: Int): RecordsPublisherRunStatus = {

    if (!runningSupplier) return CANCELLED
    if (nextShardItr == null) return COMPLETE
    
    val result = getRecords(nextShardItr, maxNumberOfRecords)
    val recordBatch = RecordBatch(result.records().asScala.toSeq, streamShard, result.millisBehindLatest)
    val latestSequenceNumber = consumer.accept(recordBatch)
    nextStartingPosition = getNextStartingPosition(latestSequenceNumber, nextStartingPosition)
    nextShardItr = result.nextShardIterator

    if (nextShardItr == null) COMPLETE
    else INCOMPLETE
  }




  // It is important that this method is not called again before all the records from the
  // last result have been fully collected, otherwise latestSequenceNumber
  // may refer to a sub-record in the middle of an aggregated
  // record, leading to incorrect shard iteration if the iterator had to be refreshed.
  private def getRecords(shardItr: String, maxNumberOfRecords: Int): GetRecordsResponse = {
    var getRecordsResult: GetRecordsResponse = null
    var nextShardItr = shardItr
    while (getRecordsResult == null) {
      try {
        getRecordsResult = kinesisConsumer.getKinesisRecords(nextShardItr, maxNumberOfRecords)
      }
      catch {
        case eiEx: ExpiredIteratorException =>
          logError(s"Encountered an unexpected expired iterator ${nextShardItr} for shard ${streamShard}. refreshing the iterator ...", eiEx)
          nextShardItr = getShardIterator
          // sleep for the fetch interval before the next getRecords attempt with the
          // refreshed iterator
          if (fetchIntervalMillis != 0) Thread.sleep(fetchIntervalMillis)
      }
    }
    getRecordsResult
  }

  private def getShardIterator = {

    val iteratorType = if ( nextStartingPosition.iteratorType == AfterSequenceNumber.iteratorType
      && nextStartingPosition.subSequenceNumber != NO_SUB_SEQUENCE_NUMBER
      && !nextStartingPosition.isLast) {
      AtSequenceNumber.iteratorType // Need to reread aggregated record as last user record not processed
    } else {
      nextStartingPosition.iteratorType
    }

    kinesisConsumer.getShardIterator(
      streamShard.shard.shardId(),
      iteratorType,
      nextStartingPosition.iteratorPosition)
  }

  private def adjustToFetchInterval(processingStartTimeNanos: Long, processingEndTimeNanos: Long) = {
    var endTimeNanos = processingEndTimeNanos
    if (fetchIntervalMillis != 0) {
      val processingTimeNanos = processingEndTimeNanos - processingStartTimeNanos
      val sleepTimeMillis = fetchIntervalMillis - (processingTimeNanos / 1000000)
      if (sleepTimeMillis > 0) {
        Thread.sleep(sleepTimeMillis)
        endTimeNanos = System.nanoTime
      }
    }
    endTimeNanos
  }
}