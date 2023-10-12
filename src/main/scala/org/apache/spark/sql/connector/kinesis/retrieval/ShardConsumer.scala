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

package org.apache.spark.sql.connector.kinesis.retrieval

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.CANCELLED
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.COMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM

object ShardConsumer {

  /** An exception wrapper to indicate an error has been thrown from the shard consumer. */
  class ShardConsumerException(message: String) extends RuntimeException(message) {}

  /** An exception to indicate the shard consumer has been cancelled. */
  class ShardConsumerCancelledException(message: String) extends ShardConsumer.ShardConsumerException(message) {}
  class ShardConsumerInterruptedException(message: String) extends ShardConsumer.ShardConsumerException(message) {}
}

class ShardConsumer(
      val dataReader: DataReceiver,
      val recordBatchPublisher: RecordBatchPublisher) extends Runnable with Logging{

  private var lastSequenceNum: SequenceNumber = SequenceNumber.toSequenceNumber(recordBatchPublisher.initialStartingPosition)
  val streamShard = recordBatchPublisher.streamShard

  override def run(): Unit = {
    try {
      breakable{
        while (isRunning) {
          val result = recordBatchPublisher.runProcessLoop((batch: RecordBatch) => {

            if (batch.userRecords.nonEmpty) {
              logDebug(s"${streamShard} - millis behind latest: ${batch.millisBehindLatest}," +
                s" batch size: ${batch.totalSizeInBytes}, " +
                s"number of raw Records ${batch.numberOfRawRecords}")

              batch.userRecords.foreach { userRecord =>
                if (filterDeaggregatedRecord(userRecord)) {
                  enqueueRecord(userRecord)
                }
              }
            } else {
              logDebug(s"empty user record - ${streamShard} for batch ${batch}")

              if (batch.millisBehindLatest > 0) {
                // to avoid upper layer timeout
                enqueueRecord(KinesisUserRecord.getEmptyUserRecord(batch.millisBehindLatest))
              }
            }
            
            lastSequenceNum
          })


          if (result == COMPLETE) {
            dataReader.updateState(streamShard, SENTINEL_SHARD_ENDING_SEQUENCE_NUM)
            // close this consumer as reached the end of the subscribed shard
            break
          }
          else if (isRunning && (result == CANCELLED)) {
            val errorMessage = s"Shard consumer cancelled: ${streamShard}"
            logInfo(errorMessage)
            throw new ShardConsumer.ShardConsumerCancelledException(errorMessage)
          }
        }
      }

      logInfo(s"ShardConsumer run loop done for ${streamShard}.")

    }
    catch {
      case t: InterruptedException =>
        val errorMessage = s"Shard consumer interrupted: ${streamShard}"
        logWarning(errorMessage, t)
        dataReader.stopWithError(new ShardConsumer.ShardConsumerInterruptedException(errorMessage))
      case t: Throwable =>
        dataReader.stopWithError(t)
    }
  }


   // The loop in run() checks this before fetching next batch of records.
  private def isRunning = (!Thread.interrupted) && dataReader.isRunning


  private def enqueueRecord(record: KinesisUserRecord): Unit = synchronized {
    var result = false

    breakable {
      while (isRunning && !result) {
        result = dataReader.enqueueRecord(streamShard, record)
        logDebug(s"ShardConsumer.enqueueRecord record sequenceNumber ${record.sequenceNumber}, result ${result}")

        if (KinesisUserRecord.emptyUserRecord(record)) {
          break // Don't retry empty user record
        }
      }
    }

    if (result && KinesisUserRecord.nonEmptyUserRecord(record)) {
      this.lastSequenceNum = record.sequenceNumber
    }

  }

  /**
   * Filters out aggregated records that have previously been processed. This method is to support
   * restarting from a partially consumed aggregated sequence number.
   *
   * @param record the record to filter
   * @return true if the record should be retained
   */
  private def filterDeaggregatedRecord(record: KinesisUserRecord): Boolean = {
    if (!lastSequenceNum.isAggregated) return true
    !(record.sequenceNumber.sequenceNumber == lastSequenceNum.sequenceNumber)||
      record.sequenceNumber.subSequenceNumber > lastSequenceNum.subSequenceNumber
  }
}
