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

import org.apache.spark.sql.connector.kinesis.AfterSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtTimeStamp
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.ShardEnd
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.RecordsPublisherRunStatus
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM

trait RecordBatchPublisher {
  def runProcessLoop(recordBatchConsumer: RecordBatchConsumer): RecordsPublisherRunStatus
  def initialStartingPosition: KinesisPosition
  def streamShard: StreamShard

  protected def getNextStartingPosition(latestSequenceNumber: SequenceNumber,
                                        nextStartingPosition: KinesisPosition): KinesisPosition = {
    latestSequenceNumber match {
      // When consuming from a timestamp sentinel/AT_TIMESTAMP ShardIteratorType.
      // If the first RecordBatch has no de-aggregated records, then the latestSequenceNumber would
      // be the timestamp sentinel.
      // This is because we have not yet received any real sequence numbers on this shard.
      // In this condition we should retry from the previous starting position (AT_TIMESTAMP).
      case SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM =>
        assert(nextStartingPosition.isInstanceOf[AtTimeStamp])
        nextStartingPosition
      case _ =>
        SequenceNumber.continueFromSequenceNumber(latestSequenceNumber)
    }
  }

}

object RecordBatchPublisherRunStatus extends Enumeration {
  type RecordsPublisherRunStatus = Value
  val COMPLETE = Value
  val INCOMPLETE = Value
  val CANCELLED = Value

}

trait RecordBatchConsumer {
  def accept(recordBatch: RecordBatch): SequenceNumber
}

case class SequenceNumber(
       sequenceNumber: String,
       subSequenceNumber: Long,
       isLast: Boolean
) {
  def isAggregated: Boolean = SequenceNumber.isAggregated(subSequenceNumber)
}

object SequenceNumber {
  val SENTINEL_LATEST_SEQUENCE_NUM = new SequenceNumber("SENTINEL_LATEST_SEQUENCE_NUM", -1, true)
  val SENTINEL_EARLIEST_SEQUENCE_NUM = new SequenceNumber("SENTINEL_EARLIEST_SEQUENCE_NUM", -1, true)
  val SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM = new SequenceNumber("SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM", -1, true)
  val SENTINEL_SHARD_ENDING_SEQUENCE_NUM = new SequenceNumber("SENTINEL_SHARD_ENDING_SEQUENCE_NUM", -1, true)

  def isSentinelSequenceNumber(sequenceNumber: SequenceNumber): Boolean = {
    sequenceNumber match {
      case SENTINEL_LATEST_SEQUENCE_NUM |
           SENTINEL_EARLIEST_SEQUENCE_NUM |
           SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM |
           SENTINEL_SHARD_ENDING_SEQUENCE_NUM => true
      case _ => false
    }
  }

  def isAggregated(subSequenceNumber: Long): Boolean = subSequenceNumber >= 0

  def continueFromSequenceNumber(sequenceNumber: SequenceNumber): KinesisPosition = {
    sequenceNumber match {
      case SENTINEL_LATEST_SEQUENCE_NUM => new Latest()
      case SENTINEL_EARLIEST_SEQUENCE_NUM => new TrimHorizon()
      case _ if isSentinelSequenceNumber(sequenceNumber) =>
        throw new IllegalArgumentException("Unexpected sequenceNumber: " + sequenceNumber)
      case _ => new AfterSequenceNumber(sequenceNumber.sequenceNumber,
        sequenceNumber.subSequenceNumber,
        sequenceNumber.isLast
      )
    }
  }

  def toSequenceNumber(kinesisPosition: KinesisPosition): SequenceNumber = {
    kinesisPosition.iteratorType match {
      case TrimHorizon.iteratorType => SENTINEL_EARLIEST_SEQUENCE_NUM
      case Latest.iteratorType => SENTINEL_LATEST_SEQUENCE_NUM
      case ShardEnd.iteratorType => SENTINEL_SHARD_ENDING_SEQUENCE_NUM
      case AtTimeStamp.iteratorType => SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM
      case _ => new SequenceNumber(kinesisPosition.iteratorPosition,
        kinesisPosition.subSequenceNumber,
        kinesisPosition.isLast
      )

    }
  }




}


