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

import java.time.Instant
import scala.collection.JavaConverters._
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.Shard
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM
case class RecordBatch (
    private val rawRecords: Seq[Record],
    streamShard: StreamShard,
    millisBehindLatest: Long) {

  private lazy val aggregatorUtil = new AggregatorUtil
  lazy val numberOfRawRecords: Int = rawRecords.length
  lazy val numberOfDeaggregatedRecord: Int = userRecords.length
  lazy val userRecords: Seq[KinesisUserRecord] = deaggregateRecords
  lazy val totalSizeInBytes: Int = userRecords.map(_.data.length).sum


  private def deaggregateRecords: Seq[KinesisUserRecord] = {
    val hashKeyRange = streamShard.shard.hashKeyRange()
    if (hashKeyRange != null) {
      aggregatorUtil.deaggregate(
        rawRecords.asJava,
        streamShard.shard.hashKeyRange().startingHashKey(),
        streamShard.shard.hashKeyRange().endingHashKey(),
        millisBehindLatest).asScala.toSeq
    } else {
      aggregatorUtil.deaggregate(
        rawRecords.asJava,
        millisBehindLatest
      ).asScala.toSeq
    }
  }

}

case class StreamShard(
    streamName: String,
    shard: Shard) {
  override def toString: String = s"StreamShard($streamName, ${shard.shardId()})"
}

case class KinesisUserRecord (sequenceNumber: SequenceNumber,
                              approximateArrivalTimestamp: Instant,
                              data: Array[Byte],
                              partitionKey: String,
                              encryptionType: EncryptionType,
                              millisBehindLatest: Long,
                              fromAggregated: Boolean,
                              totalSubSequence: Long,
                              explicitHashKey: String,
                             ) {
  def isLastSubSequence: Boolean = {
    sequenceNumber.subSequenceNumber == NO_SUB_SEQUENCE_NUMBER || sequenceNumber.isLast
  }
}

object KinesisUserRecord {
  private val NO_EXPLICIT_HASH_KEY: String = ""

  private val EMPTY_USER_RECORD = new KinesisUserRecord(null, null, null, null, null, -1, false, -1, null)

  def apply(record: Record,
            millisBehindLatest: Long,
  ): KinesisUserRecord = {

    KinesisUserRecord(
      sequenceNumber = SequenceNumber(
        record.sequenceNumber(), NO_SUB_SEQUENCE_NUMBER, isLast = true
      ),
      approximateArrivalTimestamp = record.approximateArrivalTimestamp(),
      data = record.data().asByteArray(),
      partitionKey = record.partitionKey(),
      encryptionType = record.encryptionType(),
      millisBehindLatest = millisBehindLatest,
      fromAggregated = false,
      totalSubSequence = 0,
      explicitHashKey = NO_EXPLICIT_HASH_KEY)
  }

  def emptyUserRecord(userRecord: KinesisUserRecord): Boolean = {
    // all fields exception for millisBehindLatest need to be the same as EMPTY_USER_RECORD
    userRecord.sequenceNumber == EMPTY_USER_RECORD.sequenceNumber &&
      userRecord.approximateArrivalTimestamp == EMPTY_USER_RECORD.approximateArrivalTimestamp &&
      userRecord.data == EMPTY_USER_RECORD.data &&
      userRecord.partitionKey == EMPTY_USER_RECORD.partitionKey &&
      userRecord.encryptionType == EMPTY_USER_RECORD.encryptionType &&
      userRecord.fromAggregated == EMPTY_USER_RECORD.fromAggregated &&
      userRecord.totalSubSequence == EMPTY_USER_RECORD.totalSubSequence &&
      userRecord.explicitHashKey == EMPTY_USER_RECORD.explicitHashKey
  }

  def nonEmptyUserRecord(userRecord: KinesisUserRecord): Boolean = {
    !emptyUserRecord(userRecord)
  }

  def createEmptyUserRecord(millisBehindLatest: Long): KinesisUserRecord = {
    EMPTY_USER_RECORD.copy(millisBehindLatest = millisBehindLatest)
  }

  def createShardEndUserRecord: KinesisUserRecord = {
    new KinesisUserRecord(
      SENTINEL_SHARD_ENDING_SEQUENCE_NUM,
      null, null, null, null, -1, false, -1, null)
  }

  def shardEndUserRecord(userRecord: KinesisUserRecord): Boolean = {
    userRecord.sequenceNumber == SENTINEL_SHARD_ENDING_SEQUENCE_NUM
  }
}

