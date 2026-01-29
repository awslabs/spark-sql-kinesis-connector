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

package org.apache.spark.sql.connector.kinesis.retrieval.client

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.StringUtils
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.HashKeyRange
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange
import software.amazon.awssdk.services.kinesis.model.Shard

import org.apache.spark.sql.connector.kinesis.agg.RecordAggregator
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard

object FakePollingClientConsumerFactory {

  def noShardsFoundForRequestedStreams: KinesisClientConsumer = new FakeKinesisClientConsumerAdapter() {
    override def getShards: Seq[Shard] = Seq.empty[Shard]

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = null

    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = null
  }

  def emptyShard(numberOfIterations: Int): KinesisClientConsumer = new SingleShardEmittingZeroRecords(numberOfIterations)

  def nonReshardedStreamsBehaviour(streamName: String,
                                   shardCount: Integer): KinesisClientConsumer = {
    new NonReshardedStreamsKinesis(streamName, shardCount)
  }

  def totalNumOfRecordsAfterNumOfGetRecordsCalls(numOfRecords: Int,
                                                 numOfGetRecordsCalls: Int,
                                                 millisBehindLatest: Long): KinesisClientConsumer = {
    new SingleShardEmittingFixNumOfRecordsKinesis(numOfRecords,
      numOfGetRecordsCalls,
      millisBehindLatest)
  }

  def totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(numOfRecords: Int,
                                                                              numOfGetRecordsCall: Int,
                                                                              orderOfCallToExpire: Int,
                                                                              millisBehindLatest: Long): KinesisClientConsumer = {
    new SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(numOfRecords,
      numOfGetRecordsCall,
      orderOfCallToExpire,
      millisBehindLatest)
  }

  def aggregatedRecords(numOfAggregatedRecords: Int, numOfChildRecords: Int, numOfGetRecordsCalls: Int): KinesisClientConsumer = {
    new SingleShardEmittingAggregatedRecordsKinesis(numOfAggregatedRecords, numOfChildRecords, numOfGetRecordsCalls)
  }

  def blockingQueueGetRecords(streamName: String, shardQueues: Seq[BlockingQueue[String]]): KinesisClientConsumer = {
    new BlockingQueueKinesis(streamName, shardQueues)
  }

  def generateFromShardOrder(order: Int): String = "shardId-%012d".format(order)

  def createDummyStreamShard: StreamShard = createDummyStreamShard("stream-name", "000000")

  def createDummyStreamShard(streamName: String, shardId: String): StreamShard = {
    createDummyStreamShard(streamName,
      shardId,
      HashKeyRange.builder()
        .startingHashKey("0")
        .endingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString)
        .build
    )
  }

  def createDummyStreamShard(streamName: String, shardId: String, hashKeyRange: HashKeyRange): StreamShard = {

    val shard = Shard.builder()
      .sequenceNumberRange(SequenceNumberRange.builder()
        .startingSequenceNumber("0")
        .endingSequenceNumber("9999999999999")
        .build
      )
      .hashKeyRange(hashKeyRange)
      .shardId(shardId)
      .build
    StreamShard(streamName, shard)
  }

  class SingleShardEmittingZeroRecords (var remainingIterators: Int) extends FakeKinesisClientConsumerAdapter {

    override def getShards: Seq[Shard] = null

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = {
      val t = remainingIterators.toString
      remainingIterators -= 1
      t
    }

    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
      GetRecordsResponse.builder()
        .millisBehindLatest(0)
        .nextShardIterator(
          if (remainingIterators == 0) null
          else {
            val t = remainingIterators.toString
            remainingIterators -= 1
            t
          }
        )
        .build
    }
  }

  class NonReshardedStreamsKinesis(streamName: String, shardCount: Integer) extends FakeKinesisClientConsumerAdapter {

    private val dummyShards = mutable.ArrayBuffer.empty[StreamShard]

    if (shardCount == 0) {
    }
    else {
      (0 until shardCount).foreach { i =>
        val t: StreamShard = createDummyStreamShard(streamName, generateFromShardOrder(i))
        dummyShards += t
      }
    }

    override def getShards: Seq[Shard] = {
        dummyShards.map(_.shard).toSeq
      }

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = null


    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = null
  }

  object SingleShardEmittingFixNumOfRecordsKinesis {
    def createRecordBatchWithRange(min: Int, max: Int): Seq[Record] = {
      val batch = mutable.ArrayBuffer.empty[Record]
      (min until max).foreach { i =>
        batch += Record.builder()
          .data(SdkBytes.fromByteArray(String.valueOf(i).getBytes(StandardCharsets.UTF_8)))
          .partitionKey(UUID.randomUUID.toString)
          .approximateArrivalTimestamp(new Date(System.currentTimeMillis).toInstant)
          .sequenceNumber(i.toString)
          .build
      }
      batch.toSeq
    }
  }

  class SingleShardEmittingFixNumOfRecordsKinesis(totalNumOfRecords: Int,
                                                  totalNumOfGetRecordsCalls: Int,
                                                  millisBehindLatest: Long) extends FakeKinesisClientConsumerAdapter {

    // initialize the record batches that we will be fetched
    val shardItrToRecordBatch = mutable.Map.empty[String, Seq[Record]]
    var numOfAlreadyPartitionedRecords = 0
    val numOfRecordsPerBatch: Int = totalNumOfRecords / totalNumOfGetRecordsCalls + 1
    (0 until totalNumOfGetRecordsCalls).foreach { batch =>
      if (batch != totalNumOfGetRecordsCalls - 1) {
        shardItrToRecordBatch.put(
          batch.toString,
          SingleShardEmittingFixNumOfRecordsKinesis.createRecordBatchWithRange(
            numOfAlreadyPartitionedRecords,
            numOfAlreadyPartitionedRecords + numOfRecordsPerBatch))
        numOfAlreadyPartitionedRecords += numOfRecordsPerBatch
      }
      else {
        shardItrToRecordBatch.put(
          batch.toString,
          SingleShardEmittingFixNumOfRecordsKinesis.createRecordBatchWithRange(numOfAlreadyPartitionedRecords, totalNumOfRecords))
      }
    }

    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
      // assuming that the limit is always large enough
      GetRecordsResponse.builder()
        .records(shardItrToRecordBatch(shardIterator): _*)
        .millisBehindLatest(millisBehindLatest)
        .nextShardIterator(
          if (shardIterator.toInt == totalNumOfGetRecordsCalls - 1) null // last next shard iterator is null
          else (shardIterator.toInt + 1).toString
        )
        .build
    }

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = {
      // Should be called only once. Simply return the iterator of the first batch of records
      "0"
    }

    override def getShards: Seq[Shard] = null
  }


  class SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(numOfRecords: Int,
                                                                     numOfGetRecordsCalls: Int,
                                                                     orderOfCallToExpire: Int,
                                                                     millisBehindLatest: Long)
    extends SingleShardEmittingFixNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls, millisBehindLatest) {

    assert(orderOfCallToExpire <= numOfGetRecordsCalls)

    private var expiredOnceAlready = false
    private var expiredIteratorRefreshed = false

    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
      if ((shardIterator.toInt == orderOfCallToExpire - 1) && !expiredOnceAlready) {
        // we fake only once the expired iterator exception at the specified get records
        // attempt order
        expiredOnceAlready = true
        throw ExpiredIteratorException.builder()
          .message("Artificial expired shard iterator")
          .build
      }
      else if (expiredOnceAlready && !expiredIteratorRefreshed) {
        // if we've thrown the expired iterator exception already, but the iterator was not
        // refreshed,
        // throw a hard exception to the test that is testing this Kinesis behaviour
        throw new RuntimeException("expired shard iterator was not refreshed on the next getRecords() call")
      }
      else {
        super.getKinesisRecords(shardIterator, limit)
      }
    }

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = if (!expiredOnceAlready) {
      // for the first call, just return the iterator of the first batch of records
      "0"
    }
    else {
      // fake the iterator refresh when this is called again after getRecords throws
      // expired iterator
      // exception on the orderOfCallToExpire attempt
      expiredIteratorRefreshed = true
      String.valueOf(orderOfCallToExpire - 1)
    }
  }

  object SingleShardEmittingAggregatedRecordsKinesis {
    def initShardItrToRecordBatch(numOfAggregatedRecords: Int,
                                          numOfChildRecords: Int,
                                          numOfGetRecordsCalls: Int): Map[String, Seq[Record]] = {
      val shardToRecordBatch = mutable.Map.empty[String, Seq[Record]]
      val sequenceNumber = new AtomicInteger
      (0 until numOfGetRecordsCalls).foreach {batch =>
        val recordBatch = createAggregatedRecordBatch(numOfAggregatedRecords, numOfChildRecords, sequenceNumber)
        shardToRecordBatch.put(batch.toString, recordBatch)
      }
      shardToRecordBatch.toMap
    }

    def createAggregatedRecordBatch(numOfAggregatedRecords: Int,
                                    numOfChildRecords: Int,
                                    sequenceNumber: AtomicInteger): Seq[Record] = {
      val recordBatch = mutable.ArrayBuffer.empty[Record]
      val recordAggregator = new RecordAggregator
      (0 until numOfAggregatedRecords).foreach { _ =>
        val partitionKey = UUID.randomUUID.toString
        (0 until numOfChildRecords).foreach {_ =>
          val data = RandomStringUtils.randomAlphabetic(1024).getBytes(StandardCharsets.UTF_8)
          try recordAggregator.addUserRecord(partitionKey, data)
          catch {
            case e: Exception =>
              throw new IllegalStateException("Error aggregating message", e)
          }
        }
        val aggRecord = recordAggregator.clearAndGet
        recordBatch += Record.builder()
                .data(SdkBytes.fromByteArray(aggRecord.toRecordBytes))
                .partitionKey(partitionKey)
                .approximateArrivalTimestamp(new Date(System.currentTimeMillis).toInstant)
                .sequenceNumber(sequenceNumber.getAndAdd(numOfChildRecords).toString)
                .build
      }
      recordBatch.toSeq
    }

  }

  private class SingleShardEmittingAggregatedRecordsKinesis(numOfAggregatedRecords: Int,
                                                            numOfChildRecords: Int,
                                                            numOfGetRecordsCalls: Int)
    extends SingleShardEmittingKinesis(
      SingleShardEmittingAggregatedRecordsKinesis.initShardItrToRecordBatch(numOfAggregatedRecords, numOfChildRecords, numOfGetRecordsCalls)
    ) {}

  abstract class SingleShardEmittingKinesis (shardItrToRecordBatch: Map[String, Seq[Record]],
                                             millisBehindLatest: Long) extends FakeKinesisClientConsumerAdapter {
    def this(shardItrToRecordBatch: Map[String, Seq[Record]]) {
      this(shardItrToRecordBatch, 0L)
    }

    override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
      val index = shardIterator.toInt
      // last next shard iterator is null
      val nextShardIterator = {
        if (index == shardItrToRecordBatch.size - 1) null
        else (index + 1).toString
      }
      // assuming that the maxRecordsToGet is always large enough
      GetRecordsResponse.builder()
        .records(shardItrToRecordBatch(shardIterator): _*)
        .nextShardIterator(nextShardIterator)
        .millisBehindLatest(millisBehindLatest)
        .build
    }

    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = {
      // Should be called only once. Simply return the iterator of the first batch of records
      "0"
    }

    override def getShards: Seq[Shard] = null
  }


  object BlockingQueueKinesis {
    def getShardIterator(streamName: String, shardId: String): String = s"${streamName}-${shardId}"
  }

  class BlockingQueueKinesis(streamName: String, shardQueues: Seq[BlockingQueue[String]]) extends FakeKinesisClientConsumerAdapter {

    private val listOfShards = mutable.ArrayBuffer.empty[StreamShard]
    private val shardIteratorToQueueMap = mutable.Map.empty[String, BlockingQueue[String]]

    private val shardCount = shardQueues.length
    if (shardCount == 0) {
    }
    else {
      for (i <- 0 until shardCount) {
        val streamShard = StreamShard(streamName,
          Shard.builder()
            .shardId(generateFromShardOrder(i))
            .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("0").build)
            .hashKeyRange(HashKeyRange.builder().startingHashKey("0").endingHashKey("0").build)
            .build)
        listOfShards += streamShard
        shardIteratorToQueueMap.put(BlockingQueueKinesis.getShardIterator(streamName, streamShard.shard.shardId), shardQueues(i))
      }

    }




    override def getShardIterator(shardId: String,
                                  iteratorType: String,
                                  iteratorPosition: String,
                                  failOnDataLoss: Boolean = true): String = {
      BlockingQueueKinesis.getShardIterator(streamName, shardId)
    }

  override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
    val queue: BlockingQueue[String] = shardIteratorToQueueMap(shardIterator)

    var nextShardIterator = shardIterator
    val records = mutable.ArrayBuffer.empty[Record]
    try {
      val data: String = queue.poll(100, TimeUnit.MILLISECONDS)
      if (data != null) {
        val record: Record = Record.builder()
          .data(SdkBytes.fromByteArray(data.getBytes(StandardCharsets.UTF_8)))
          .partitionKey(UUID.randomUUID.toString)
          .approximateArrivalTimestamp(new Date(System.currentTimeMillis).toInstant)
          .sequenceNumber("0")
          .build
        records += record
      }
    } catch {
      case _: InterruptedException =>
        nextShardIterator = null
    }

    GetRecordsResponse.builder()
      .records(records.toSeq: _*)
      .millisBehindLatest(0L)
      .nextShardIterator(nextShardIterator)
      .build
  }

    override def getShards: Seq[Shard] = {
      listOfShards.map { streamShards =>
        streamShards.shard
      }.toSeq
    }
  }
}
