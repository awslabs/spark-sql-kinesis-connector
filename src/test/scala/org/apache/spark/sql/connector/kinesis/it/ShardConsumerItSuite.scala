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


import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.SpanSugar.convertLongToGrainOfTime
import software.amazon.awssdk.services.kinesis.model.Shard
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisUtils
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.kinesis.retrieval.DataReceiver
import org.apache.spark.sql.connector.kinesis.retrieval.KinesisUserRecord
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisher
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherFactory
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber
import org.apache.spark.sql.connector.kinesis.retrieval.ShardConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard



abstract class ShardConsumerItSuite(aggregateTestData: Boolean, consumerType: String)
  extends KinesisIntegrationTestBase(consumerType, 1)
  with Matchers
  with Logging {

  val WAIT_TIME = 60000
  class FakeDataReceiver extends DataReceiver {

    var running: AtomicBoolean = new AtomicBoolean(true)
    var withError: Option[Throwable] = None
    var streamShard: StreamShard = _
    var sequenceNumber: SequenceNumber = _
    var recordCounter: Int = 0
    var hasShardEnd: AtomicBoolean = new AtomicBoolean(false)
    override def isRunning: Boolean = {
      running.get()
    }

    override def stopWithError(t: Throwable): Unit = {
      withError = Some(t)
    }

    override def updateState(streamShard: StreamShard, sequenceNumber: SequenceNumber): Unit = {
      this.streamShard = streamShard
      this.sequenceNumber = sequenceNumber
    }

    override def enqueueRecord(streamShard: StreamShard, record: KinesisUserRecord): Boolean = {
      if (KinesisUserRecord.shardEndUserRecord(record)) {
        hasShardEnd.set(true)
      } else if (KinesisUserRecord.nonEmptyUserRecord(record)) {
        recordCounter += 1
      }
      
      true
    }
  }


  test("ShardConsumer set shard end status") {
    val shardConsumersExecutor: ExecutorService = createThreadPool

    val dataReader = new FakeDataReceiver()

    val kinesisPosition = new TrimHorizon
    val kinesisStreamShard = StreamShard(testUtils.streamName, Shard.builder().shardId("shardId-000000000000").build())
    val kinesisOptions = defaultKinesisOptions

    val kinesisReader = KinesisClientFactory.createConsumer(
      kinesisOptions,
      kinesisOptions.streamName,
      kinesisOptions.endpointUrl
    )

    val consumerArn = registerShardConsumerIfEfo(kinesisReader)

    try {

      val recordBatchPublisher: RecordBatchPublisher = RecordBatchPublisherFactory.create(
        kinesisPosition,
        consumerArn,
        kinesisStreamShard,
        kinesisReader,
        kinesisOptions,
        dataReader.isRunning
      )

      val shardConsumer = new ShardConsumer(
        dataReader,
        recordBatchPublisher)

      val testData1 = 1 to 5
      testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)

      logInfo("Splitting Shards")
      val shardToSplit = testUtils.getShards().head
      testUtils.splitShard(shardToSplit.shardId())
      val (splitOpenShards, splitCloseShards) = testUtils.getShards().partition { shard =>
        shard.sequenceNumberRange().endingSequenceNumber() == null
      }
      logInfo(s"splitCloseShards ${splitCloseShards}, splitOpenShards ${splitOpenShards}")
      // We should have one closed shard and two open shards
      assert(splitCloseShards.size == 1)
      assert(splitOpenShards.size == 2)

      shardConsumersExecutor.submit(shardConsumer)

      eventually(timeout(WAIT_TIME.milliseconds)) {
        assert(dataReader.hasShardEnd.get)
      }

      dataReader.withError shouldBe None
      dataReader.recordCounter shouldBe testData1.length
      dataReader.running.set(false)
    } finally {
      KinesisUtils.deregisterStreamConsumer(
        kinesisReader,
        IT_CONSUMER_NAME
      )
    }

  }

  // run the whole test suite. This test uses shardId-000000000001 which has dependency on the split shard.
  test("When RecordBatchPublisher is cancelled but DataReader still running, DataReader is stopped with error") {
    val shardConsumersExecutor: ExecutorService = createThreadPool

    val dataReader = new FakeDataReceiver()

    val kinesisPosition = new Latest
    val kinesisStreamShard = StreamShard(testUtils.streamName, Shard.builder().shardId("shardId-000000000001").build())
    val kinesisOptions = defaultKinesisOptions

    val kinesisReader = KinesisClientFactory.createConsumer(
      kinesisOptions,
      kinesisOptions.streamName,
      kinesisOptions.endpointUrl
    )

    val consumerArn = registerShardConsumerIfEfo(kinesisReader)

    val isCancelled: AtomicBoolean = new AtomicBoolean(false)
    try {
      val recordBatchPublisher: RecordBatchPublisher = RecordBatchPublisherFactory.create(
        kinesisPosition,
        consumerArn,
        kinesisStreamShard,
        kinesisReader,
        kinesisOptions,
        !isCancelled.get() // runningSupplier can be different from shardConsumer's dataReader.isRunning
      )

      val shardConsumer = new ShardConsumer(
        dataReader,
        recordBatchPublisher)

      shardConsumersExecutor.submit(shardConsumer)

      val testData1 = 1 to 5
      testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)

      Thread.sleep(1000)
      isCancelled.set(true)

      eventually(timeout(WAIT_TIME.milliseconds)) {
        assert(dataReader.withError.isDefined
          && dataReader.withError.get.isInstanceOf[ShardConsumer.ShardConsumerCancelledException])
      }

      dataReader.hasShardEnd.get shouldBe false

      dataReader.running.set(false)

    } finally {
      KinesisUtils.deregisterStreamConsumer(
        kinesisReader,
        IT_CONSUMER_NAME
      )
    }

  }

  // run the whole test suite. This test uses shardId-000000000002 which has dependency on the split shard.
  test("ShardConsumer is interrupted") {
    val shardConsumersExecutor: ExecutorService = createThreadPool

    val dataReader = new FakeDataReceiver()

    val kinesisPosition = new Latest
    val kinesisStreamShard = StreamShard(testUtils.streamName, Shard.builder().shardId("shardId-000000000002").build())
    val kinesisOptions = defaultKinesisOptions

    val kinesisReader = KinesisClientFactory.createConsumer(
      kinesisOptions,
      kinesisOptions.streamName,
      kinesisOptions.endpointUrl
    )

    val consumerArn = registerShardConsumerIfEfo(kinesisReader)

    try {
      val recordBatchPublisher: RecordBatchPublisher = RecordBatchPublisherFactory.create(
        kinesisPosition,
        consumerArn,
        kinesisStreamShard,
        kinesisReader,
        kinesisOptions,
        dataReader.isRunning
      )

      val shardConsumer = new ShardConsumer(
        dataReader,
        recordBatchPublisher)

      shardConsumersExecutor.submit(shardConsumer)

      val testData1 = 1 to 5
      testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData, Some("1"))

      Thread.sleep(1000)

      shardConsumersExecutor.shutdownNow()

      eventually(timeout(WAIT_TIME.milliseconds)) {
        assert(dataReader.withError.isDefined && dataReader.withError.get.isInstanceOf[ShardConsumer.ShardConsumerInterruptedException])
      }

      dataReader.hasShardEnd.get shouldBe false
      dataReader.running.set(false)
    } finally {
      KinesisUtils.deregisterStreamConsumer(
        kinesisReader,
        IT_CONSUMER_NAME
      )
    }

  }

  private def createThreadPool: ExecutorService = {
    Executors.newCachedThreadPool()
  }

  private def registerShardConsumerIfEfo(kinesisReader: KinesisClientConsumer): String = {
    if (consumerType == EFO_CONSUMER_TYPE) {
      KinesisUtils.registerStreamConsumer(
        kinesisReader,
        IT_CONSUMER_NAME
      )
    } else ""
  }
}

@IntegrationTestSuite
class NoAggEfoShardConsumerItSuite
  extends ShardConsumerItSuite(aggregateTestData = false, consumerType = EFO_CONSUMER_TYPE)
@IntegrationTestSuite
class AggEfoShardConsumerItSuite
  extends ShardConsumerItSuite(aggregateTestData = true, consumerType = EFO_CONSUMER_TYPE)
@IntegrationTestSuite
class NoAggPollingShardConsumerItSuite
  extends ShardConsumerItSuite(aggregateTestData = false, consumerType = POLLING_CONSUMER_TYPE)
@IntegrationTestSuite
class AggPollingShardConsumerItSuite
  extends ShardConsumerItSuite(aggregateTestData = true, consumerType = POLLING_CONSUMER_TYPE)
