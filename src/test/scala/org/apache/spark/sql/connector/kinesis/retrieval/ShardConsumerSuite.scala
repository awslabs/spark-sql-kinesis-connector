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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.COMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.RecordsPublisherRunStatus

class ShardConsumerSuite extends KinesisTestBase {

  private val streamShard = StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD)

  /** Delivers one empty batch with the given millisBehindLatest, then reports the shard complete. */
  private class OneEmptyBatchPublisher(millisBehindLatest: Long) extends RecordBatchPublisher {
    override def runProcessLoop(recordBatchConsumer: RecordBatchConsumer): RecordsPublisherRunStatus = {
      recordBatchConsumer.accept(RecordBatch(Seq.empty, streamShard, millisBehindLatest))
      COMPLETE
    }
    override def initialStartingPosition: KinesisPosition = DEFAULT_START_POSITION
    override def currentStartingPosition: KinesisPosition = DEFAULT_START_POSITION
    override def streamShard: StreamShard = ShardConsumerSuite.this.streamShard
  }

  private class RecordingReceiver extends DataReceiver {
    val records: ArrayBuffer[KinesisUserRecord] = ArrayBuffer.empty
    override def isRunning: Boolean = true
    override def stopWithError(t: Throwable): Unit = throw t
    override def updateState(streamShard: StreamShard, sequenceNumber: SequenceNumber): Unit = {}
    override def enqueueRecord(streamShard: StreamShard, record: KinesisUserRecord): Boolean = {
      records += record
      true
    }
  }

  private def run(millisBehindLatest: Long, signalCaughtUp: Boolean): Seq[KinesisUserRecord] = {
    val receiver = new RecordingReceiver
    new ShardConsumer(receiver, new OneEmptyBatchPublisher(millisBehindLatest), signalCaughtUp).run()
    receiver.records.toSeq
  }

  test("caught-up empty batch is signalled to the reader when signalCaughtUp is set") {
    val records = run(millisBehindLatest = 0, signalCaughtUp = true)

    records.size shouldBe 2
    KinesisUserRecord.emptyUserRecord(records.head) shouldBe true
    records.head.millisBehindLatest shouldBe 0L
    KinesisUserRecord.shardEndUserRecord(records(1)) shouldBe true
  }

  test("caught-up empty batch enqueues nothing by default") {
    val records = run(millisBehindLatest = 0, signalCaughtUp = false)

    records.size shouldBe 1
    KinesisUserRecord.shardEndUserRecord(records.head) shouldBe true
  }

  test("empty batch behind the tip is enqueued as before, with or without signalCaughtUp") {
    Seq(true, false).foreach { signalCaughtUp =>
      val records = run(millisBehindLatest = 1000, signalCaughtUp = signalCaughtUp)

      records.size shouldBe 2
      KinesisUserRecord.emptyUserRecord(records.head) shouldBe true
      records.head.millisBehindLatest shouldBe 1000L
      KinesisUserRecord.shardEndUserRecord(records(1)) shouldBe true
    }
  }
}
