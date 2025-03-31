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

import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.apache.spark.sql.connector.kinesis.AfterSequenceNumber
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TestConsumer
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.COMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.INCOMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.client.FakePollingClientConsumerFactory._
import org.apache.spark.sql.connector.kinesis.retrieval.polling.PollingRecordBatchPublisher

class PollingRecordBatchPublisherSuite extends KinesisTestBase {


  test("polling record batch publisher successfully read non-aggregated records") {
    val kinesisClient = totalNumOfRecordsAfterNumOfGetRecordsCalls(5, 1, 100)
    val publisher = createPollingRecordPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    val startTime = System.nanoTime
    publisher.runProcessLoop(consumer)
    val endTime = System.nanoTime

    consumer.getRecordBatches.size shouldBe 1
    consumer.getRecordBatches.get(0).userRecords.size shouldBe 5
    consumer.getRecordBatches.get(0).millisBehindLatest shouldBe 100L

    // fetch interval is respected
    (endTime - startTime) should be >= DEFAULT_KINESIS_OPTIONS.pollingFetchIntervalMs * 1000000L
  }

  test("polling record batch publisher successfully read aggregated records") {
    val kinesisClient = aggregatedRecords(5, 5, 1)
    val publisher = createPollingRecordPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    publisher.runProcessLoop(consumer)

    consumer.getRecordBatches.size shouldBe 1
    consumer.getRecordBatches.get(0).userRecords.size shouldBe 25
    consumer.getRecordBatches.get(0).millisBehindLatest shouldBe 0L
  }

  test("return COMPLETE when shard end") {
    val totalRecords = 5
    val kinesisClient = totalNumOfRecordsAfterNumOfGetRecordsCalls(totalRecords, 2, 100)
    val publisher = createPollingRecordPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)

    publisher.runProcessLoop(consumer) shouldBe INCOMPLETE

    publisher.runProcessLoop(consumer) shouldBe COMPLETE

    // second run on completed shard returns COMPLETE
    publisher.runProcessLoop(consumer) shouldBe COMPLETE

    consumer.getRecordBatches.size shouldBe 2
    consumer.getRecordBatches.get(0).userRecords.size + consumer.getRecordBatches.get(1).userRecords.size shouldBe 5
    consumer.getRecordBatches.get(0).millisBehindLatest shouldBe 100L
    consumer.getRecordBatches.get(1).millisBehindLatest shouldBe 100L

    publisher.nextStartingPosition shouldBe AfterSequenceNumber(
      s"${totalRecords-1}",
      NO_SUB_SEQUENCE_NUMBER,
      last = true
    )
  }

  test("return COMPLETE when no shard") {
    val totalRecords = 5
    val kinesisClient = noShardsFoundForRequestedStreams
    val publisher = createPollingRecordPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)

    publisher.runProcessLoop(consumer) shouldBe COMPLETE

    consumer.getRecordBatches.size shouldBe 0
  }

  test("recover from ExpiredIteratorException") {
    val kinesisClient = spy[KinesisClientConsumer](
      totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(3, 2, 1, 500)
    )
    val publisher = createPollingRecordPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)

    val startTime = System.nanoTime
    publisher.runProcessLoop(consumer) shouldBe INCOMPLETE
    val endTime = System.nanoTime

    // Get shard iterator is called twice, once during first run, secondly to refresh expired
    // iterator
    verify(kinesisClient, times(2)).getShardIterator("", "", "")

    consumer.getRecordBatches.size shouldBe 1
    consumer.getRecordBatches.get(0).userRecords.size shouldBe 2
    consumer.getRecordBatches.get(0).millisBehindLatest shouldBe 500L

    // fetch interval is respected
    (endTime - startTime) should be >= DEFAULT_KINESIS_OPTIONS.pollingFetchIntervalMs * 1000000L
  }

  private def createPollingRecordPublisher(kinesisClient: KinesisClientConsumer, startingPosition: KinesisPosition) = {
    new PollingRecordBatchPublisher(startingPosition,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true)
  }

}
