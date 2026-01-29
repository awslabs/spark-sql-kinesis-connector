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

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import io.netty.handler.timeout.ReadTimeoutException
import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.exception.SdkInterruptedException
import software.amazon.awssdk.services.kinesis.model.HashKeyRange
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent

import org.apache.spark.sql.connector.kinesis.AfterSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtTimeStamp
import org.apache.spark.sql.connector.kinesis.FullJitterBackoffManager
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TestConsumer
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.agg.RecordAggregator
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.CANCELLED
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.INCOMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.client.FakeEfoClientConsumerFactory._
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRecordBatchPublisher

class EfoRecordBatchPublisherSuite extends KinesisTestBase {


  test("EFO record batch publisher successfully read non-aggregated records") {
    val kinesisClient = boundedShard
      .withBatchCount(10)
      .withBatchesPerSubscription(3)
      .withRecordsPerBatch(12)
      .build

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    var cnt = 0
    breakable {
      while (publisher.runProcessLoop(consumer) == INCOMPLETE) {
        cnt += 1
        if (cnt > 4) break
      }
    }

    val userRecords = flattenToUserRecords(consumer.getRecordBatches)

    // Should have received 10 * 12 = 120 records
    userRecords.length shouldBe 120

    var expectedSequenceNumber = 1
    userRecords.foreach { r =>
      r.sequenceNumber.sequenceNumber shouldBe expectedSequenceNumber.toString
      r.sequenceNumber.subSequenceNumber shouldBe NO_SUB_SEQUENCE_NUMBER
      r.sequenceNumber.isLast shouldBe true
      expectedSequenceNumber += 1
    }
  }

  test("EFO record batch publisher successfully read aggregated records") {
    val kinesisClient = boundedShard
      .withBatchCount(10)
      .withBatchesPerSubscription(5)
      .withAggregationFactor(5)
      .withRecordsPerBatch(12)
      .build

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    var cnt = 0
    breakable {
      while (publisher.runProcessLoop(consumer) == INCOMPLETE) {
        cnt += 1
        if (cnt > 5) break
      }
    }

    val userRecords = flattenToUserRecords(consumer.getRecordBatches)

    // Should have received 10 * 12 * 5 = 600 records
    userRecords.length shouldBe 600

    var expectedSequenceNumber = 1
    var expectedSubsequenceNumber = 0
    userRecords.foreach { r =>
      r.sequenceNumber.sequenceNumber shouldBe expectedSequenceNumber.toString
      r.sequenceNumber.subSequenceNumber shouldBe expectedSubsequenceNumber
      expectedSubsequenceNumber += 1
      if (expectedSubsequenceNumber == 5) {
        r.sequenceNumber.isLast shouldBe true
        expectedSequenceNumber += 1
        expectedSubsequenceNumber = 0
      } else {
        r.sequenceNumber.isLast shouldBe false
      }
    }
  }

  test("Non-aggregated Starting Position After Sequence Number") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AfterSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AfterSequenceNumber.iteratorType
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Aggregated Starting Position After Sequence Number isLast=true") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AfterSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        DEFAULT_SUB_SEQUENCE,
        isLast = true
      )
    )


    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AfterSequenceNumber.iteratorType
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }



  test("Aggregated Starting Position After Sequence Number isLast=false") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AfterSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        DEFAULT_SUB_SEQUENCE,
        isLast = false
      )
    )

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE

    // If it is not the last subsequence, reread the same Sequence number to avoid missing data
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtSequenceNumber.iteratorType
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Non-aggregated Starting Position At Sequence Number") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AtSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )


    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtSequenceNumber.iteratorType
    publisher.nextStartingPosition.iteratorType shouldBe AfterSequenceNumber.iteratorType
  }

  test("Aggregated Starting Position At Sequence Number isLast=true") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AtSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        DEFAULT_SUB_SEQUENCE,
        isLast = true
      )
    )

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtSequenceNumber.iteratorType
    publisher.nextStartingPosition.iteratorType shouldBe AfterSequenceNumber.iteratorType
  }


  test("Aggregated Starting Position At Sequence Number isLast=false") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AfterSequenceNumber.iteratorType,
        DEFAULT_SEQUENCE,
        DEFAULT_SUB_SEQUENCE,
        isLast = false
      )
    )

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe DEFAULT_SEQUENCE
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtSequenceNumber.iteratorType
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Starting Position AtTimestamp") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(AtTimeStamp.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )


    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).timestamp.toEpochMilli shouldBe DEFAULT_TIMESTAMP.toLong
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtTimeStamp.iteratorType
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Starting Position Latest") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )


    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe Latest.iteratorType
    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe null
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Starting Position TrimHorizon") {
    val kinesisClient = emptyShard

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(TrimHorizon.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )


    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe TrimHorizon.iteratorType
    kinesisClient.getStartingPositionForSubscription(0).sequenceNumber shouldBe null
    publisher.nextStartingPosition shouldBe publisher.initialStartingPosition
  }

  test("Starting Position AtTimestamp with aggregated record") {
    // Create Aggregate Record with explicit hash keys 0 and 1
    val recordAggregator: RecordAggregator = new RecordAggregator
    recordAggregator.addUserRecord("pk", "0", randomAlphabetic(32).getBytes(UTF_8))
    recordAggregator.addUserRecord("pk", "1", randomAlphabetic(32).getBytes(UTF_8))

    val record: Record = Record.builder
      .approximateArrivalTimestamp(Instant.now)
      .data(SdkBytes.fromByteArray(recordAggregator.clearAndGet.toRecordBytes))
      .sequenceNumber("1")
      .partitionKey("pk")
      .build

    // Create 2 Subscription events for asserting that requested sequence number stays the same
    val events = new mutable.ArrayBuffer[SubscribeToShardEvent]
    events += createSubscribeToShardEvent(record)
    events += createSubscribeToShardEvent(record)

    val kinesisClient = singleShardWithEvents(events.toSeq)
    val now: Date = new Date

    val hashKeyRange: HashKeyRange = HashKeyRange.builder()
      .startingHashKey("0")
      .endingHashKey("100")
      .build

    val publisher = new EfoRecordBatchPublisher(
      KinesisPosition.make(AtTimeStamp.iteratorType,
        now.toInstant.toEpochMilli.toString,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      ),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME,
        Shard.builder()
          .shardId("shardId-000000000000")
          .hashKeyRange(hashKeyRange)
          .build),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true)


    val testConsumer = new TestConsumer(AtTimeStamp(100000))

    publisher.runProcessLoop(testConsumer)

    kinesisClient.getStartingPositionForSubscription(0).timestamp shouldBe now.toInstant
    kinesisClient.getStartingPositionForSubscription(0).`type`.toString shouldBe AtTimeStamp.iteratorType
    publisher.nextStartingPosition.iteratorType shouldBe AfterSequenceNumber.iteratorType
    publisher.nextStartingPosition.iteratorPosition shouldBe "1"
    publisher.nextStartingPosition.subSequenceNumber shouldBe 1
    publisher.nextStartingPosition.isLast shouldBe true
    testConsumer.getRecordBatches.size() shouldBe 1

  }

  test("Starting Position AtTimestamp with dropped aggregated record") {
    // Create Aggregate Record with explicit hash keys 0 and 1
    val recordAggregator: RecordAggregator = new RecordAggregator
    recordAggregator.addUserRecord("pk", "0", randomAlphabetic(32).getBytes(UTF_8))
    recordAggregator.addUserRecord("pk", "1", randomAlphabetic(32).getBytes(UTF_8))

    val record: Record = Record.builder
      .approximateArrivalTimestamp(Instant.now)
      .data(SdkBytes.fromByteArray(recordAggregator.clearAndGet.toRecordBytes))
      .sequenceNumber("1")
      .partitionKey("pk")
      .build

    val events = new mutable.ArrayBuffer[SubscribeToShardEvent]
    events += createSubscribeToShardEvent(record)
    events += createSubscribeToShardEvent(record)

    val kinesisClient = singleShardWithEvents(events.toSeq)
    val now: Date = new Date

    // Create ShardHandle with HashKeyRange excluding single UserRecord with hash key 0
    val hashKeyRange: HashKeyRange = HashKeyRange.builder()
                                          .startingHashKey("1")
                                          .endingHashKey("100")
                                          .build

    val publisher = new EfoRecordBatchPublisher(
      KinesisPosition.make(AtTimeStamp.iteratorType,
        now.toInstant.toEpochMilli.toString,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      ),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME,
        Shard.builder()
          .shardId("shardId-000000000000")
          .hashKeyRange(hashKeyRange)
          .build),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true)

    publisher.runProcessLoop(new TestConsumer(AtTimeStamp(100000)))

    // Run a second time to ensure the StartingPosition is still respected, indicating we are
    // still using the original AT_TIMESTAMP behaviour
    publisher.runProcessLoop(new TestConsumer(AtTimeStamp(100000)))

    kinesisClient.getStartingPositionForSubscription(1).timestamp shouldBe now.toInstant
    kinesisClient.getStartingPositionForSubscription(1).`type`.toString shouldBe AtTimeStamp.iteratorType
    publisher.nextStartingPosition.iteratorType shouldBe AtTimeStamp.iteratorType
    publisher.nextStartingPosition.iteratorPosition shouldBe now.toInstant.toEpochMilli.toString

  }

  test("Exception from consumer") {
    val kinesisClient = boundedShard.build

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val exception = intercept[RuntimeException] {
      publisher.runProcessLoop((_: RecordBatch) => {
        throw new RuntimeException("An error thrown from the consumer")
      })
    }

    exception.getMessage should include ("An error thrown from the consumer")
  }


  test("ResourceNotFoundException when obtaining subscription") {
    val kinesisClient = resourceNotFoundWhenObtainingSubscription

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    intercept[ResourceNotFoundException] {
      publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))
    }
  }

  test("ResourceNotFoundException from subscription") {
    val exception = ResourceNotFoundException.builder.build
    val kinesisClient = errorDuringSubscription(exception)

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val state = publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    state shouldBe RecordBatchPublisherRunStatus.INCOMPLETE
    kinesisClient.getNumberOfSubscribeToShardInvocations shouldBe 1
  }

  test("LimitExceededException from subscription") {
    val exception = LimitExceededException.builder.build
    val kinesisClient = errorDuringSubscription(exception)

    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    val testConsumer = new TestConsumer(publisher.initialStartingPosition)
    val state = publisher.runProcessLoop(testConsumer)

    state shouldBe RecordBatchPublisherRunStatus.INCOMPLETE
    kinesisClient.getNumberOfSubscribeToShardInvocations shouldBe 1
    testConsumer.getRecordBatches.size() shouldBe SubscriptionErrorEfoClient.NUMBER_OF_EVENTS_PER_SUBSCRIPTION
  }

  test("Retryable error when SubscribeToShard") {
    val exception = LimitExceededException.builder.build
    val kinesisClient = errorDuringSubscription(exception)

    val backoff = mock(classOf[FullJitterBackoffManager])
    when(backoff.calculateFullJitterBackoff(anyInt)).thenReturn(100L)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true,
      Some(backoff))

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    verify(backoff).calculateFullJitterBackoff(1)

    verify(backoff).sleep(100L)
  }

  test("RuntimeException is retryable error when SubscribeToShard") {
    val exception = new RuntimeException("runtime error")
    val kinesisClient = errorDuringSubscription(exception)

    val backoff = mock(classOf[FullJitterBackoffManager])
    when(backoff.calculateFullJitterBackoff(anyInt)).thenReturn(100L)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true,
      Some(backoff))

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition))

    verify(backoff).calculateFullJitterBackoff(1)

    verify(backoff).sleep(100L)
  }

  test("CANCELLED state returned when interrupted") {

    val kinesisClient = errorDuringSubscription(new SdkInterruptedException(null))


    val publisher = createRecordBatchPublisher(kinesisClient,
      KinesisPosition.make(Latest.iteratorType,
        DEFAULT_TIMESTAMP,
        NO_SUB_SEQUENCE_NUMBER,
        isLast = true
      )
    )

    publisher.runProcessLoop(new TestConsumer(publisher.initialStartingPosition)) shouldBe CANCELLED
  }

  test("Max retries exceeded when SubscribeToShard") {

    val maxRetries = 3

    val exceptionResult = intercept[RuntimeException] {
      val exception = LimitExceededException.builder.build
      val kinesisClient = errorDuringSubscription(exception)

      val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        kinesisClient,
        DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = maxRetries),
        true)

      var cnt = 0
      breakable {
        while (publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition)) == INCOMPLETE) {
          cnt += 1
          if (cnt > maxRetries) break
        }
      }
    }

    exceptionResult.getMessage should include(s"Maximum retries exceeded for SubscribeToShard: failed ${maxRetries} times.")

  }

  test("Recoverable error max retries exceeded") {

    val maxRetries = 3
    val maxRecoverableRetries = maxRetries * EfoRecordBatchPublisher.RECOVERABLE_RETRY_MULTIPLIER
    var cnt = 0

    val exceptionResult = intercept[RuntimeException] {
      val exception = ReadTimeoutException.INSTANCE
      val kinesisClient = new SubscriptionErrorEfoClient(maxRecoverableRetries + 1, exception)

      val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        kinesisClient,
        DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = maxRetries),
        true)
      
      breakable {
        while (publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition)) == INCOMPLETE) {
          cnt += 1
          if (cnt > maxRecoverableRetries) break
        }
      }
    }

    cnt shouldBe maxRecoverableRetries
    exceptionResult.getMessage should include(s"Maximum retries exceeded for SubscribeToShard: failed ${maxRecoverableRetries} times.")

  }
  
  test("Recoverable error retry more times") {

    val maxRetries = 3
    var cnt = 0
    
    val exception = ReadTimeoutException.INSTANCE
    val kinesisClient = errorDuringSubscription(exception)

    val backoff = mock(classOf[FullJitterBackoffManager])
    when(backoff.calculateFullJitterBackoff(anyInt)).thenReturn(100L)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = maxRetries),
      true,
      Some(backoff))
    
    breakable {
      while (publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition)) == INCOMPLETE) {
        cnt += 1
      }
    }

    cnt shouldBe SubscriptionErrorEfoClient.NUMBER_OF_SUBSCRIPTIONS
    // No exception is thrown, but still do backoff retry
    verify(backoff, times(SubscriptionErrorEfoClient.NUMBER_OF_SUBSCRIPTIONS)).calculateFullJitterBackoff(anyInt)

  }

  test("backoff attempt increases") {

    val maxRetries = 3

    val exception = LimitExceededException.builder.build
    val kinesisClient = errorDuringSubscription(exception)

    val backoff = mock(classOf[FullJitterBackoffManager])
    when(backoff.calculateFullJitterBackoff(anyInt)).thenReturn(100L)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = 10),
      true,
      Some(backoff))

    var cnt = 0
    breakable {
      while (publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition)) == INCOMPLETE) {
        cnt += 1
        if (cnt > maxRetries) break
      }
    }

    verify(backoff, never).calculateFullJitterBackoff(ArgumentMatchers.eq(0))
    verify(backoff).calculateFullJitterBackoff(ArgumentMatchers.eq(1))
    verify(backoff).calculateFullJitterBackoff(ArgumentMatchers.eq(2))
    verify(backoff).calculateFullJitterBackoff(ArgumentMatchers.eq(3))
    verify(backoff).calculateFullJitterBackoff(ArgumentMatchers.eq(4))
    verify(backoff, never).calculateFullJitterBackoff(ArgumentMatchers.eq(5))

  }


  test("backoff attempt reset after successful subscription") {

    val kinesisClient = alternatingSuccessErrorDuringSubscription

    val backoff = mock(classOf[FullJitterBackoffManager])
    when(backoff.calculateFullJitterBackoff(anyInt)).thenReturn(100L)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = 10),
      true,
      Some(backoff))

    publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition))
    publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition))
    publisher.runProcessLoop(new TestConsumer(publisher.nextStartingPosition))

    // Expecting:
    // - first attempt to fail, and backoff attempt #1
    // - second attempt to succeed, and reset attempt index
    // - third attempt to fail, and backoff attempt #1
    verify(backoff, times(2)).calculateFullJitterBackoff(ArgumentMatchers.eq(1))

    verify(backoff, never).calculateFullJitterBackoff(ArgumentMatchers.eq(0))
    verify(backoff, never).calculateFullJitterBackoff(ArgumentMatchers.eq(2))
  }


  test("gracefully close publisher") {

    val kinesisClient = boundedShard
      .withBatchCount(10)
      .withBatchesPerSubscription(3)
      .withRecordsPerBatch(12)
      .build

    val run = new AtomicBoolean(true)

    val publisher = new EfoRecordBatchPublisher(AtTimeStamp(100000),
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS.copy(efoSubscribeToShardMaxRetries = 10),
      run.get)

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    var count = 0
    var state = publisher.runProcessLoop(consumer)
    while ( state == INCOMPLETE) {
      run.set(false)
      count += 1
      Thread.sleep(100)
      state = publisher.runProcessLoop(consumer)
    }

    state shouldBe CANCELLED
    count shouldBe 1
  }

  private def flattenToUserRecords(recordBatch: util.List[RecordBatch]): Seq[KinesisUserRecord] = {
    recordBatch.asScala.flatMap(_.userRecords).toSeq
  }



  private def createSubscribeToShardEvent(records: Record*) = {
    SubscribeToShardEvent.builder
      .records(records: _*)
      .build
  }

  private def createRecordBatchPublisher(kinesisClient: KinesisClientConsumer, startingPosition: KinesisPosition) = {

    new EfoRecordBatchPublisher(startingPosition,
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      kinesisClient,
      DEFAULT_KINESIS_OPTIONS,
      true)
  }
}
