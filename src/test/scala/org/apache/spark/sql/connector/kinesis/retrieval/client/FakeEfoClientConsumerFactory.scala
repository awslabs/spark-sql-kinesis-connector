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

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Consumer
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus.ACTIVE
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus.CREATING
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus.DELETING
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StartingPosition
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler

import org.apache.spark.sql.connector.kinesis.agg.RecordAggregator

object FakeEfoClientConsumerFactory {
  val STREAM_ARN = "stream-arn"
  val STREAM_CONSUMER_ARN_EXISTING = "stream-consumer-arn"
  val STREAM_CONSUMER_ARN_NEW = "stream-consumer-arn-new"

  def boundedShard: SingleShardEfoClient.Builder = new SingleShardEfoClient.Builder

  def singletonShard(event: SubscribeToShardEvent): SingletonEfoClient = new SingletonEfoClient(event)

  def singleShardWithEvents(events: Seq[SubscribeToShardEvent]): EventEfoClient = new EventEfoClient(events)

  def emptyShard: SingleShardEfoClient = new SingleShardEfoClient.Builder().withBatchCount(0).build

  def resourceNotFoundWhenObtainingSubscription: ExceptionalEfoClient = {
    new ExceptionalEfoClient(ResourceNotFoundException.builder.build)
  }

  def errorDuringSubscription(throwables: Throwable*): SubscriptionErrorEfoClient = {
    new SubscriptionErrorEfoClient(SubscriptionErrorEfoClient.NUMBER_OF_SUBSCRIPTIONS, throwables: _*)
  }

  def alternatingSuccessErrorDuringSubscription: AlternatingSubscriptionErrorEfoClient = {
    new AlternatingSubscriptionErrorEfoClient(LimitExceededException.builder.build)
  }

  def failsToAcquireSubscription: FailsToAcquireSubscriptionKinesisAsync = new FailsToAcquireSubscriptionKinesisAsync

  def shardThatCreatesBackpressureOnQueue: MultipleEventsForSingleRequest = new MultipleEventsForSingleRequest

  def streamNotFound: StreamConsumerFakeKinesisSync = {
    new StreamConsumerFakeKinesisSync.Builder().withThrowsWhileDescribingStream(ResourceNotFoundException.builder.build).build
  }

  def streamConsumerNotFound: StreamConsumerFakeKinesisSync = {
    new StreamConsumerFakeKinesisSync.Builder().withStreamConsumerNotFound(true).build
  }

  def existingActiveConsumer: StreamConsumerFakeKinesisSync = {
    new StreamConsumerFakeKinesisSync.Builder().build
  }

  def registerExistingConsumerAndWaitToBecomeActive: StreamConsumerFakeKinesisSync = {
    new StreamConsumerFakeKinesisSync.Builder().withStreamConsumerStatus(CREATING).build
  }

  /** A dummy EFO implementation that fails to acquire subscription (no response). */
  class FailsToAcquireSubscriptionKinesisAsync extends FakeKinesisClientConsumerAdapter {
    override def subscribeToShard(request: SubscribeToShardRequest,
              responseHandler: SubscribeToShardResponseHandler): CompletableFuture[Void] = {
      CompletableFuture.supplyAsync(() => null)
    }
  }

  def emptyBatchFollowedBySingleRecord: AbstractSingleShardEfoClient =
    new AbstractSingleShardEfoClient(2) {
      private var subscriptionCount = 0

      override def getEventsToSend: Seq[SubscribeToShardEvent] = {
        val builder = SubscribeToShardEvent.builder.continuationSequenceNumber(if (subscriptionCount == 0) "1"
        else null)
        if (subscriptionCount == 1) builder.records(createRecord(new AtomicInteger(1)))
        subscriptionCount += 1
        Seq(builder.build)
      }
  }

  /**
   * An unbounded fake Kinesis that offers subscriptions with 5 records, alternating throwing the
   * given exception. The first subscription is exceptional, second successful, and so on.
   */
  class AlternatingSubscriptionErrorEfoClient (throwable: Throwable)
    extends SubscriptionErrorEfoClient(SubscriptionErrorEfoClient.NUMBER_OF_SUBSCRIPTIONS, throwable) {
    var index = 0

    override def completeSubscription(subscriber: Subscriber[_ >: SubscribeToShardEventStream]): Unit = {
      if ( {
        index += 1; index - 1
      } % 2 == 0) {
        // Fail the subscription
        super.completeSubscription(subscriber)
      }
      else {
        // Do not fail the subscription
        subscriber.onComplete()
      }
    }
  }

  /**
   * A fake Kinesis that throws the given exception after sending 5 records. A total of 5
   * subscriptions can be acquired.
   */
  object SubscriptionErrorEfoClient {
    val NUMBER_OF_SUBSCRIPTIONS = 5
    val NUMBER_OF_EVENTS_PER_SUBSCRIPTION = 5
  }

  class SubscriptionErrorEfoClient (numberOfSubscriptions: Int,
                                   val throwables: Throwable*)
    extends AbstractSingleShardEfoClient(numberOfSubscriptions) {
    val sequenceNumber = new AtomicInteger

    override def getEventsToSend: Seq[SubscribeToShardEvent] = {
      generateEvents(SubscriptionErrorEfoClient.NUMBER_OF_EVENTS_PER_SUBSCRIPTION, sequenceNumber)
    }

    override def completeSubscription(subscriber: Subscriber[_ >: SubscribeToShardEventStream]): Unit = {

      // Add an artificial delay to allow records to flush
      Thread.sleep(200)
      for (throwable <- throwables) {
        subscriber.onError(throwable)
      }
    }
  }

  class ExceptionalEfoClient (private val exception: RuntimeException) extends FakeKinesisClientConsumerAdapter {
    override def subscribeToShard(request: SubscribeToShardRequest, responseHandler: SubscribeToShardResponseHandler): CompletableFuture[Void] = {
      responseHandler.exceptionOccurred(exception)
      CompletableFuture.completedFuture(null)
    }
  }

  class SingletonEfoClient (private val event: SubscribeToShardEvent)
    extends AbstractSingleShardEfoClient(1) {
    override def getEventsToSend: Seq[SubscribeToShardEvent] = Seq(event)
  }

  class EventEfoClient (private val events: Seq[SubscribeToShardEvent]) extends AbstractSingleShardEfoClient(1) {
    override def getEventsToSend: Seq[SubscribeToShardEvent] = events
  }

  class MultipleEventsForSingleRequest extends AbstractSingleShardEfoClient(1) {
    override def getEventsToSend: Seq[SubscribeToShardEvent] = generateEvents(2, new AtomicInteger(1))

    override def completeSubscription(subscriber: Subscriber[_ >: SubscribeToShardEventStream]): Unit = {
      generateEvents(3, new AtomicInteger(2)).foreach { e =>
        subscriber.onNext(e)
      }
      super.completeSubscription(subscriber)
    }
  }

  /**
   * A fake implementation of KinesisProxyV2 SubscribeToShard that provides dummy records for EFO
   * subscriptions. Aggregated and non-aggregated records are supported with various batch and
   * subscription sizes.
   */
  object SingleShardEfoClient {
    class Builder {
      var batchesPerSubscription = 100000
      var recordsPerBatch = 10
      var millisBehindLatest: Long = 0L
      var batchCount = 1
      var aggregationFactor = 1

      def getSubscriptionCount: Int = Math.ceil(getTotalRecords.toDouble / batchesPerSubscription / recordsPerBatch).toInt

      def getTotalRecords: Int = batchCount * recordsPerBatch

      def withBatchesPerSubscription(batchesPerSubscription: Int): SingleShardEfoClient.Builder = {
        this.batchesPerSubscription = batchesPerSubscription
        this
      }

      def withRecordsPerBatch(recordsPerBatch: Int): SingleShardEfoClient.Builder = {
        this.recordsPerBatch = recordsPerBatch
        this
      }

      def withBatchCount(batchCount: Int): SingleShardEfoClient.Builder = {
        this.batchCount = batchCount
        this
      }

      def withMillisBehindLatest(millisBehindLatest: Long): SingleShardEfoClient.Builder = {
        this.millisBehindLatest = millisBehindLatest
        this
      }

      def withAggregationFactor(aggregationFactor: Int): SingleShardEfoClient.Builder = {
        this.aggregationFactor = aggregationFactor
        this
      }

      def build: SingleShardEfoClient = new SingleShardEfoClient(this)
    }
  }

  class SingleShardEfoClient (builder: SingleShardEfoClient.Builder)
    extends AbstractSingleShardEfoClient(builder.getSubscriptionCount) {

    private val batchesPerSubscription = builder.batchesPerSubscription
    private val recordsPerBatch = builder.recordsPerBatch
    private val millisBehindLatest = builder.millisBehindLatest
    private val totalRecords = builder.getTotalRecords
    private val aggregationFactor = builder.aggregationFactor
    private val sequenceNumber = new AtomicInteger(0)

    override def getEventsToSend: Seq[SubscribeToShardEvent] = {
      val events = ArrayBuffer.empty[SubscribeToShardEvent]
      val eventBuilder = SubscribeToShardEvent.builder.millisBehindLatest(millisBehindLatest)
      var batchIndex = 0
      while (batchIndex < batchesPerSubscription && sequenceNumber.get < totalRecords) {
        val records = new util.ArrayList[Record]
        for (i <- 0 until recordsPerBatch) {
          var record: Record = null
          if (aggregationFactor == 1) record = createRecord(sequenceNumber)
          else record = createAggregatedRecord(aggregationFactor, sequenceNumber)
          records.add(record)
        }
        eventBuilder.records(records)
        val continuation = if (sequenceNumber.get < totalRecords) String.valueOf(sequenceNumber.get + 1)
        else null
        eventBuilder.continuationSequenceNumber(continuation)
        events += eventBuilder.build

        batchIndex += 1
      }
      events.toSeq
    }
  }

  /**
   * A single shard dummy EFO implementation that provides basic responses and subscription
   * management. Does not provide any records.
   */
  abstract class AbstractSingleShardEfoClient (var remainingSubscriptions: Int)
    extends FakeKinesisClientConsumerAdapter {

    final private val requests = new util.ArrayList[SubscribeToShardRequest]

    def getNumberOfSubscribeToShardInvocations: Int = requests.size

    def getStartingPositionForSubscription(subscriptionIndex: Int): StartingPosition = {
      assert(subscriptionIndex >= 0)
      assert(subscriptionIndex < getNumberOfSubscribeToShardInvocations)
      requests.get(subscriptionIndex).startingPosition
    }

    override def subscribeToShard(request: SubscribeToShardRequest, responseHandler: SubscribeToShardResponseHandler): CompletableFuture[Void] = {
      requests.add(request)
      CompletableFuture.supplyAsync(() => {
        responseHandler.responseReceived(SubscribeToShardResponse.builder.build)
        responseHandler.onEventStream((subscriber: Subscriber[_ >: SubscribeToShardEventStream]) => {
          var eventsToSend: Seq[SubscribeToShardEvent] = null
          if (remainingSubscriptions > 0) {
            eventsToSend = getEventsToSend
            remainingSubscriptions -= 1
          }
          else eventsToSend = Seq(SubscribeToShardEvent.builder.millisBehindLatest(0L).continuationSequenceNumber(null).build)
          val subscription = mock(classOf[Subscription])
          val iterator = eventsToSend.iterator
          doAnswer((a: InvocationOnMock) => {
            if (!iterator.hasNext) completeSubscription(subscriber)
            else subscriber.onNext(iterator.next)
            null

          }).when(subscription).request(anyLong)
          subscriber.onSubscribe(subscription)

        })
        null

      })
    }

    def completeSubscription(subscriber: Subscriber[_ >: SubscribeToShardEventStream]): Unit = {
      subscriber.onComplete()
    }

    def getEventsToSend: Seq[SubscribeToShardEvent]
  }

  object StreamConsumerFakeKinesisSync {
    val NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE = 5
    val NUMBER_OF_DESCRIBE_REQUESTS_TO_DELETE = 5

    class Builder {
      var throwsWhileDescribingStream: RuntimeException = null
      var streamConsumerStatus = ACTIVE
      var streamConsumerNotFound = false

      def build: StreamConsumerFakeKinesisSync = new StreamConsumerFakeKinesisSync(this)

      def withStreamConsumerNotFound(streamConsumerNotFound: Boolean): StreamConsumerFakeKinesisSync.Builder = {
        this.streamConsumerNotFound = streamConsumerNotFound
        this
      }

      def withThrowsWhileDescribingStream(throwsWhileDescribingStream: RuntimeException): StreamConsumerFakeKinesisSync.Builder = {
        this.throwsWhileDescribingStream = throwsWhileDescribingStream
        this
      }

      def withStreamConsumerStatus(streamConsumerStatus: ConsumerStatus): StreamConsumerFakeKinesisSync.Builder = {
        this.streamConsumerStatus = streamConsumerStatus
        this
      }
    }
  }

  class StreamConsumerFakeKinesisSync (builder: StreamConsumerFakeKinesisSync.Builder) extends FakeKinesisClientConsumerAdapter {

    private var throwsWhileDescribingStream: RuntimeException = builder.throwsWhileDescribingStream
    private var streamConsumerArn = STREAM_CONSUMER_ARN_EXISTING
    private var streamConsumerStatus: ConsumerStatus = builder.streamConsumerStatus
    private var streamConsumerNotFound = builder.streamConsumerNotFound
    private var numberOfDescribeStreamConsumerInvocations = 0

    def getNumberOfDescribeStreamConsumerInvocations: Int = numberOfDescribeStreamConsumerInvocations

    override def describeStreamSummary(stream: String): DescribeStreamSummaryResponse = {
      if (throwsWhileDescribingStream != null) throw throwsWhileDescribingStream
      DescribeStreamSummaryResponse.builder.streamDescriptionSummary(StreamDescriptionSummary.builder.streamARN(STREAM_ARN).build).build
    }


    override def registerStreamConsumer(streamArn: String, consumerName: String): RegisterStreamConsumerResponse = {
      assert(streamArn == STREAM_ARN)
      streamConsumerNotFound = false
      streamConsumerArn = STREAM_CONSUMER_ARN_NEW
      RegisterStreamConsumerResponse.builder
        .consumer(Consumer.builder.consumerARN(STREAM_CONSUMER_ARN_NEW).consumerStatus(streamConsumerStatus).build)
        .build
    }

    override def deregisterStreamConsumer(consumerArn: String,
                                          consumerName: String): DeregisterStreamConsumerResponse = {
      streamConsumerStatus = DELETING
      DeregisterStreamConsumerResponse.builder.build
    }

    override def describeStreamConsumer(streamArn: String, consumerName: String): DescribeStreamConsumerResponse = {
      assert(streamArn == STREAM_ARN)
      numberOfDescribeStreamConsumerInvocations += 1
      if (streamConsumerStatus == DELETING
        && numberOfDescribeStreamConsumerInvocations
          == StreamConsumerFakeKinesisSync.NUMBER_OF_DESCRIBE_REQUESTS_TO_DELETE) {
        streamConsumerNotFound = true
      } else if (numberOfDescribeStreamConsumerInvocations == StreamConsumerFakeKinesisSync.NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE) {
        streamConsumerStatus = ACTIVE
      }

      if (streamConsumerNotFound) throw new ExecutionException(ResourceNotFoundException.builder.build)

      DescribeStreamConsumerResponse.builder
        .consumerDescription(ConsumerDescription.builder.consumerARN(streamConsumerArn).consumerName(consumerName)
        .consumerStatus(streamConsumerStatus).build)
        .build
    }
  }

  private def createRecord(sequenceNumber: AtomicInteger): Record = {
    createRecord(randomAlphabetic(32).getBytes(UTF_8), sequenceNumber)
  }

  private def createRecord(data: Array[Byte], sequenceNumber: AtomicInteger): Record = {
    Record.builder
      .approximateArrivalTimestamp(Instant.now)
      .data(SdkBytes.fromByteArray(data))
      .sequenceNumber(String.valueOf(sequenceNumber.incrementAndGet)).
      partitionKey("pk")
      .build
  }

  private def createAggregatedRecord(aggregationFactor: Int, sequenceNumber: AtomicInteger) = {
    val recordAggregator = new RecordAggregator
    for (i <- 0 until aggregationFactor) {
      try recordAggregator.addUserRecord("pk", randomAlphabetic(32).getBytes(UTF_8))
      catch {
        case e: Exception =>
          throw new RuntimeException(e)
      }
    }
    createRecord(recordAggregator.clearAndGet.toRecordBytes, sequenceNumber)
  }

  private def generateEvents(numberOfEvents: Int, sequenceNumber: AtomicInteger): Seq[SubscribeToShardEvent] = {
    (0 until numberOfEvents)
      .map { i =>
        SubscribeToShardEvent.builder
          .records(createRecord(sequenceNumber))
          .continuationSequenceNumber(String.valueOf(i)).build
      }
  }
}
