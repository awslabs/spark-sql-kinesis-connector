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

package org.apache.spark.sql.connector.kinesis.retrieval.efo

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletionException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

import scala.util.Try
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.util.control.NonFatal

import io.netty.handler.timeout.ReadTimeoutException
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.http.SdkCancellationException
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisUtils.toSdkStartingPosition
import org.apache.spark.sql.connector.kinesis.ShardInfo
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus._
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber.EfoSubscriptionEvent
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber.MONITOR_INTERVAL_MS
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber.MONITOR_THREAD_POOL
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber.SubscriptionCancelEvent
import org.apache.spark.sql.connector.kinesis.throwableIsCausedBy
import org.apache.spark.util.ThreadUtils.newDaemonSingleThreadScheduledExecutor

object EfoShardSubscriber {
  private val EVENT_QUEUE_CAPACITY = 2

  private val DEFAULT_EVENT_QUEUE_TIMEOUT = Duration.ofSeconds(35)

  val MONITOR_THREAD_POOL = newDaemonSingleThreadScheduledExecutor("EfoShardSubscriber-monitoring")
  val MONITOR_INTERVAL_MS: Int = 500 // milliseconds


  def apply(consumerArn: String,
            streamShard: StreamShard,
            kinesis: KinesisClientConsumer,
            subscribeToShardTimeout: Duration,
            runningSupplier: => Boolean): EfoShardSubscriber = {
    new EfoShardSubscriber(consumerArn, streamShard, kinesis, subscribeToShardTimeout, DEFAULT_EVENT_QUEUE_TIMEOUT, runningSupplier)
  }

  private sealed trait EfoSubscriptionEvent {
    def isSubscribeToShardEvent: Boolean = false

    def isSubscriptionComplete: Boolean = false

    def isSubscriptionCancelled: Boolean = false

    def getSubscribeToShardEvent: SubscribeToShardEvent =
      throw new UnsupportedOperationException("This event does not support getSubscribeToShardEvent()")

    def getThrowable: Throwable = throw new UnsupportedOperationException("This event does not support getThrowable()")
  }

  private class SubscriptionCompleteEvent extends EfoSubscriptionEvent {
    override def isSubscriptionComplete: Boolean = true
  }

  private class SubscriptionCancelEvent extends EfoSubscriptionEvent {
    override def isSubscriptionCancelled: Boolean = true
  }

  private class SubscriptionErrorEvent(val throwable: Throwable) extends EfoSubscriptionEvent {
    override def getThrowable: Throwable = throwable
  }

  private class SubscriptionNextEvent (val subscribeToShardEvent: SubscribeToShardEvent) extends EfoSubscriptionEvent {
    override def isSubscribeToShardEvent: Boolean = true
    override def getSubscribeToShardEvent: SubscribeToShardEvent = subscribeToShardEvent
  }
}
class EfoShardSubscriber(val consumerArn: String,
                                          val streamShard: StreamShard,
                                          val kinesis: KinesisClientConsumer,
                                          val subscribeToShardTimeout: Duration,
                                          val queueWaitTimeout: Duration,
                                          runningSupplier: => Boolean) extends Logging{

  private val eventQueue: BlockingQueue[EfoSubscriptionEvent] =
    new ArrayBlockingQueue[EfoSubscriptionEvent](EfoShardSubscriber.EVENT_QUEUE_CAPACITY)

  private val subscriptionErrorEvent: AtomicReference[EfoSubscriptionEvent] = new AtomicReference[EfoSubscriptionEvent]

  private val runningMonitorThreadRunning = new AtomicBoolean(true)

  MONITOR_THREAD_POOL.schedule(new Runnable() {
    override def run(): Unit = {
      Try {
        if (!runningMonitorThreadRunning.get || !runningSupplier) {
          // If there is space in the queue, insert the event to wake up blocked thread
          logInfo(s"runningMonitorThread send SubscriptionCancelEvent for streamShard ${streamShard}")
          if (!eventQueue.offer(new SubscriptionCancelEvent())) {
            logInfo("SubscriptionCancelEvent not send as event queue is full for streamShard ${streamShard}")
          }
        }
        else {
          MONITOR_THREAD_POOL.schedule(this, MONITOR_INTERVAL_MS, TimeUnit.MILLISECONDS)
        }
      }
    }
  }, MONITOR_INTERVAL_MS, TimeUnit.MILLISECONDS)


  def subscribeToShardAndConsumeRecords(
               startingPosition: KinesisPosition,
               eventConsumer: Consumer[SubscribeToShardEvent]): RecordsPublisherRunStatus = {
    logInfo(s"Subscribing to shard ${consumerArn}::${streamShard}, startingPosition ${startingPosition}")

    val subscription: EfoShardSubscription = try {
      openSubscriptionToShard(startingPosition)
    } catch {
      case ex: EfoSubscriberException =>
        ex.getCause match {
          case exception: ResourceNotFoundException =>
            throw exception
          case _ => throw ex
        }
    }
    consumeAllRecordsFromKinesisShard(eventConsumer, subscription)
  }

  // visible for test only
  def openSubscriptionToShard(startingPosition: KinesisPosition): EfoShardSubscription = {
    logInfo(s"openSubscriptionToShard startingPosition: ${startingPosition}, ${streamShard}")
    val request: SubscribeToShardRequest = SubscribeToShardRequest
      .builder
      .consumerARN(consumerArn)
      .shardId(streamShard.shard.shardId())
      .startingPosition(
        toSdkStartingPosition(
          ShardInfo(streamShard.shard.shardId(),
          startingPosition.iteratorType,
          startingPosition.iteratorPosition,
          startingPosition.subSequenceNumber,
          startingPosition.isLast
        )))
      .build
    val exception: AtomicReference[Throwable] = new AtomicReference[Throwable]
    val waitForSubscriptionLatch: CountDownLatch = new CountDownLatch(1)
    val subscription: EfoShardSubscription = new EfoShardSubscription(waitForSubscriptionLatch)
    val responseHandler: SubscribeToShardResponseHandler = SubscribeToShardResponseHandler.builder
      .onError((e: Throwable) => {
          if (waitForSubscriptionLatch.getCount > 0) {
            exception.set(e)
            waitForSubscriptionLatch.countDown()
          }
      })
      .subscriber(() => subscription)
      .build

    kinesis.subscribeToShard(request, responseHandler)
    val subscriptionEstablished: Boolean = waitForSubscriptionLatch.await(subscribeToShardTimeout.toMillis, TimeUnit.MILLISECONDS)
    if (!subscriptionEstablished) {
      val errorMessage = s"Timed out after ${subscribeToShardTimeout.toMillis}ms acquiring subscription - ${consumerArn} :: ${streamShard}"
      logError(errorMessage)
      subscription.cancelSubscription()
      handleError(new EfoRecoverableSubscriberException(new TimeoutException(errorMessage)))
    }
    val throwable: Throwable = exception.get
    if (throwable != null) {
      handleError(throwable)
    }
    logInfo(s"Acquired subscription - ${consumerArn} :: ${streamShard}")

    // Request the first record
    subscription.requestRecord()
    subscription
  }

  private def handleError(throwable: Throwable): Unit = {
    logWarning(s"handleError occurred on EFO subscriber: ${consumerArn}::${streamShard})", throwable)

    if (isInterrupted(throwable)) {
      throw new EfoSubscriberInterruptedException(throwable)
    }

    val cause: Throwable = throwable match {
      case _ if throwable.isInstanceOf[CompletionException] || throwable.isInstanceOf[ExecutionException] =>
        throwable.getCause
      case _ =>
        throwable
    }

    cause match {
      case e: EfoSubscriberException => throw e
      case _ if cause.isInstanceOf[ReadTimeoutException] =>
          throw new EfoRecoverableSubscriberException(cause)
      case _ =>
          throw new EfoRetryableSubscriberException(cause)
    }
  }

  private def isInterrupted(throwable: Throwable): Boolean = {
    var cause: Throwable = throwable
    while (cause != null) {
      if (cause.isInstanceOf[InterruptedException]) {
        return true
      }
      cause = cause.getCause
    }
    false
  }

  private def consumeAllRecordsFromKinesisShard(eventConsumer: Consumer[SubscribeToShardEvent],
                                                  subscription: EfoShardSubscription): RecordsPublisherRunStatus = {
    var continuationSequenceNumber: String = null
    var result: RecordsPublisherRunStatus = COMPLETE

    breakable {
      do {

        if (!runningSupplier) {
          logInfo(s"EfoShardSubscriber cancelled - ${consumerArn}::${streamShard}")
          result = CANCELLED
          break
        }
        val subscriptionEvent = if (subscriptionErrorEvent.get != null) {
          Some(subscriptionErrorEvent.get)
        } else {
          Option(eventQueue.poll(queueWaitTimeout.toMillis, MILLISECONDS))
        }

        if (subscriptionEvent.isEmpty) {
          logInfo(s"Timed out waiting events from network, reacquiring subscription - ${consumerArn}::${streamShard}")
          result = INCOMPLETE
          break
        } else {
          if (subscriptionEvent.get.isSubscribeToShardEvent) { // Request for KDS to send the next record batch
            logTrace(s"EfoShardSubscriber isSubscribeToShardEvent - ${consumerArn}::${streamShard}")
            subscription.requestRecord()
            val event: SubscribeToShardEvent = subscriptionEvent.get.getSubscribeToShardEvent
            continuationSequenceNumber = event.continuationSequenceNumber
            eventConsumer.accept(event)
          } else {
            if (subscriptionEvent.get.isSubscriptionComplete) {
              // The subscription is complete, but the shard might not be, so we return incomplete
              logInfo(s"EfoShardSubscriber isSubscriptionComplete - ${consumerArn}::${streamShard}")
              result = INCOMPLETE
              break
            } else if (subscriptionEvent.get.isSubscriptionCancelled) {
              // The subscription is cancelled, but the shard might not be, so we return incomplete
              logInfo(s"EfoShardSubscriber isSubscriptionCancelled - ${consumerArn}::${streamShard}")
              result = INCOMPLETE
              break

            } else {
              logInfo(s"EfoShardSubscriber handleError - ${consumerArn}::${streamShard}")
              handleError(subscriptionEvent.get.getThrowable)
              result = INCOMPLETE
              break
            }
          }
        }
      } while (continuationSequenceNumber != null)
    }

    subscription.cancelSubscription()
    result
  }

  def close(): Unit = {
    runningMonitorThreadRunning.set(false)
  }

  // visible for test only
  class EfoShardSubscription (val waitForSubscriptionLatch: CountDownLatch)
    extends Subscriber[SubscribeToShardEventStream] {
    private var subscription: Subscription = _
    private val cancelled = new AtomicBoolean(false)

    private def isCancelled = cancelled.get || !runningSupplier

    /** Flag to the producer that we are ready to receive more events. */
    def requestRecord(): Unit = {
      if (!isCancelled) {
        logTrace(s"Requesting more records from EFO subscription - ${consumerArn}::${streamShard}")
        subscription.request(1)
      }
    }

    override def onSubscribe(subscription: Subscription): Unit = {
      this.subscription = subscription
      waitForSubscriptionLatch.countDown()
    }

    override def onNext(subscribeToShardEventStream: SubscribeToShardEventStream): Unit = {
      subscribeToShardEventStream.accept(new SubscribeToShardResponseHandler.Visitor() {
        override def visit(event: SubscribeToShardEvent): Unit = {
          enqueueEvent(new EfoShardSubscriber.SubscriptionNextEvent(event))
        }
      })
    }

    override def onError(throwable: Throwable): Unit = {
      logWarning(s"Error occurred on EFO subscription ${consumerArn}::${streamShard}: ", throwable)

      val subscriptionErrorEvent = new EfoShardSubscriber.SubscriptionErrorEvent(throwable)
      if (EfoShardSubscriber.this.subscriptionErrorEvent.get == null) {
        EfoShardSubscriber.this.subscriptionErrorEvent.set(subscriptionErrorEvent)
      }
      else {
        logWarning("Error already queued. Ignoring subsequent exception.", throwable)
      }

      cancelSubscription()

      // If there is space in the queue, insert the error to wake up blocked thread
      eventQueue.offer(subscriptionErrorEvent)
    }

    override def onComplete(): Unit = {
      logInfo(s"EFO subscription complete - ${consumerArn}::${streamShard}")
      enqueueEvent(new EfoShardSubscriber.SubscriptionCompleteEvent)
    }

    def cancelSubscription(): Unit = {
      if (cancelled.compareAndSet(false, true)) {
        if (subscription != null) {
          try {
            subscription.cancel()
          } catch {
            case ce if throwableIsCausedBy[SdkCancellationException](ce) =>
              logWarning(s"Error ${ce.getMessage} caused by cancel subscription exception. Ignore it.", ce)
            case NonFatal(e) =>
              logWarning(s"exception when cancel subscription: ${e.getMessage}")
              throw e
          }
        }
      } else {
        logInfo(s"EFO subscription ${consumerArn}::${streamShard} " +
          s"- is already cancelled.")
      }
    }

    private def enqueueEvent(event: EfoShardSubscriber.EfoSubscriptionEvent): Unit = {
      if (!isCancelled && !eventQueue.offer(event, queueWaitTimeout.toMillis, TimeUnit.MILLISECONDS)) {
        val errorMessage = s"Timed out enqueuing event ${event.getClass.getSimpleName} at ${consumerArn}::${streamShard}"
        logError(errorMessage)
        onError(new EfoRecoverableSubscriberException(new TimeoutException(errorMessage)))
      }
    }
  }
}
