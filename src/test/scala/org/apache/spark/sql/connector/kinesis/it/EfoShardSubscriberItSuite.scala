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

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

import scala.jdk.CollectionConverters._
import scala.util.Random

import com.google.common.util.concurrent.MoreExecutors
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.KinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisUtils
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.INCOMPLETE
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherRunStatus.RecordsPublisherRunStatus
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRecoverableSubscriberException
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRetryableSubscriberException
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber

@IntegrationTestSuite
class EfoShardSubscriberItSuite extends KinesisIntegrationTestBase(EFO_CONSUMER_TYPE)
  with Matchers
  with Logging {

  val stopTimerExecutorService: ExecutorService = MoreExecutors.getExitingExecutorService(
    Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor])

  val asyncDataSenderExecutorService: ExecutorService = MoreExecutors.getExitingExecutorService(
    Executors.newFixedThreadPool(1).asInstanceOf[ThreadPoolExecutor])

  override def afterAll(): Unit = {
    try {
      stopTimerExecutorService.shutdown()
      asyncDataSenderExecutorService.shutdown()
    } finally {
      super.afterAll()
    }
  }

  def createConsumer(receivedCount: AtomicInteger,
                     nextStartingPosition: AtomicReference[KinesisPosition],
                     sleepTimeMs: Long
                    ): Consumer[SubscribeToShardEvent] = {
    new Consumer[SubscribeToShardEvent]() {
      override def accept(event: SubscribeToShardEvent): Unit = {
        logInfo(s"eventConsumer ${event}")

        var lastSequenceNumber: String = null
        event.records().asScala.foreach { record =>

          if (sleepTimeMs > 0) {
            Thread.sleep(sleepTimeMs)
          }
          else if (sleepTimeMs < 0) { // use the abs value for random sleep time
            val randomSleepTimeSec = Random.nextInt(Math.abs(sleepTimeMs/1000).toInt)
            logInfo(s"randomSleepTimeSec ${randomSleepTimeSec}")
            Thread.sleep(randomSleepTimeSec * 1000)
          }
          receivedCount.incrementAndGet()
          lastSequenceNumber = record.sequenceNumber()
          logInfo(s"receivedCount ${receivedCount.get()}, lastSequenceNumber ${lastSequenceNumber}")
        }
        if (lastSequenceNumber != null) {
          nextStartingPosition.set(SequenceNumber.continueFromSequenceNumber(
            SequenceNumber(lastSequenceNumber, NO_SUB_SEQUENCE_NUMBER, isLast = true)))
        }
      }
    }
  }

  def createAsyncDataSender(sendDataTimeInSec: Long,
                            sleepTimeInMs: Long,
                            sentCount: AtomicInteger,
                            kinesisTestUtils: KinesisTestUtils,
                            testdata: Array[String],
                            isRunning: Option[AtomicBoolean] = None
                           ): Future[_] = {


    asyncDataSenderExecutorService.submit(
      new Runnable {
        def run(): Unit = {
          val start = System.currentTimeMillis()
          var last = System.currentTimeMillis()
          while ((last - start) < (sendDataTimeInSec * 1000L)) {
            logInfo(s"Elapsed time in seconds ${(last - start) / 1000L}")
            kinesisTestUtils.pushData(testdata, aggregate = false)
            sentCount.addAndGet(testdata.length)
            Thread.sleep(sleepTimeInMs)
            last = System.currentTimeMillis()
          }

          if (isRunning.isDefined) isRunning.get.set(false)

          Thread.sleep(sleepTimeInMs)
          logInfo("pushData done")
        }
      }
    )
  }

  def createStopProcessingTimer(waitTimeMs: Long, isRunning: AtomicBoolean): Future[_] = {
    stopTimerExecutorService.submit(
      new Runnable {
        def run(): Unit = {
          Thread.sleep(waitTimeMs)
          logInfo(s"Stop processing timer fired after ${waitTimeMs}ms.")
          isRunning.set(false)
        }
      }
    )
  }

  test("EfoShardSubscriber read data successfully") {

    val receivedCount = new AtomicInteger(0)
    val testdata = Array("1", "2", "3") // all data goes to shardId-000000000001

    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    localTestUtils.pushData(testdata, aggregate = false)

    val kinesisReader: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(
        defaultKinesisOptions,
        localTestUtils.streamName, localTestUtils.endpointUrl)
    }

    try {

      val consumerArn = KinesisUtils.registerStreamConsumer(
        kinesisReader,
        defaultKinesisOptions.consumerName.get
      )
      val isRunning = new AtomicBoolean(true)

      def getEfoSubscriber: EfoShardSubscriber = new EfoShardSubscriber(
        consumerArn,
        StreamShard(localTestUtils.streamName,
          Shard.builder().shardId("shardId-000000000001").build()
        ),
        kinesisReader,
        Duration.ofSeconds(35),
        Duration.ofSeconds(35),
        isRunning.get)

      val nextStartingPosition: AtomicReference[KinesisPosition] = new AtomicReference(TrimHorizon())

      def eventConsumer: Consumer[SubscribeToShardEvent] = createConsumer(receivedCount, nextStartingPosition, 0)

      var result: RecordsPublisherRunStatus = INCOMPLETE

      createStopProcessingTimer(10000, isRunning)
      while (result == INCOMPLETE ) {
        result = getEfoSubscriber.subscribeToShardAndConsumeRecords(
          nextStartingPosition.get(),
          eventConsumer
        )

        logInfo(s"result: ${result}")
      }

      receivedCount.get shouldBe testdata.length

    }
    finally {
      KinesisUtils.deregisterStreamConsumer(kinesisReader, defaultKinesisOptions.consumerName.get)
      kinesisReader.close()
      localTestUtils.deleteStream()
    }
  }


  // If call SubscribeToShard again with the same ConsumerARN and ShardId within 5 seconds of a successful call,
  // will get a ResourceInUseException
  test("EfoShardSubscriber throws EfoRetryableSubscriberException on ResourceInUseException") {

    val kinesisReader1: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(defaultKinesisOptions,
        testUtils.streamName, testUtils.endpointUrl)
    }

    val kinesisReader2: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(defaultKinesisOptions,
        testUtils.streamName, testUtils.endpointUrl)
    }

    try {

      val consumerArn = KinesisUtils.registerStreamConsumer(
        kinesisReader1,
        defaultKinesisOptions.consumerName.get
      )

      val isRunning = new AtomicBoolean(true)

      def getEfoSubscriber(kinesisReader: KinesisClientConsumer): EfoShardSubscriber = new EfoShardSubscriber(
        consumerArn,
        StreamShard(testUtils.streamName,
          Shard.builder().shardId("shardId-000000000000").build()
        ),
        kinesisReader,
        Duration.ofSeconds(5),
        Duration.ofSeconds(5),
        isRunning.get)

      val subscription1 = getEfoSubscriber(kinesisReader1).openSubscriptionToShard(
        new Latest()
      )

      val ex = intercept[EfoRetryableSubscriberException] {
        // 2nd subscribeToShard will cause ResourceInUseException
        getEfoSubscriber(kinesisReader2).openSubscriptionToShard(
          new Latest()
        )
      }

      subscription1.cancelSubscription()
      assert(ex.getCause.isInstanceOf[ResourceInUseException], s"${ex.getCause} is not ResourceInUseException")
      ex.getCause.getMessage should include ("Another active subscription exists for this consumer")

    }
    finally {
      KinesisUtils.deregisterStreamConsumer(kinesisReader1, defaultKinesisOptions.consumerName.get)
      kinesisReader1.close()
      kinesisReader2.close()
    }

  }

  test("EfoShardSubscriber throws EfoRecoverableSubscriberException on subscribeToShard timeout") {

    val waitTimeMs = 10
    val kinesisReader1: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(defaultKinesisOptions,
        testUtils.streamName, testUtils.endpointUrl)
    }

    try {

      val consumerArn = KinesisUtils.registerStreamConsumer(
        kinesisReader1,
        defaultKinesisOptions.consumerName.get
      )

      val isRunning = new AtomicBoolean(true)

      def getEfoSubscriber(kinesisReader: KinesisClientConsumer): EfoShardSubscriber = new EfoShardSubscriber(
        consumerArn,
        StreamShard(testUtils.streamName,
          Shard.builder().shardId("shardId-000000000000").build()
        ),
        kinesisReader,
        Duration.ofMillis(waitTimeMs),
        Duration.ofSeconds(5),
        isRunning.get)

      val ex = intercept[EfoRecoverableSubscriberException] {
        getEfoSubscriber(kinesisReader1).openSubscriptionToShard(
          new Latest()
        )
      }

      assert(ex.getCause.isInstanceOf[TimeoutException], s"${ex.getCause} is not TimeoutException")
      ex.getCause.getMessage should include(s"Timed out after ${waitTimeMs}ms acquiring subscription")

    }
    finally {
      KinesisUtils.deregisterStreamConsumer(kinesisReader1, defaultKinesisOptions.consumerName.get)
      kinesisReader1.close()
    }

  }

  // Any back pressure larger than httpClientReadTimeout will result in a ReadTimeoutException
  test("EfoShardSubscriber throws EfoRecoverableSubscriberException on ReadTimeoutException") {

    val httpClientReadTimeoutSec = 10
    val sendDataTimeInSec: Long = 60L
    val sleepTimeInMs: Long = 20000L
    val receivedCount = new AtomicInteger(0)
    val sentCount = new AtomicInteger(0)
    val testdata = Array("1", "2", "3") // all data goes to shardId-000000000001

    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()
    val kinesisReader: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(
        defaultKinesisOptions.copy(httpClientReadTimeout = Duration.ofSeconds(httpClientReadTimeoutSec)),
        localTestUtils.streamName, localTestUtils.endpointUrl)
    }

    try {

      val consumerArn = KinesisUtils.registerStreamConsumer(
        kinesisReader,
        defaultKinesisOptions.consumerName.get
      )
      val isRunning = new AtomicBoolean(true)

      def getEfoSubscriber: EfoShardSubscriber = new EfoShardSubscriber(
        consumerArn,
        StreamShard(localTestUtils.streamName,
          Shard.builder().shardId("shardId-000000000001").build()
        ),
        kinesisReader,
        Duration.ofSeconds(35),
        Duration.ofSeconds(35),
        isRunning.get)

      var nextStartingPosition: AtomicReference[KinesisPosition] = new AtomicReference(TrimHorizon())

      def randomSleepEventConsumer: Consumer[SubscribeToShardEvent] = createConsumer(
        receivedCount,
        nextStartingPosition,
        httpClientReadTimeoutSec * 2000 * -1) // add random sleep so sometimes can cause ReadTimeoutException

      createAsyncDataSender(sendDataTimeInSec, sleepTimeInMs, sentCount, localTestUtils, testdata)

      createStopProcessingTimer(
        httpClientReadTimeoutSec * 2000 *(sendDataTimeInSec*1000/sleepTimeInMs + 1), // make sure all data are processed
        isRunning
      )

      var result: RecordsPublisherRunStatus = INCOMPLETE
      while (result == INCOMPLETE) {
        try {
          result = getEfoSubscriber.subscribeToShardAndConsumeRecords(
            nextStartingPosition.get,
            randomSleepEventConsumer
          )
          logInfo(s"result: ${result}")
        } catch {
          case e: EfoRecoverableSubscriberException =>
            logInfo(s"got recoverable error. Retry.", e)
            assert(
              e.getCause.isInstanceOf[io.netty.handler.timeout.ReadTimeoutException],
              s"${e.getCause} is not ReadTimeoutException"
            )
            Thread.sleep(1000)
        }
      }


      receivedCount.get() shouldBe sentCount.get()

    }
    finally {
      KinesisUtils.deregisterStreamConsumer(kinesisReader, defaultKinesisOptions.consumerName.get)
      localTestUtils.deleteStream()
      kinesisReader.close()
    }
  }

  // Established HTTP/2 connections are terminated by KDS every 5 minutes,
  // so the client will need to call SubscribeToShard again to continue receiving events
  test("EfoShardSubscriber returns INCOMPLETE when subscription expired every 5 min") {

    val sendDataTimeInSec: Long = 360L // more than 5 min so that the subscription is expired
    val sleepTimeInMs: Long = 5000L
    val receivedCount = new AtomicInteger(0)
    val sentCount = new AtomicInteger(0)
    val testdata = Array("1", "2", "3") // all data goes to shardId-000000000001

    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    val kinesisReader: KinesisClientConsumer = {
      KinesisClientFactory.createConsumer(defaultKinesisOptions,
        localTestUtils.streamName, localTestUtils.endpointUrl)
    }

    try {

      val consumerArn = KinesisUtils.registerStreamConsumer(
        kinesisReader,
        defaultKinesisOptions.consumerName.get
      )
      val isRunning = new AtomicBoolean(true)

      def getEfoSubscriber: EfoShardSubscriber = new EfoShardSubscriber(
        consumerArn,
        StreamShard(localTestUtils.streamName,
          Shard.builder().shardId("shardId-000000000001").build()
        ),
        kinesisReader,
        Duration.ofSeconds(35),
        Duration.ofSeconds(35),
        isRunning.get)


      var nextStartingPosition: AtomicReference[KinesisPosition] = new AtomicReference(TrimHorizon())

      def eventConsumer: Consumer[SubscribeToShardEvent] = createConsumer(
        receivedCount,
        nextStartingPosition,
        0)

      createAsyncDataSender(sendDataTimeInSec, sleepTimeInMs, sentCount, localTestUtils, testdata)

      createStopProcessingTimer(
        sendDataTimeInSec * 1000 + sleepTimeInMs, // make sure all data are processed
        isRunning
      )


      var result: RecordsPublisherRunStatus = INCOMPLETE

      while (result == INCOMPLETE) {
        result = getEfoSubscriber.subscribeToShardAndConsumeRecords(
          nextStartingPosition.get,
          eventConsumer
        )
        logInfo(s"result: ${result}")
      }

      logInfo(s"total receive count is ${receivedCount.get}, sent count is ${sentCount.get}")
      receivedCount.get shouldBe sentCount.get
    }
    finally {
      KinesisUtils.deregisterStreamConsumer(kinesisReader, defaultKinesisOptions.consumerName.get)
      localTestUtils.deleteStream()
      kinesisReader.close()
    }
  }
}
