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

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import io.netty.handler.timeout.ReadTimeoutException
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.core.exception.SdkInterruptedException
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent

import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.retrieval.client.FakeEfoClientConsumerFactory
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRecoverableSubscriberException
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRetryableSubscriberException
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoShardSubscriber
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoSubscriberInterruptedException

class EfoShardSubscriberSuite extends KinesisTestBase {

  test("subscription completed") {

    val efoClient = FakeEfoClientConsumerFactory.emptyBatchFollowedBySingleRecord

    val subscriber = new EfoShardSubscriber(
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      efoClient,
      DEFAULT_TEST_TIMEOUT,
      DEFAULT_TEST_TIMEOUT,
      true)

    val result = subscriber.subscribeToShardAndConsumeRecords(
      DEFAULT_START_POSITION,
      (_: SubscribeToShardEvent) => {}
    )

    result shouldBe RecordBatchPublisherRunStatus.INCOMPLETE

    subscriber.close()

  }

  test("shard ended") {

    val efoClient = FakeEfoClientConsumerFactory.boundedShard.withBatchCount(128).build

    val isRunning = new AtomicBoolean(true)

    val subscriber = new EfoShardSubscriber(
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      efoClient,
      DEFAULT_TEST_TIMEOUT,
      DEFAULT_TEST_TIMEOUT,
      isRunning.get)

    val batches = new AtomicInteger(0)

    val result = subscriber.subscribeToShardAndConsumeRecords(
      DEFAULT_START_POSITION,
      (_: SubscribeToShardEvent) => {
        batches.incrementAndGet
      }
    )

    batches.get shouldBe 128
    result shouldBe RecordBatchPublisherRunStatus.COMPLETE

    subscriber.close()
  }

  test("subscription cancelled") {

    val efoClient = FakeEfoClientConsumerFactory.boundedShard.withBatchCount(128).build

    val isRunning = new AtomicBoolean(true)

    val subscriber = new EfoShardSubscriber(
      DEFAULT_TEST_CONSUMER_ARN,
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      efoClient,
      DEFAULT_TEST_TIMEOUT,
      DEFAULT_TEST_TIMEOUT,
      isRunning.get)

    val batches = new AtomicInteger(0)

    val result = subscriber.subscribeToShardAndConsumeRecords(
      DEFAULT_START_POSITION,
      (_: SubscribeToShardEvent) => {
        batches.incrementAndGet

        if (batches.get == 8) {
          // Set running to false, this will cancel record consumption
          isRunning.set(false)
          Thread.sleep(100)
        }
      }
    )

    batches.get shouldBe 8
    result shouldBe RecordBatchPublisherRunStatus.CANCELLED

    subscriber.close()
  }

  test("Throw EfoRecoverableSubscriberException") {

    val exception = intercept[EfoRecoverableSubscriberException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.errorDuringSubscription(ReadTimeoutException.INSTANCE)

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {}
      )

      subscriber.close()
    }

    exception.getMessage should include ("io.netty.handler.timeout.ReadTimeoutException")

  }

  test("Subscribe to shard timeout") {

    val exception = intercept[EfoRecoverableSubscriberException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.failsToAcquireSubscription

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {}
      )

      subscriber.close()
    }

    exception.getCause.isInstanceOf[TimeoutException] shouldBe true
  }

  test("enqueue timeout") {

    val exception = intercept[EfoRecoverableSubscriberException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.shardThatCreatesBackpressureOnQueue

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {
          Thread.sleep(120)
        }
      )

      subscriber.close()
    }

    exception.getMessage should include ("Timed out enqueuing event SubscriptionNextEvent")
  }

  test("Throw EfoRetryableSubscriberException") {

    val exception = intercept[EfoRetryableSubscriberException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.errorDuringSubscription(new RuntimeException("Runtime Error."))

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {}
      )

      subscriber.close()
    }

    exception.getMessage should include("Runtime Error.")

  }

  test("Pass first error") {

    val exception = intercept[EfoRetryableSubscriberException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.errorDuringSubscription(
        new RuntimeException("Runtime Error1."),
        new RuntimeException("Runtime Error2.")
      )

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {}
      )

      subscriber.close()
    }

    exception.getMessage should include("Runtime Error1.")


  }

  test("Throw EfoInterruptedException") {

    intercept[EfoSubscriberInterruptedException] {
      val errorEfoClient = FakeEfoClientConsumerFactory.errorDuringSubscription(new SdkInterruptedException(null))

      val subscriber = new EfoShardSubscriber(
        DEFAULT_TEST_CONSUMER_ARN,
        StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
        errorEfoClient,
        DEFAULT_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT,
        true)

      subscriber.subscribeToShardAndConsumeRecords(
        DEFAULT_START_POSITION,
        (_: SubscribeToShardEvent) => {}
      )

      subscriber.close()
    }
  }

}
