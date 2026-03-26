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
package org.apache.spark.sql.connector.kinesis.client

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException

import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TestConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard
import org.apache.spark.sql.connector.kinesis.retrieval.client.FakeKinesisClientConsumerAdapter
import org.apache.spark.sql.connector.kinesis.retrieval.polling.PollingRecordBatchPublisher
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KinesisClientConsumerGetShardIteratorSuite extends KinesisTestBase {

  test("KinesisOptions defaults failOnDataLoss to false") {
    DEFAULT_KINESIS_OPTIONS.failOnDataLoss shouldBe false
  }

  test("KinesisOptions parses explicit failOnDataLoss true") {
    val options = KinesisOptions(new CaseInsensitiveStringMap(Map(
      REGION -> DEFAULT_TEST_REGION,
      ENDPOINT_URL -> DEFAULT_TEST_ENDPOINT_URL,
      CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
      STREAM_NAME -> DEFAULT_TEST_STEAM_NAME,
      FAIL_ON_DATA_LOSS -> "true"
    ).asJava))
    options.failOnDataLoss shouldBe true
  }

  test("getShardIterator with default failOnDataLoss swallows ResourceNotFoundException") {
    val client = new FakeKinesisClientConsumerAdapter {
      override def getShardIterator(shardId: String,
                                    iteratorType: String,
                                    iteratorPosition: String,
                                    failOnDataLoss: Boolean = false): String = {
        val ex = ResourceNotFoundException.builder().message("Shard not found").build()
        if (!failOnDataLoss) {
          GetShardIteratorResponse.builder().build().shardIterator()
        } else {
          throw ex
        }
      }

      override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
        GetRecordsResponse.builder().millisBehindLatest(0L).build()
      }
    }

    // Calling with default (no failOnDataLoss arg) should not throw
    val result = client.getShardIterator("shardId-000000000000", "LATEST", "")
    result shouldBe null
  }

  test("getShardIterator with failOnDataLoss=true throws ResourceNotFoundException") {
    val client = new FakeKinesisClientConsumerAdapter {
      override def getShardIterator(shardId: String,
                                    iteratorType: String,
                                    iteratorPosition: String,
                                    failOnDataLoss: Boolean = false): String = {
        val ex = ResourceNotFoundException.builder().message("Shard not found").build()
        if (!failOnDataLoss) {
          GetShardIteratorResponse.builder().build().shardIterator()
        } else {
          throw ex
        }
      }
    }

    intercept[ResourceNotFoundException] {
      client.getShardIterator("shardId-000000000000", "LATEST", "", failOnDataLoss = true)
    }
  }

  test("PollingRecordBatchPublisher passes failOnDataLoss=false to getShardIterator on init") {
    var capturedFailOnDataLoss: Option[Boolean] = None

    val client = new FakeKinesisClientConsumerAdapter {
      override def getShardIterator(shardId: String,
                                    iteratorType: String,
                                    iteratorPosition: String,
                                    failOnDataLoss: Boolean = false): String = {
        capturedFailOnDataLoss = Some(failOnDataLoss)
        "iter-0"
      }

      override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
        GetRecordsResponse.builder().millisBehindLatest(0L).build()
      }
    }

    val publisher = new PollingRecordBatchPublisher(
      KinesisPosition.make(Latest.iteratorType, DEFAULT_TIMESTAMP, NO_SUB_SEQUENCE_NUMBER, isLast = true),
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      client,
      DEFAULT_KINESIS_OPTIONS,
      true
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    publisher.runProcessLoop(consumer)

    capturedFailOnDataLoss shouldBe Some(false)
  }

  test("PollingRecordBatchPublisher passes failOnDataLoss from KinesisOptions on expired iterator refresh") {
    val capturedFailOnDataLossValues = scala.collection.mutable.ArrayBuffer.empty[Boolean]
    var callCount = 0

    val optionsWithFailOnDataLossTrue = KinesisOptions(new CaseInsensitiveStringMap(Map(
      REGION -> DEFAULT_TEST_REGION,
      ENDPOINT_URL -> DEFAULT_TEST_ENDPOINT_URL,
      CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
      STREAM_NAME -> DEFAULT_TEST_STEAM_NAME,
      FAIL_ON_DATA_LOSS -> "true"
    ).asJava))

    val client = new FakeKinesisClientConsumerAdapter {
      override def getShardIterator(shardId: String,
                                    iteratorType: String,
                                    iteratorPosition: String,
                                    failOnDataLoss: Boolean = false): String = {
        capturedFailOnDataLossValues += failOnDataLoss
        "iter-0"
      }

      override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
        callCount += 1
        if (callCount == 1) {
          throw software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
            .builder().message("Expired").build()
        }
        GetRecordsResponse.builder().millisBehindLatest(0L).build()
      }
    }

    val publisher = new PollingRecordBatchPublisher(
      KinesisPosition.make(Latest.iteratorType, DEFAULT_TIMESTAMP, NO_SUB_SEQUENCE_NUMBER, isLast = true),
      StreamShard(DEFAULT_TEST_STEAM_NAME, DEFAULT_TEST_SHARD),
      client,
      optionsWithFailOnDataLossTrue,
      true
    )

    val consumer = new TestConsumer(publisher.initialStartingPosition)
    publisher.runProcessLoop(consumer)

    // First call (init) uses default false, second call (expired refresh) uses kinesisOptions.failOnDataLoss = true
    capturedFailOnDataLossValues.size shouldBe 2
    capturedFailOnDataLossValues(0) shouldBe false
    capturedFailOnDataLossValues(1) shouldBe true
  }
}
