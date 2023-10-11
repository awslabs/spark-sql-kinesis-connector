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
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.google.common.util.concurrent.MoreExecutors
import org.apache.commons.lang3.RandomStringUtils.randomAlphabetic
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest

import org.apache.spark.sql.connector.kinesis.AtSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtTimeStamp
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.KinesisTestBase
import org.apache.spark.sql.connector.kinesis.ShardInfo
import org.apache.spark.sql.connector.kinesis.it.DynamodbMetadataCommitterItSuite._
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter
import org.apache.spark.sql.util.CaseInsensitiveStringMap



abstract class DynamodbMetadataCommitterItSuite(optionMap: CaseInsensitiveStringMap) extends KinesisTestBase {
  var batchId: Int = 0

  val options: KinesisOptions = KinesisOptions(optionMap)
  val table: String = options.dynamodbTableName

  override def afterAll(): Unit = {
    try {
      val client = DynamoDbAsyncClient.builder()
        .region(Region.of(options.region))
        .build();

      val deleteTableRequest = DeleteTableRequest.builder.tableName(table).build

      client.deleteTable(deleteTableRequest).join()

    } finally {
      super.afterAll()
    }
  }

  test("create Dynamodb table") {
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)
    dynamodbCommitter.newTableCreated shouldBe true
    dynamodbCommitter.isTableActive shouldBe true

    val dynamodbCommitter2 = new DynamodbMetadataCommitter[ShardInfo](table, options)
    dynamodbCommitter2.newTableCreated shouldBe false
    dynamodbCommitter2.isTableActive shouldBe true
  }

  test("add metadata item to Dynamodb") {
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)
    val localBatchId = { batchId += 1; batchId }

    val shardId1 = "0000001"
    val shardInfo1 = ShardInfo(
      shardId = shardId1,
      iteratorType = AtTimeStamp.iteratorType,
      iteratorPosition = "1234567890",
      subSequenceNumber = NO_SUB_SEQUENCE_NUMBER,
      isLast = true)

    val res1 = dynamodbCommitter.add(localBatchId, shardId1, shardInfo1)
    val res2 = dynamodbCommitter.add(localBatchId, shardId1, shardInfo1)

    res1 shouldBe true
    res2 shouldBe false
  }

  test("get/exists/delete from Dynamodb by batchId") {
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)

    val localBatchId1 = { batchId += 1; batchId }
    val localBatchId2 = { batchId += 1; batchId }

    val shardId1_1 = "0000001"
    val shardId1_2 = "0000002"

    val shardId2_1 = "0000001"
    val shardInfo1_1 = ShardInfo(
      shardId = shardId1_1,
      iteratorType = AtTimeStamp.iteratorType,
      iteratorPosition = "1234567890",
      subSequenceNumber = NO_SUB_SEQUENCE_NUMBER,
      isLast = true)

    val shardInfo1_2 = ShardInfo(
      shardId = shardId1_2,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "abcdefghijklmn",
      subSequenceNumber = 1000,
      isLast = true)

    val shardInfo2_1 = ShardInfo(
      shardId = shardId2_1,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "opqrstuvxyz",
      subSequenceNumber = 100,
      isLast = false)

    val res1 = dynamodbCommitter.add(localBatchId1, shardId1_1, shardInfo1_1)
    val res2 = dynamodbCommitter.add(localBatchId1, shardId1_2, shardInfo1_2)
    val res3 = dynamodbCommitter.add(localBatchId2, shardId2_1, shardInfo2_1)

    res1 shouldBe true
    res2 shouldBe true
    res3 shouldBe true

    val data1 = dynamodbCommitter.get(localBatchId1)
    val data2 = dynamodbCommitter.get(localBatchId2)
    val data3 = dynamodbCommitter.get(localBatchId2 + 100000)
    data1.length shouldBe 2
    data1 should contain allOf(shardInfo1_1, shardInfo1_2)
    data2.length shouldBe 1
    data2 should contain (shardInfo2_1)
    data3.length shouldBe 0

    val exists1 = dynamodbCommitter.exists(localBatchId1)
    val exists2 = dynamodbCommitter.exists(localBatchId2)
    val exists3 = dynamodbCommitter.exists(localBatchId2 + 100000)
    exists1 shouldBe true
    exists2 shouldBe true
    exists3 shouldBe false

    val delete1 = dynamodbCommitter.delete(localBatchId1)
    val delete2 = dynamodbCommitter.delete(localBatchId2)
    val delete3 = dynamodbCommitter.delete(localBatchId2 + 100000)
    delete1 shouldBe true
    delete2 shouldBe true
    delete3 shouldBe false

  }

  test("purge/delete from Dynamodb by batchId") {
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)

    val localBatchId1 = {
      batchId += 1; batchId
    }
    val localBatchId2 = {
      batchId += 1; batchId
    }
    val localBatchId3 = {
      batchId += 1;
      batchId
    }

    val shardId1_1 = "0000001"
    val shardId2_1 = "0000001"
    val shardId3_1 = "0000002"

    val shardInfo1_1 = ShardInfo(
      shardId = shardId1_1,
      iteratorType = AtTimeStamp.iteratorType,
      iteratorPosition = "1234567890",
      subSequenceNumber = NO_SUB_SEQUENCE_NUMBER,
      isLast = true)

    val shardInfo2_1 = ShardInfo(
      shardId = shardId2_1,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "opqrstuvxyz",
      subSequenceNumber = 100,
      isLast = false)

    val shardInfo3_1 = ShardInfo(
      shardId = shardId3_1,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "abcdefghijklmn",
      subSequenceNumber = 1000,
      isLast = true)

    val res1 = dynamodbCommitter.add(localBatchId1, shardId1_1, shardInfo1_1)
    val res2 = dynamodbCommitter.add(localBatchId2, shardId2_1, shardInfo2_1)
    val res3 = dynamodbCommitter.add(localBatchId3, shardId3_1, shardInfo3_1)

    res1 shouldBe true
    res2 shouldBe true
    res3 shouldBe true

    dynamodbCommitter.purge(1)

    val data1 = dynamodbCommitter.get(localBatchId1)
    val data2 = dynamodbCommitter.get(localBatchId2)
    val data3 = dynamodbCommitter.get(localBatchId3)
    data1.length shouldBe 0
    data2.length shouldBe 0
    data3.length shouldBe 1
    data3 should contain(shardInfo3_1)

    val delete1 = dynamodbCommitter.delete(localBatchId1)
    val delete2 = dynamodbCommitter.delete(localBatchId2)
    val delete3 = dynamodbCommitter.delete(localBatchId3)
    delete1 shouldBe false
    delete2 shouldBe false
    delete3 shouldBe true

  }

  test("get/delete from Dynamodb by batchId and shardId") {
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)

    val localBatchId1 = {
      batchId += 1; batchId
    }
    val localBatchId2 = {
      batchId += 1; batchId
    }

    val shardId1_1 = "0000001"
    val shardId1_2 = "0000002"

    val shardId2_1 = "0000001"
    val shardId2_2 = "0000002"
    val shardInfo1_1 = ShardInfo(
      shardId = shardId1_1,
      iteratorType = AtTimeStamp.iteratorType,
      iteratorPosition = "1234567890",
      subSequenceNumber = NO_SUB_SEQUENCE_NUMBER,
      isLast = true)

    val shardInfo1_2 = ShardInfo(
      shardId = shardId1_2,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "abcdefghijklmn",
      subSequenceNumber = 1000,
      isLast = true)

    val shardInfo2_1 = ShardInfo(
      shardId = shardId2_1,
      iteratorType = AtSequenceNumber.iteratorType,
      iteratorPosition = "opqrstuvxyz",
      subSequenceNumber = 100,
      isLast = false)

    val res1 = dynamodbCommitter.add(localBatchId1, shardId1_1, shardInfo1_1)
    val res2 = dynamodbCommitter.add(localBatchId1, shardId1_2, shardInfo1_2)
    val res3 = dynamodbCommitter.add(localBatchId2, shardId2_1, shardInfo2_1)

    res1 shouldBe true
    res2 shouldBe true
    res3 shouldBe true

    val data1 = dynamodbCommitter.get(localBatchId1, shardId1_1)
    val data2 = dynamodbCommitter.get(localBatchId1, shardId1_2)
    val data3 = dynamodbCommitter.get(localBatchId2, shardId2_1)
    val data4 = dynamodbCommitter.get(localBatchId2, shardId2_2)
    data1 shouldBe Some(shardInfo1_1)
    data2 shouldBe Some(shardInfo1_2)
    data3 shouldBe Some(shardInfo2_1)
    data4 shouldBe None


    val delete1 = dynamodbCommitter.delete(localBatchId1, shardId1_1)
    val delete2 = dynamodbCommitter.delete(localBatchId1, shardId1_2)
    val delete3 = dynamodbCommitter.delete(localBatchId2, shardId2_1)
    val delete4 = dynamodbCommitter.delete(localBatchId2, shardId2_2)
    delete1 shouldBe true
    delete2 shouldBe true
    delete3 shouldBe true
    delete4 shouldBe false

    val data21 = dynamodbCommitter.get(localBatchId1, shardId1_1)
    val data22 = dynamodbCommitter.get(localBatchId1, shardId1_2)
    val data23 = dynamodbCommitter.get(localBatchId2, shardId2_1)
    val data24 = dynamodbCommitter.get(localBatchId2, shardId2_2)
    data21 shouldBe None
    data22 shouldBe None
    data23 shouldBe None
    data24 shouldBe None
  }

  test("parallel add and delete from Dynamodb by batchId and shardId") {

    val POOL_SIZE = 30
    val LOOP_COUNT = 50
    val dynamodbCommitter = new DynamodbMetadataCommitter[ShardInfo](table, options)

    val executorService: ExecutorService = MoreExecutors.getExitingExecutorService(
                                          Executors.newFixedThreadPool(POOL_SIZE).asInstanceOf[ThreadPoolExecutor])

    val counter = new AtomicInteger(0)


    val localBatchId1 = {
      batchId += 1;
      batchId
    }
    val localBatchId2 = {
      batchId += 1;
      batchId
    }

    val shardId1_base = "0000001"
    val shardId2_base = "0000002"

    0 until POOL_SIZE foreach { i =>
      executorService.submit(
        new Runnable {
          def run(): Unit = {
            0 until LOOP_COUNT foreach { j =>
              val shardId1 = s"${shardId1_base}${counter.incrementAndGet()}"
              val shardId2 = s"${shardId2_base}${counter.incrementAndGet()}"

              val shardInfo1 = ShardInfo(
                shardId = shardId1,
                iteratorType = AtTimeStamp.iteratorType,
                iteratorPosition = "1234567890",
                subSequenceNumber = NO_SUB_SEQUENCE_NUMBER,
                isLast = true)

              val shardInfo2 = ShardInfo(
                shardId = shardId2,
                iteratorType = AtSequenceNumber.iteratorType,
                iteratorPosition = "abcdefghijklmn",
                subSequenceNumber = 1000,
                isLast = true)


              dynamodbCommitter.add(localBatchId1, shardId1, shardInfo1) shouldBe true
              dynamodbCommitter.add(localBatchId2, shardId2, shardInfo2) shouldBe true
              dynamodbCommitter.get(localBatchId1, shardId1) shouldBe Some(shardInfo1)
              dynamodbCommitter.get(localBatchId2, shardId2) shouldBe Some(shardInfo2)
            }
          }

        }
      )
    }

    executorService.awaitTermination(120, TimeUnit.SECONDS)
    dynamodbCommitter.delete(localBatchId1) shouldBe true
    dynamodbCommitter.delete(localBatchId2) shouldBe true
  }

}

object DynamodbMetadataCommitterItSuite {

  val TESTBASE_DEFAULT_REGION: String = "us-east-2"
  val TESTBASE_DEFAULT_STREAM_NAME: String = "teststream"
  val TESTBASE_DEFAULT_EFO_CONSUMER_NAME: String = "EFOTestConsumer" + randomAlphabetic(2)
  val TESTBASE_DEFAULT_ENDPOINT_URL: String = s"https://kinesis.${TESTBASE_DEFAULT_REGION}.amazonaws.com"
  val TESTBASE_DEFAULT_METADATA_PATH: String = "/test/path"
  val TESTBASE_DEFAULT_METADATA_DYNAMODB_TABLE: String = "test_table" + randomAlphabetic(2)

  val defaultPollingOptionMap = new CaseInsensitiveStringMap(Map(
    REGION -> TESTBASE_DEFAULT_REGION,
    ENDPOINT_URL -> TESTBASE_DEFAULT_ENDPOINT_URL,
    CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
    STREAM_NAME -> TESTBASE_DEFAULT_STREAM_NAME,
    DYNAMODB_TABLE_NAME -> TESTBASE_DEFAULT_METADATA_DYNAMODB_TABLE
  ).asJava)

  val defaultEfoOptionMap = new CaseInsensitiveStringMap(Map(
    REGION -> TESTBASE_DEFAULT_REGION,
    ENDPOINT_URL -> TESTBASE_DEFAULT_ENDPOINT_URL,
    CONSUMER_TYPE -> EFO_CONSUMER_TYPE,
    STREAM_NAME -> TESTBASE_DEFAULT_STREAM_NAME,
    CONSUMER_NAME -> TESTBASE_DEFAULT_EFO_CONSUMER_NAME,
    METADATA_PATH -> TESTBASE_DEFAULT_METADATA_PATH,
  ).asJava)
}

@IntegrationTestSuite
class EfoDynamodbMetadataCommitterItSuite
  extends DynamodbMetadataCommitterItSuite(defaultEfoOptionMap)

@IntegrationTestSuite
class PollingDynamodbMetadataCommitterItSuite
  extends DynamodbMetadataCommitterItSuite(defaultPollingOptionMap)
