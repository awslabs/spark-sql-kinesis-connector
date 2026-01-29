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

import java.time.Instant
import java.time.ZonedDateTime
import java.time.ZoneId

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.AtTimeStamp
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitterFactory
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.util.StreamManualClock


abstract class KinesisSourceItSuite(aggregateTestData: Boolean,
                                    consumerType: String,
                                    metadataCommitterType: String
                                   )
  extends KinesisIntegrationTestBase(consumerType)
  with Matchers
  with Logging {

  import testImplicits._

  test("Kinesis connector reads from latest (default)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir


    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5),  // should not have 0
      StopStream
    )
  }

  test("LATEST can be used as Starting position") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir


    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)
                .option(KinesisOptions.STARTING_POSITION, Latest.iteratorType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData1 = 1 to 15
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1: _*), // should have 0
      StopStream
    )
  }

  test("TRIM_HORIZON can be used as Starting position") {
    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    try {
      val testData0 = 0 to 0
      localTestUtils.pushData(testData0.map(_.toString).toArray, aggregateTestData)

      // sleep for 1 s to avoid any concurrency issues
      Thread.sleep(1000.toLong)
      val clock = new StreamManualClock

      val checkpointDir = getRandomCheckpointDir

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.STARTING_POSITION, TrimHorizon.iteratorType)

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData1 = 1 to 5
      val testData2 = 11 to 20
      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        Execute { _ =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData0.toArray ++ testData1: _*), // should have 0
        StopStream,
        Execute { _ =>
          logInfo("Merging Shards")
          val (openShard, _) = localTestUtils.getShards().partition { shard =>
            shard.sequenceNumberRange().endingSequenceNumber() == null
          }
          val Seq(shardToMerge, adjShard) = openShard
          localTestUtils.mergeShard(shardToMerge.shardId(), adjShard.shardId())
          val (mergedOpenShards, mergedCloseShards) = localTestUtils.getShards().partition { shard =>
            shard.sequenceNumberRange().endingSequenceNumber() == null
          }
          assert(mergedCloseShards.size == 2)
          assert(mergedOpenShards.size == 1)

          Thread.sleep(reshardWaitTime)

          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
        },
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData0.toArray ++ testData1 ++ testData2: _*),
        StopStream
      )
    } finally {
      localTestUtils.deleteStream()
    }

  }


  test("EARLIEST can be used as Starting position") {
    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    try{
      val testData0 = 0 to 1
      localTestUtils.pushData(testData0.map(_.toString).toArray, aggregateTestData)

      // sleep for 1 s to avoid any concurrency issues
      Thread.sleep(1000.toLong)
      val clock = new StreamManualClock

      val checkpointDir = getRandomCheckpointDir

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.STARTING_POSITION, "EARLIEST")

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData1 = 1000 to 2000
      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        Execute { _ =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData0.toArray ++ testData1: _*), // should have 0
        StopStream
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  test("AT_TIMESTAMP can be used as Starting position") {
    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    try {
      val testData0 = 0 to 1
      localTestUtils.pushData(testData0.map(_.toString).toArray, aggregateTestData)

      // sleep for 1s to avoid any concurrency issues
      Thread.sleep(1000.toLong)
      
      val instant = Instant.ofEpochMilli(System.currentTimeMillis())
      val zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")).toString.substring(0, 19) + "Z"
      logInfo(s"zonedDateTime String is ${zonedDateTime}")
      
      val clock = new StreamManualClock

      val checkpointDir = getRandomCheckpointDir

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.STARTING_POSITION, s"${AtTimeStamp.iteratorType} ${zonedDateTime}")

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData1 = 1 to 50
      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        Execute { _ =>
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData1: _*), // shouldn't have 0
        StopStream
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  test("Kinesis connector restore from previous checkpoint (committed)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    val testData2 = 6 to 11
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5),
      StopStream,
      Execute { _ =>
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
      },
      // should be able to resume from the checkpoint
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(6, 7, 8, 9, 10, 11),
      StopStream
    )
  }

  test("Kinesis connector restore from previous checkpoint (uncommitted)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    val testData2 = 6 to 11
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5),
      StopStream,
      Execute { query =>
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)

        // remove the commit marker so that the previous batch is not committed
        query.commitLog.purgeAfter(0)

        // clear sink memory so that it doesn't remember previous batch
        query.sink.asInstanceOf[MemorySink].clear()
      },
      // should be able to rerun previous batch
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5), // Only read the data already processed in previous batch
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(6, 7, 8, 9, 10, 11),
      StopStream
    )
  }

  test("Kinesis connector restore from previous checkpoint (uncommitted and entire metadata missing)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    val testData2 = 6 to 11
    val testData3 = 20 to 21
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 4, 5),
      Execute { _ =>
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(6, 7, 8, 9, 10, 11),
      StopStream,
      Execute { query =>
        testUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)

        // remove the commit marker so that the previous batch is not committed
        query.commitLog.purgeAfter(1)

        val committer = metadataCommitterType match {
          case DYNAMODB_COMMITTER_TYPE =>
            MetadataCommitterFactory.createMetadataCommitter(
              defaultKinesisOptions.copy(
                metadataCommitterType = DYNAMODB_COMMITTER_TYPE,
                dynamodbTableName = currentDynamoTableName.getOrElse(
                defaultKinesisOptions.dynamodbTableName)
              ),
              s"${checkpointDir}/sources/0/"
            )
          case _ =>
            MetadataCommitterFactory.createMetadataCommitter(
              defaultKinesisOptions.copy(metadataCommitterType = HDFS_COMMITTER_TYPE),
              s"${checkpointDir}/sources/0/"
            )
        }
        committer.delete(2)

        // clear sink memory so that it doesn't remember previous batch
        query.sink.asInstanceOf[MemorySink].clear()
      },
      // should be able to restart from previous batch's checkpoint and include the new data
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckAnswer(6, 7, 8, 9, 10, 11, 20, 21),
      StopStream
    )
  }

  test("Kinesis connector restore from previous checkpoint (uncommitted and partial metadata missing)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData = 1 to 5
    val testData2_1 = 6 to 7
    val testData2_2 = 8 to 11
    val testData3 = 20 to 21
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData: _*),
      Execute { _ =>
        testUtils.pushData(testData2_1.map(_.toString).toArray, aggregateTestData, Some("6"))
        testUtils.pushData(testData2_2.map(_.toString).toArray, aggregateTestData, Some("7"))
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(testData2_1.toArray ++ testData2_2: _*),
      StopStream,
      Execute { query =>
        testUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)

        // remove the commit marker so that the previous batch is not committed
        query.commitLog.purgeAfter(1)

        val committer = metadataCommitterType match {
          case DYNAMODB_COMMITTER_TYPE =>
            MetadataCommitterFactory.createMetadataCommitter(
              defaultKinesisOptions.copy(
                metadataCommitterType = DYNAMODB_COMMITTER_TYPE,
                dynamodbTableName = currentDynamoTableName.getOrElse(
                defaultKinesisOptions.dynamodbTableName)
              ),
              s"${checkpointDir}/sources/0/"
            )
          case _ =>
            MetadataCommitterFactory.createMetadataCommitter(
              defaultKinesisOptions.copy(metadataCommitterType = HDFS_COMMITTER_TYPE),
              s"${checkpointDir}/sources/0/"
            )
        }
        committer.delete(2, "shardId-000000000000")


        // clear sink memory so that it doesn't remember previous batch
        query.sink.asInstanceOf[MemorySink].clear()
      },
      // should be able to restart from previous batch's checkpoint
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData2_2: _*), // only read the data from the shards existing in sources/0/.../2/
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(testData2_1.toArray ++ testData3: _*),
      StopStream
    )
  }

  test("Kinesis connector restore from previous checkpoint (no new event including emtpy event in KDS)") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData1 = 1 to 5
    val testData2 = 6 to 10
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData, Some("1"))
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray: _*),
      Execute { _ =>
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData, Some("1"))
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(testData2.toArray: _*),
      StopStream,

      // All start/stop Should be successful
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(Array[Int](): _*),
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(Array[Int](): _*),
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(Array[Int](): _*),
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(Array[Int](): _*),
      StopStream
    )
  }

  test("Kinesis connector reads based on MAX_FETCH_RECORDS_PER_SHARD") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)
      .option(KinesisOptions.MAX_FETCH_RECORDS_PER_SHARD, "3")
      .option(KinesisOptions.DATA_QUEUE_CAPACITY, "1")

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData1 = 1 to 10
    val testData2 = 11 to 20
    val testData3 = 21 to 23
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData, Some("1"))
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData, Some("6"))
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(1, 2, 3, 11, 12, 13),  // first batch
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(4, 5, 6, 14, 15, 16),  // second batch
      StopStream,
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(7, 8, 9, 17, 18, 19), // only read the data from the shards existing in sources/0/.../2/
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(10, 20),
      Execute { _ =>
        testUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray ++ testData2 ++ testData3: _*),
      StopStream
    )
  }

  test("Kinesis connector reads based on MAX_FETCH_TIME_PER_SHARD_SEC") {
    testUtils.pushData(Array("0"), aggregateTestData)

    // sleep for 1s to avoid any concurrency issues
    Thread.sleep(1000.toLong)
    val clock = new StreamManualClock

    var startTime = System.currentTimeMillis
    val maxFetchTime = 30
    
    val checkpointDir = getRandomCheckpointDir

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)
      .option(KinesisOptions.MAX_FETCH_RECORDS_PER_SHARD, "1000000") // a big number can never reach in this test
      .option(KinesisOptions.MAX_DATA_QUEUE_EMPTY_COUNT, "100") // a big number can never reach in this test
      .option(KinesisOptions.MAX_FETCH_TIME_PER_SHARD_SEC, maxFetchTime.toString)
      .option(KinesisOptions.DATA_QUEUE_CAPACITY, "1")

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)
    val testData1 = 1 to 5
    val testData2 = 6 to 10
    val testData3 = 11 to 15
    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { _ =>
        testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData, Some("1"))
        startTime = System.currentTimeMillis
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      AssertOnQuery { query =>
        val elapsedTime = System.currentTimeMillis - startTime
        (maxFetchTime * 1000) <= elapsedTime && elapsedTime <= (maxFetchTime * 3 * 1000)
      },
      CheckAnswer(testData1.toArray: _*),
      Execute { _ =>
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData, Some("1"))
        Future {
          Thread.sleep(maxFetchTime * 1000 + 5000)
          testUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData, Some("1"))
        }
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(testData2.toArray: _*),
      StopStream,
      Execute { _ =>
        Thread.sleep(10000L)
      },
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(testData3.toArray: _*),
      StopStream
    )
  }
  
  // ignore avoidEmptyBatch related tests. Empty events generated by Kinesis can move batchId forward, even avoidEmptyBatch is true
  ignore("avoidEmptyBatch is enabled by default") {
    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()
    try {
      val clock = new StreamManualClock
      val checkpointDir = getRandomCheckpointDir
      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData = 6 to 10
      val testData2 = 11 to 20

      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        Execute { _ =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData: _*),
        AssertOnQuery { query =>
          query.lastExecution.currentBatchId == 1
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          currentBatchId == 1
        },
        Execute { _ =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(testData2: _*),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          query.lastExecution.currentBatchId == 2
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          currentBatchId == 2
        },
        StopStream
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  ignore("When avoidEmptyBatch is disabled") {

    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()
    try {
      val clock = new StreamManualClock
      val checkpointDir = getRandomCheckpointDir

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.AVOID_EMPTY_BATCHES, "false")

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)
      val testData = 6 to 10
      val testData2 = 11 to 20

      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        AssertOnQuery { _ =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData.map(_.toString).toArray, aggregateTestData)
          true
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData: _*),
        AssertOnQuery { query =>
          query.lastExecution.currentBatchId == 1
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          currentBatchId == 3
        },
        Execute { _ =>
          logInfo("Push Data ")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(testData2: _*),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          query.lastExecution.currentBatchId == 4
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        AssertOnQuery { query =>
          val currentBatchId = query.lastExecution.currentBatchId
          logInfo(s"currentBatchId is ${currentBatchId}")
          currentBatchId == 6
        },
        StopStream
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  test("output data should have valid Kinesis Schema ") {

    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    val checkpointDir = getRandomCheckpointDir

    try {
      val now = System.currentTimeMillis()
      localTestUtils.pushData(Array(1).map(_.toString), aggregateTestData, Some("1"))
      Thread.sleep(1000.toLong)

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.STARTING_POSITION, TrimHorizon.iteratorType)

      val kinesis = reader.load()
      assert(kinesis.schema == KinesisClientFactory.kinesisSchema)

      val result = kinesis.selectExpr("CAST(data AS STRING)", "streamName",
        "partitionKey", "sequenceNumber", "CAST(approximateArrivalTimestamp AS TIMESTAMP)")
        .as[(String, String, String, String, Long)]

      val query = result.writeStream
        .format("memory")
        .queryName("schematest")
        .start()

      query.processAllAvailable()


      val rows = spark.table("schematest").collect()
      assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
      val row = rows(0)

      // We cannot check the exact event time. Checking using some low bound
      assert(
        row.getAs[java.sql.Timestamp]("approximateArrivalTimestamp").getTime >= now - 5 * 1000,
        s"Unexpected results: $row")

      assert(row.getAs[String]("streamName") === localTestUtils.streamName, s"Unexpected results: $row")
      assert(row.getAs[String]("partitionKey") === "1", s"Unexpected results: $row")
      query.stop()
    } finally {
      localTestUtils.deleteStream()
    }
  }
}

@IntegrationTestSuite
class NoAggEfoKinesisSourceHDFSItSuite
  extends KinesisSourceItSuite(aggregateTestData = false,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class NoAggEfoKinesisSourceDDBItSuite
  extends KinesisSourceItSuite(aggregateTestData = false,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggEfoKinesisSourceHDFSItSuite
  extends KinesisSourceItSuite(aggregateTestData = true,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )
@IntegrationTestSuite
class AggEfoKinesisSourceDDBItSuite
  extends KinesisSourceItSuite(aggregateTestData = true,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )

@IntegrationTestSuite
class NoAggPollingKinesisSourceHDFSItSuite
  extends KinesisSourceItSuite(aggregateTestData = false,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )
@IntegrationTestSuite
class NoAggPollingKinesisSourceDDBItSuite
  extends KinesisSourceItSuite(aggregateTestData = false,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )
@IntegrationTestSuite
class AggPollingKinesisSourceHDFSItSuite
  extends KinesisSourceItSuite(aggregateTestData = true,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggPollingKinesisSourceDDBItSuite
  extends KinesisSourceItSuite(aggregateTestData = true,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )
