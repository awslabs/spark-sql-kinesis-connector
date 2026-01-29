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

import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions.DYNAMODB_COMMITTER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisOptions.HDFS_COMMITTER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisOptions.POLLING_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.Latest
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitterFactory
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.util.StreamManualClock


abstract class KinesisSourceReshardItSuite(aggregateTestData: Boolean,
                                           consumerType: String,
                                           metadataCommitterType: String
                                          )
  extends KinesisIntegrationTestBase(consumerType, 1)
  with Matchers
  with Logging {

  import testImplicits._

  test("split and merge shards in a stream") {

    val checkpointDir = getRandomCheckpointDir

    val clock = new StreamManualClock

    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, metadataCommitterType)
                  .option(KinesisOptions.DESCRIBE_SHARD_INTERVAL, "0")
                  .option(KinesisOptions.STARTING_POSITION, Latest.iteratorType)

    val kinesis = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]
    val result = kinesis.map(_.toInt)

    val testData1 = 1 to 10
    val testData2 = 11 to 20
    val testData3 = 21 to 2000
    val testData4 = 2001 to 3000
    val testData5 = 3001 to 5000

    testStream(result)(
      StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
      waitUntilBatchProcessed(clock),
      Execute { query =>
        logInfo("Push testData1 ")
        testUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1: _*),
      Execute { query =>
        logInfo("Splitting Shards")
        val shardToSplit = testUtils.getShards().head
        testUtils.splitShard(shardToSplit.shardId())
        val (splitOpenShards, splitCloseShards) = testUtils.getShards().partition { shard =>
          shard.sequenceNumberRange().endingSequenceNumber() == null
        }
        logInfo(s"splitCloseShards ${splitCloseShards}, splitOpenShards ${splitOpenShards}")
        // We should have one closed shard and two open shards
        assert(splitCloseShards.size == 1)
        assert(splitOpenShards.size == 2)

        Thread.sleep(reshardWaitTime)
      },
      Execute { query =>
        logInfo("Push testData2 ")
        testUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray ++ testData2: _*),
      Execute { query =>
        logInfo("Push testData3 ")
        testUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray ++ testData2 ++ testData3 : _*),
      Execute { query =>
        logInfo("Merging Shards")
        val (openShard, closeShard) = testUtils.getShards().partition { shard =>
          shard.sequenceNumberRange().endingSequenceNumber() == null
        }
        val Seq(shardToMerge, adjShard) = openShard
        testUtils.mergeShard(shardToMerge.shardId(), adjShard.shardId())
        val (mergedOpenShards, mergedCloseShards) = testUtils.getShards().partition
        { shard =>
          shard.sequenceNumberRange().endingSequenceNumber() == null
        }
        logInfo(s"mergedCloseShards ${mergedCloseShards}, mergedOpenShards ${mergedOpenShards}")
        // We should have three closed shards and one open shard
        assert(mergedCloseShards.size == 3)
        assert(mergedOpenShards.size == 1)

        Thread.sleep(reshardWaitTime)
      },
      Execute { query =>
        logInfo("Push testData4 ")
        testUtils.pushData(testData4.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray ++ testData2 ++ testData3 ++ testData4: _*),
      Execute { query =>
        logInfo("Push testData5 ")
        testUtils.pushData(testData5.map(_.toString).toArray, aggregateTestData)
      },
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(testData1.toArray ++ testData2 ++ testData3 ++ testData4 ++ testData5: _*),
      AssertOnQuery { q =>

        // ensure that old shard is not saved anymore
        val committer = MetadataCommitterFactory.createMetadataCommitter(
          defaultKinesisOptions.copy(dynamodbTableName = currentDynamoTableName.getOrElse(
            defaultKinesisOptions.dynamodbTableName)
          ),
          s"${checkpointDir}/sources/0/"
        )
        committer.get(5, "shardId-000000000000").isEmpty
      },
    )
  }

  test("Read from TRIM_HORIZON after reshards") {
    val localTestUtils = new EnhancedKinesisTestUtils(1)
    localTestUtils.createStream()
    try {
      val checkpointDir = getRandomCheckpointDir

      val clock = new StreamManualClock

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.DESCRIBE_SHARD_INTERVAL, "0")
        .option(KinesisOptions.STARTING_POSITION, TrimHorizon.iteratorType)

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)

      val testData1 = 1 to 10
      val testData2 = 11 to 20
      val testData3 = 21 to 2000
      val testData4 = 2001 to 3000
      val testData5 = 3001 to 5000

      logInfo("Push testData1 ")
      localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)

      logInfo("Push testData2 ")
      localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)

      logInfo("Splitting Shards")
      val shardToSplit = localTestUtils.getShards().head
      localTestUtils.splitShard(shardToSplit.shardId())
      val (splitOpenShards, splitCloseShards) = localTestUtils.getShards().partition { shard =>
        shard.sequenceNumberRange().endingSequenceNumber() == null
      }
      logInfo(s"splitCloseShards ${splitCloseShards}, splitOpenShards ${splitOpenShards}")
      // We should have one closed shard and two open shards
      assert(splitCloseShards.size == 1)
      assert(splitOpenShards.size == 2)

      Thread.sleep(reshardWaitTime)

      logInfo("Push testData3 ")
      localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)

      logInfo("Merging Shards")
      val (openShard, closeShard) = localTestUtils.getShards().partition { shard =>
        shard.sequenceNumberRange().endingSequenceNumber() == null
      }
      val Seq(shardToMerge, adjShard) = openShard
      localTestUtils.mergeShard(shardToMerge.shardId(), adjShard.shardId())
      val (mergedOpenShards, mergedCloseShards) = localTestUtils.getShards().partition { shard =>
        shard.sequenceNumberRange().endingSequenceNumber() == null
      }
      logInfo(s"mergedCloseShards ${mergedCloseShards}, mergedOpenShards ${mergedOpenShards}")
      // We should have three closed shards and one open shard
      assert(mergedCloseShards.size == 3)
      assert(mergedOpenShards.size == 1)

      Thread.sleep(reshardWaitTime)

      logInfo("Push testData4 ")
      localTestUtils.pushData(testData4.map(_.toString).toArray, aggregateTestData)

      logInfo("Push testData5 ")
      localTestUtils.pushData(testData5.map(_.toString).toArray, aggregateTestData)


      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData1.toArray ++ testData2 ++ testData3 ++ testData4 ++ testData5: _*)
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

  test("reshard and stream read run in parallel") {
    val localTestUtils = new EnhancedKinesisTestUtils(1)
    localTestUtils.createStream()
    try {
      val checkpointDir = getRandomCheckpointDir

      val clock = new StreamManualClock

      val reader = createSparkReadStream(consumerType, localTestUtils, checkpointDir, metadataCommitterType)
        .option(KinesisOptions.DESCRIBE_SHARD_INTERVAL, "0")
        .option(KinesisOptions.STARTING_POSITION, TrimHorizon.iteratorType)

      val kinesis = reader.load()
        .selectExpr("CAST(data AS STRING)")
        .as[String]
      val result = kinesis.map(_.toInt)

      val testData1 = 1 to 10
      val testData2 = 11 to 20
      val testData3 = 21 to 2000
      val testData4 = 2001 to 3000
      val testData5 = 3001 to 5000

      val thread = new Thread {
        override def run(): Unit = {
          val localTestUtils2 = new EnhancedKinesisTestUtils(1)
          localTestUtils2.setStreamName(localTestUtils.streamName)
          Thread.sleep(reshardWaitTime * 5)

          1 to 10 foreach { i =>
            logInfo(s"Round ${i} Splitting Shards")
            val shardToSplit = localTestUtils2.getShards().last
            localTestUtils2.splitShard(shardToSplit.shardId())
            val (splitOpenShards, splitCloseShards) = localTestUtils2.getShards().partition { shard =>
              shard.sequenceNumberRange().endingSequenceNumber() == null
            }
            logInfo(s"Round ${i} splitCloseShards ${splitCloseShards}, splitOpenShards ${splitOpenShards}")

            Thread.sleep(reshardWaitTime * 5)

            logInfo(s"Round ${i} Merging Shards")
            val (openShard, closeShard) = localTestUtils2.getShards().partition { shard =>
              shard.sequenceNumberRange().endingSequenceNumber() == null
            }
            val Seq(shardToMerge, adjShard) = openShard.take(2)
            localTestUtils2.mergeShard(shardToMerge.shardId(), adjShard.shardId())
            val (mergedOpenShards, mergedCloseShards) = localTestUtils2.getShards().partition { shard =>
              shard.sequenceNumberRange().endingSequenceNumber() == null
            }
            logInfo(s"Round ${i} mergedCloseShards ${mergedCloseShards}, mergedOpenShards ${mergedOpenShards}")

            Thread.sleep(reshardWaitTime * 5)
          }


        }
      }
      thread.setDaemon(true)
      thread.start()


      testStream(result)(
        StartStream(Trigger.ProcessingTime(100), clock, Map.empty, checkpointDir),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Push testData1 ")
          localTestUtils.pushData(testData1.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Push testData2 ")
          localTestUtils.pushData(testData2.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Push testData3 ")
          localTestUtils.pushData(testData3.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Push testData4 ")
          localTestUtils.pushData(testData4.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Push testData5 ")
          localTestUtils.pushData(testData5.map(_.toString).toArray, aggregateTestData)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(testData1.toArray ++ testData2 ++ testData3 ++ testData4 ++ testData5: _*),
      )
    } finally {
      localTestUtils.deleteStream()
    }
  }

}

@IntegrationTestSuite
class NoAggEfoKinesisSourceReshardHDFSItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = false,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class NoAggEfoKinesisSourceReshardDDBItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = false,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggEfoKinesisSourceReshardHDFSItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = true,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggEfoKinesisSourceReshardDDBItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = true,
    consumerType = EFO_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )

@IntegrationTestSuite
class NoAggPollingKinesisSourceReshardHDFSItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = false,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class NoAggPollingKinesisSourceReshardDDBItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = false,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggPollingKinesisSourceReshardHDFSItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = true,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = HDFS_COMMITTER_TYPE
  )

@IntegrationTestSuite
class AggPollingKinesisSourceReshardDDBItSuite
  extends KinesisSourceReshardItSuite(aggregateTestData = true,
    consumerType = POLLING_CONSUMER_TYPE,
    metadataCommitterType = DYNAMODB_COMMITTER_TYPE
  )
