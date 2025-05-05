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

package org.apache.spark.sql.connector.kinesis

import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange
import software.amazon.awssdk.services.kinesis.model.Shard

import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER

class ShardSyncerSuite extends KinesisTestBase  {

  val latestShards: Seq[Shard] = Seq(createShard("shard1", "1"))
  val prevShardInfo: Seq[ShardInfo] = Seq(ShardInfo("shard0",
    AfterSequenceNumber.iteratorType,
    "0",
    NO_SUB_SEQUENCE_NUMBER,
    isLast = true
  ))

  test("Should error out when failondataloss is true and a shard is deleted") {
    intercept[ IllegalStateException ] {
      ShardSyncer.getLatestShardInfo(latestShards, prevShardInfo,
        InitialKinesisPosition.fromPredefPosition(new TrimHorizon),
        failOnDataLoss = true)
    }
  }

  test("Should continue failondataloss is false and a shard is deleted") {
    val latest: Seq[ShardInfo] = ShardSyncer.getLatestShardInfo(
      latestShards, prevShardInfo, InitialKinesisPosition.fromPredefPosition(new TrimHorizon))
    assert(latest.nonEmpty)
    assert(latest.head.shardId === "shard1")
    assert(latest.head.iteratorType === new TrimHorizon().iteratorType )
  }

  test("Should handle a mix of open and closed shards") {
    val closedShard = Shard.builder()
      .shardId("shard1")
      .sequenceNumberRange(
        SequenceNumberRange.builder()
          .startingSequenceNumber("1")
          .endingSequenceNumber("100")
          .build
      )
      .build

    val openShard = Shard.builder()
      .shardId("shard2")
      .sequenceNumberRange(
        SequenceNumberRange.builder()
          .startingSequenceNumber("101")
          .build
      )
      .build


    val mixedShards = Seq(closedShard, openShard)

    val result = ShardSyncer.getLatestShardInfo(
      mixedShards,
      prevShardInfo,
      InitialKinesisPosition.fromPredefPosition(new TrimHorizon)
    )

    assert(result.length === 2)
    assert(result.map(_.shardId).toSet === Set("shard1", "shard2"))

    // Test open shard (shard2) properties
    val openShardInfo = result.find(_.shardId == "shard2").get
    assert(openShardInfo.shardId === "shard2")
    assert(openShardInfo.iteratorType === new TrimHorizon().iteratorType)
    assert(openShardInfo.subSequenceNumber === NO_SUB_SEQUENCE_NUMBER)
    assert(openShardInfo.isLast)


    // Test closed shard (shard1) properties
    val closedShardInfo = result.find(_.shardId == "shard1").get
    assert(closedShardInfo.shardId === "shard1")
    assert(closedShardInfo.iteratorType === new ShardEnd().iteratorType)
    assert(closedShardInfo.subSequenceNumber === NO_SUB_SEQUENCE_NUMBER)
    assert(closedShardInfo.isLast) // Should be last because it's a closed shard
  }

  private def createShard(shardId: String, seqNum: String): Shard = {
    Shard.builder()
      .shardId(shardId)
      .sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber(seqNum).build
      )
      .build
  }

}
