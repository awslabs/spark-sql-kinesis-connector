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

  private def createShard(shardId: String, seqNum: String): Shard = {
    Shard.builder()
      .shardId(shardId)
      .sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber(seqNum).build
      )
      .build
  }

}
