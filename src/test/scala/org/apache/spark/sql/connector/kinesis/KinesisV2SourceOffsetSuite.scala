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

import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.read.streaming.Offset


class KinesisV2SourceOffsetSuite extends KinesisTestBase {

  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparison $one <=> $two") {
      assert(one == one)
      assert(two == two)
      assert(one != two)
      assert(two != one)
    }
  }

  compare(
    one = KinesisV2SourceOffset(new ShardOffsets(-1L, "dummy", Array.empty[ShardInfo])),
    two = KinesisV2SourceOffset(new ShardOffsets(1L, "dummy", Array.empty[ShardInfo])))

  compare(
    one = KinesisV2SourceOffset(new ShardOffsets(1L, "foo", Array.empty[ShardInfo])),
    two = KinesisV2SourceOffset(new ShardOffsets(1L, "bar", Array.empty[ShardInfo]))
  )

  compare(
    one = KinesisV2SourceOffset(new ShardOffsets(1L, "foo", Array(
      new ShardInfo("shard-001", new TrimHorizon())))),
    two = KinesisV2SourceOffset(new ShardOffsets(1L, "foo",
      Array(new ShardInfo("shard-001", new TrimHorizon()),
        new ShardInfo("shard-002", new TrimHorizon()) )))
  )
  var shardInfo1 = Array.empty[ShardInfo]
  shardInfo1 = shardInfo1 ++ Array(ShardInfo("shard-001",
    AfterSequenceNumber.iteratorType,
    "1234",
    NO_SUB_SEQUENCE_NUMBER,
    isLast = true
  ))

  val kso1 = KinesisV2SourceOffset(
    new ShardOffsets(1L, "foo", shardInfo1))

  val shardInfo2 = shardInfo1 ++ Array(ShardInfo("shard-002",
    TrimHorizon.iteratorType,
    "",
    NO_SUB_SEQUENCE_NUMBER,
    isLast = true
  ))

  val kso2 = KinesisV2SourceOffset(
    new ShardOffsets(1L, "bar", shardInfo2))

  val shardInfo3 = shardInfo2 ++ Array(ShardInfo("shard-003",
    AfterSequenceNumber.iteratorType,
    "2342",
    NO_SUB_SEQUENCE_NUMBER,
    isLast = true
  ))
  val kso3 = KinesisV2SourceOffset(
    new ShardOffsets(1L, "bar", shardInfo3)
  )

  compare(KinesisV2SourceOffset(kso1.json), kso2)

  test("basic serialization - deserialization") {
    assert(KinesisV2SourceOffset.getShardOffsets(kso1) ==
      KinesisV2SourceOffset.getShardOffsets(KinesisV2SourceOffset(kso1.json)))
  }


}

