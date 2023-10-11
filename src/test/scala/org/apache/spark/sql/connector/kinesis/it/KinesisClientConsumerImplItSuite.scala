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

import org.scalatest.matchers.should.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumerImpl

@IntegrationTestSuite
class KinesisClientConsumerImplItSuite extends KinesisIntegrationTestBase(EFO_CONSUMER_TYPE)
  with Matchers
  with Logging {

  test("get all shards for more than maxResultListShardsPerCall") {
    val numberOfShards = 3
    val localTestUtils = new EnhancedKinesisTestUtils(numberOfShards)
    localTestUtils.createStream()

    val kinesisReader = KinesisClientConsumerImpl(
      defaultKinesisOptions.copy(maxResultListShardsPerCall = 1),
      localTestUtils.streamName,
      localTestUtils.endpointUrl
    )

    try {
      val shards = kinesisReader.getShards

      shards.map(_.shardId()) should contain theSameElementsAs (List(
        "shardId-000000000000", "shardId-000000000001", "shardId-000000000002"
      ))

    } finally {
      kinesisReader.close()
      localTestUtils.deleteStream()
    }

  }
}
