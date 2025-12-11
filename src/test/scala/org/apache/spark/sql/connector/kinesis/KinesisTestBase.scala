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

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.services.kinesis.model.Shard

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.KinesisOptions._
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KinesisTestBase extends SparkFunSuite
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfter
  with Logging {

  protected val DEFAULT_TEST_REGION = "us-east-2"
  protected val DEFAULT_TEST_ENDPOINT_URL = s"https://kinesis.${DEFAULT_TEST_REGION}.amazonaws.com"
  protected val DEFAULT_TEST_CONSUMER_ARN = "consumerTestArn"
  protected val DEFAULT_TEST_STEAM_NAME = "streamTestName"
  protected val DEFAULT_TEST_SHARD: Shard = Shard.builder().shardId("shardId-000000000000").build
  protected val DEFAULT_TEST_TIMEOUT: Duration = Duration.ofSeconds(10)
  protected val DEFAULT_START_POSITION = new Latest()
  protected val DEFAULT_SEQUENCE = "1"
  protected val DEFAULT_SUB_SEQUENCE: Long = 1
  protected val DEFAULT_TIMESTAMP: String = "123456789"
  protected val DUMMY_SEQUENCE_NUMBER: SequenceNumber = SequenceNumber("dummy", -1, isLast = true)

  protected val DEFAULT_KINESIS_OPTIONS: KinesisOptions = KinesisOptions(new CaseInsensitiveStringMap(Map(
    REGION -> DEFAULT_TEST_REGION,
    ENDPOINT_URL -> DEFAULT_TEST_ENDPOINT_URL,
    CONSUMER_TYPE -> POLLING_CONSUMER_TYPE,
    STREAM_NAME -> DEFAULT_TEST_STEAM_NAME,
  ).asJava))

}
