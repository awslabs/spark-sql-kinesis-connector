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

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.connector.kinesis.KinesisV2PartitionReader.shouldStopWhenCaughtUp

class KinesisV2PartitionReaderSuite extends KinesisTestBase {

  test("stops when enabled, caught up, on the last sub-record and nothing is queued") {
    shouldStopWhenCaughtUp(enabled = true, millisBehindLatest = 0,
      isLastSubSequence = true, queuedRecords = 0) shouldBe true
  }

  test("default (disabled) never stops early") {
    shouldStopWhenCaughtUp(enabled = false, millisBehindLatest = 0,
      isLastSubSequence = true, queuedRecords = 0) shouldBe false
  }

  test("keeps reading while behind the tip") {
    shouldStopWhenCaughtUp(enabled = true, millisBehindLatest = 1,
      isLastSubSequence = true, queuedRecords = 0) shouldBe false
  }

  test("keeps reading while records are still queued") {
    shouldStopWhenCaughtUp(enabled = true, millisBehindLatest = 0,
      isLastSubSequence = true, queuedRecords = 3) shouldBe false
  }

  test("keeps reading until the last sub-record of an aggregated record") {
    shouldStopWhenCaughtUp(enabled = true, millisBehindLatest = 0,
      isLastSubSequence = false, queuedRecords = 0) shouldBe false
  }
}
