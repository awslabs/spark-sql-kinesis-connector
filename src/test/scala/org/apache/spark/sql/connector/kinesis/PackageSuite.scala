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

import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService

import scala.collection.mutable.ArrayBuffer

import io.netty.handler.timeout.ReadTimeoutException
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoSubscriberException

class PackageSuite extends KinesisTestBase {
  test("throwableIsCausedBy can find wrapped exceptions") {

    val rootCauseException = new EfoSubscriberException(new Exception(("test")))
    val exception1_1 = new ExecutionException(rootCauseException)
    val exception1_2 = new RuntimeException(exception1_1)

    val rootCauseException2 = new ReadTimeoutException()
    val exception2_1 = new ExecutionException(rootCauseException2)

    throwableIsCausedBy[EfoSubscriberException](rootCauseException) shouldBe true
    throwableIsCausedBy[EfoSubscriberException](exception1_1) shouldBe true
    throwableIsCausedBy[EfoSubscriberException](exception1_2) shouldBe true
    throwableIsCausedBy[ExecutionException](exception1_1) shouldBe true
    throwableIsCausedBy[ExecutionException](exception1_2) shouldBe true
    throwableIsCausedBy[ReadTimeoutException](rootCauseException2) shouldBe true
    throwableIsCausedBy[ReadTimeoutException](exception2_1) shouldBe true
    throwableIsCausedBy[EfoSubscriberException](rootCauseException2) shouldBe false
    throwableIsCausedBy[EfoSubscriberException](exception2_1) shouldBe false

  }

  test("getFixedThreadPoolFromCache can remove the least recently used") {

    val firstCached = ArrayBuffer.empty[ExecutorService]

    (0 until DEFAULT_THREAD_POOL_CACHE_MAXSIZE) foreach {i =>
      val s = getFixedUninterruptibleThreadPoolFromCache(s"test${i}", 10)
      firstCached += s
    }

    (0 until DEFAULT_THREAD_POOL_CACHE_MAXSIZE) foreach { i =>
      assert(firstCached(i) eq getFixedUninterruptibleThreadPoolFromCache(s"test${i}", 10))
    }

    // add one more so that "test0" is removed
    getFixedUninterruptibleThreadPoolFromCache(s"test${DEFAULT_THREAD_POOL_CACHE_MAXSIZE}", 10)

    // get a new instance of "test0" and "test1" is removed when add new "test0"
    assert(firstCached(0) ne getFixedUninterruptibleThreadPoolFromCache(s"test0", 10))

    // the rest should be still in cache starting from "test2"
    (2 until DEFAULT_THREAD_POOL_CACHE_MAXSIZE) foreach { i =>
      assert(firstCached(i) eq getFixedUninterruptibleThreadPoolFromCache(s"test${i}", 10))
    }
  }
  
  test("getTimestampValue returns unix epoch") {
    getTimestampValue("2023-08-30T19:00:05Z") should equal (1693422005000L)
    getTimestampValue("2023-08-30T19:00:05+05:30") should equal (1693402205000L)
    getTimestampValue("2023-08-30T19:00:05-08:00") should equal (1693450805000L)
  }
}
