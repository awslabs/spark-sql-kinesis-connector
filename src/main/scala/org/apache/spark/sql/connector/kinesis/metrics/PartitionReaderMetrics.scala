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
package org.apache.spark.sql.connector.kinesis.metrics

import scala.collection.JavaConverters._
import com.codahale.metrics.Counter
import com.codahale.metrics.MetricRegistry
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class PartitionReaderMetrics() {

  private val metricRegistry = new MetricRegistry

  private def getCounter(name: String): Counter = {
    metricRegistry.counter(MetricRegistry.name("PartitionReader", name))
  }

  val enqueueRecordCounter: Counter = getCounter("enqueueRecordCounter")
  val enqueueEmptyRecordCounter: Counter = getCounter("enqueueEmptyRecordCounter")
  val enqueueFailureCounter: Counter = getCounter("enqueueFailureCounter")
  val userRecordCounter: Counter = getCounter("userRecordCounter")
  val shardEndUserRecordCounter: Counter = getCounter("shardEndUserRecordCounter")
  val emptyUserRecordCounter: Counter = getCounter("emptyUserRecordCounter")
  val zeroLengthUserRecordCounter: Counter = getCounter("zeroLengthUserRecordCounter")


  def json: String = {
      Serialization.write(
        metricRegistry.getCounters.asScala.map { kv =>
          (kv._1, kv._2.getCount)
        }
      )(PartitionReaderMetrics.format)
  }
}

object PartitionReaderMetrics {
  val format = Serialization.formats(NoTypeHints)

  def apply(): PartitionReaderMetrics = {
    new PartitionReaderMetrics()
  }
}

