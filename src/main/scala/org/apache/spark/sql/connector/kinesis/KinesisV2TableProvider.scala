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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KinesisV2TableProvider extends Logging
  with SimpleTableProvider
  with StreamSinkProvider
  with DataSourceRegister {

  override def shortName(): String = AWS_KINESIS_SHORT_NAME

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new KinesisV2Table(options)
  }

  private def validateSinkOptions(caseInsensitiveParams: Map[String, String]): Unit = {

    val streamNameKey = KinesisOptions.STREAM_NAME.toLowerCase(Locale.ROOT)
    if (!caseInsensitiveParams.contains(streamNameKey) ||
      caseInsensitiveParams(streamNameKey).isEmpty) {
      throw new IllegalArgumentException(
        s"${KinesisOptions.STREAM_NAME} is a required field")
    }

    val regioneKey = KinesisOptions.REGION.toLowerCase(Locale.ROOT)
    if (!caseInsensitiveParams.contains(regioneKey) ||
      caseInsensitiveParams(regioneKey).isEmpty) {
      throw new IllegalArgumentException(
        s"${KinesisOptions.REGION} is a required field")
    }

    val endpointUrlKey = KinesisOptions.ENDPOINT_URL.toLowerCase(Locale.ROOT)
    if (!caseInsensitiveParams.contains(endpointUrlKey) ||
      caseInsensitiveParams(endpointUrlKey).isEmpty) {
      throw new IllegalArgumentException(
        s"${KinesisOptions.ENDPOINT_URL} is a required field")
    }
  }

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {

    if (outputMode != OutputMode.Append()) {
      throw new IllegalStateException(s"OutputMode ${outputMode} not supported.")
    }

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateSinkOptions(caseInsensitiveParams)

    new KinesisSink(sqlContext, caseInsensitiveParams)
  }
}

object KinesisV2TableProvider {
  val AWS_KINESIS_SHORT_NAME = "aws-kinesis"
}
