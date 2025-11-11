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

import java.util
import java.util.{HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KinesisV2Table (tableOptions: CaseInsensitiveStringMap)
  extends Table with SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedOptions = new JHashMap[String, String](tableOptions.asCaseSensitiveMap())
    mergedOptions.putAll(options.asCaseSensitiveMap())
    new KinesisV2ScanBuilder(schema(), KinesisOptions(new CaseInsensitiveStringMap(mergedOptions)))
  }

  override def name(): String = "KinesisV2Table"

  override def schema(): StructType = KinesisClientFactory.kinesisSchema

  override def capabilities(): util.Set[TableCapability] = Set[TableCapability](
    TableCapability.MICRO_BATCH_READ
  ).asJava
}
