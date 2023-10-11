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

package org.apache.spark.sql.connector.kinesis.pt

import org.apache.log4j.LogManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataValidator {

  val testSchemaWithPartition: StructType = StructType(Array(
    StructField("valString", StringType, nullable = false),
  ))
  def main(args: Array[String]) {
    val dataSrc = args(0)
    val expectedRows: Option[Long] = if (args.length>1) Some(args(1).toLong) else None
    val log = LogManager.getRootLogger

    val spark = SparkSession.builder()
      .appName("KinesisConnectorPTDataValidator")
      .config("spark.sql.ui.explainMode", "extended")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val df = spark.read
      .schema(testSchemaWithPartition)
      .format("csv")
      .option("header", false)
      .option("recursiveFileLookup", "true")
      .load(dataSrc)

    val totalRows = df.count()

    log.info(s"totalRows: ${totalRows}")

    if (expectedRows.isDefined) {
      assert(totalRows==expectedRows.get, s"totalRows ${totalRows} doesn't match expectedRows ${expectedRows}")
    }

  }
}
