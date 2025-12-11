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

import java.util.concurrent.CountDownLatch

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.connector.kinesis.getRegionNameByEndpoint
import org.apache.spark.sql.connector.kinesis.pt.KinesisConsumerTest.addConfigIfLocalTest
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.ThreadUtils.newDaemonSingleThreadScheduledExecutor

object KinesisProducerTest {

  private val log = LoggerFactory.getLogger("KinesisProducerTest")
  val localTest: Boolean = System.getProperty("os.name").toLowerCase().startsWith("mac os")

  def main(args: Array[String]): Unit = {
    val streamName = args(0)
    val checkpointDir = args(1)
    val numPartitions = args(2).toInt
    val rowsPerSecond = args(3).toInt
    val totalSeconds = args(4).toLong
    val waitBeforeStopSeconds = args(5).toLong

    val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"
    val regionName: String = getRegionNameByEndpoint(endpointUrl)

    val dataStopLatch: CountDownLatch = new CountDownLatch(1)

    val sparkBuilder = SparkSession.builder()
      .appName("KinesisProducerTest")
      .config("spark.sql.ui.explainMode", "extended")

    addConfigIfLocalTest(sparkBuilder)
    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val reader = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", rowsPerSecond)
      .option("numPartitions", numPartitions)
      .load()

    val query = reader.toDF()
      .selectExpr("CAST(rand() AS STRING) as partitionKey", "CAST(value AS STRING) as data")
      .as[(String, String)]
      .writeStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option(KinesisOptions.REGION, regionName)
      .option(KinesisOptions.STREAM_NAME, streamName)
      .option(KinesisOptions.ENDPOINT_URL, endpointUrl)
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    val waitExecutor = newDaemonSingleThreadScheduledExecutor("waitExecutor")

    waitExecutor.submit(
        new Runnable {
          override def run(): Unit = {
            try {
                Thread.sleep(totalSeconds * 1000)
            } catch {
              case NonFatal(e) =>
                log.error("Error when send data, exiting...", e)
                throw e
            } finally {
              dataStopLatch.countDown()
            }
          }
        }
    )
    dataStopLatch.await()

    // Wait for a while to ensure all data produced to Kinesis
    query.awaitTermination(waitBeforeStopSeconds * 1000)
    query.stop
  }
}
