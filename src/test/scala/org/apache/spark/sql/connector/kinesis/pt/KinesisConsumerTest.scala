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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileContext
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.connector.kinesis.getRegionNameByEndpoint
import org.apache.spark.sql.connector.kinesis.tryAndIgnoreError
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

object KinesisConsumerTest {

  val localTest: Boolean = System.getProperty("os.name").toLowerCase().startsWith("mac os")

  implicit val stringEncoder: Encoder[String] = Encoders.STRING

  def main(args: Array[String]): Unit = {
    val streamName = args(0)
    val consumerType = args(1)
    val efoConsumerName = args(2)
    val startPosition = args(3)
    val checkpointDir = args(4)
    val writeToDir = args(5)
    val committerType = args(6)
    val tableName = checkpointDir.split("/").takeRight(2).mkString("-")

    val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"
    val regionName: String = getRegionNameByEndpoint(endpointUrl)


    val sparkBuilder = SparkSession.builder()
      .appName("KinesisConsumerTest")
      .config("spark.sql.ui.explainMode", "extended")

    addConfigIfLocalTest(sparkBuilder)
    val spark = sparkBuilder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    val reader = spark
      .readStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option(KinesisOptions.REGION, regionName)
      .option(KinesisOptions.STREAM_NAME, streamName)
      .option(KinesisOptions.ENDPOINT_URL, endpointUrl)
      .option(KinesisOptions.CONSUMER_TYPE, consumerType)
      .option(KinesisOptions.STARTING_POSITION, startPosition)
      .option(KinesisOptions.DYNAMODB_TABLE_NAME, tableName)
      .option(KinesisOptions.METADATA_COMMITTER_TYPE, committerType)


    if (consumerType == KinesisOptions.EFO_CONSUMER_TYPE) {
      reader.option(KinesisOptions.CONSUMER_NAME, efoConsumerName)
    }


    val inputDf = reader.load()
      .selectExpr("CAST(data AS STRING)")
      .as[String]

    val query = inputDf
      .writeStream
      .queryName("KinesisDataConsumer")
      .format("csv")
      .option("path", writeToDir)
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    // ensure gracefully shutdown before the process is killed
    sys.addShutdownHook {
      query.stop()
    }

    waitForQueryStop(query, writeToDir)
  }

  def addConfigIfLocalTest(sparkBuilder: SparkSession.Builder): Unit = {
    if (localTest) {
      sparkBuilder
        .master("local[2]")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
          "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider")
//        .config("spark.driver.bindAddress", "127.0.0.1") // VPN env.
    }
  }

  def waitForQueryStop(query: StreamingQuery, path: String): Unit = {
    val stopLockPath = new Path(path, "STOP_LOCK")
    val fileContext = FileContext.getFileContext(stopLockPath.toUri, new Configuration())

    while (query.isActive) {
      // Stop the query when "STOP_LOCK" file is found.
      if (fileContext.util().exists(stopLockPath)) {
        tryAndIgnoreError("delete stop lock")(fileContext.delete(stopLockPath, false))
        query.stop()
      }

      Thread.sleep(500)
    }
  }
}
