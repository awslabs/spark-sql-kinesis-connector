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

import org.slf4j.LoggerFactory

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.connector.kinesis.pt.KinesisConsumerTest.addConfigIfLocalTest
import org.apache.spark.sql.connector.kinesis.pt.KinesisConsumerTest.waitForQueryStop
import org.apache.spark.sql.streaming.Trigger

object KinesisConsumerCrossAccountTest {

  private val log = LoggerFactory.getLogger("KinesisConsumerCrossAccountTest")
  val localTest: Boolean = System.getProperty("os.name").toLowerCase().startsWith("mac os")

  implicit val stringEncoder: Encoder[String] = Encoders.STRING

  def main(args: Array[String]): Unit = {
    val streamName = args(0)
    val consumerType = args(1)
    val efoConsumerName = args(2)
    val startPosition = args(3)
    val checkpointDir = args(4)
    val writeToDir = args(5)
    val regionName = args(6)
    val stsRoleArn = args(7)
    val stsSessionName = args(8)
    val endpointUrl = args(9)
    val committerType = if (args.length>10) args(10) else ""

    val sparkBuilder = SparkSession.builder()
      .appName("KinesisConsumerCrossAccountTest")
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
      .option(KinesisOptions.STS_ROLE_ARN, stsRoleArn)
      .option(KinesisOptions.STS_SESSION_NAME, stsSessionName)

    if (consumerType == KinesisOptions.EFO_CONSUMER_TYPE) {
      reader.option(KinesisOptions.CONSUMER_NAME, efoConsumerName)
    }

    if (committerType == "dynamodb") {
      reader.option(KinesisOptions.METADATA_COMMITTER_TYPE, KinesisOptions.DYNAMODB_COMMITTER_TYPE)
    }

    val inputDf = reader.load()
      .selectExpr("CAST(data AS STRING)")

    val query = inputDf
      .writeStream
      .queryName("KinesisDataConsumerCrossAccount")
      .foreachBatch {KinesisConsumerForEachBatchTest.batchProcessor(writeToDir)}
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    // ensure gracefully shutdown before the process is killed
    sys.addShutdownHook {
      query.stop()
    }
    
    waitForQueryStop(query, writeToDir)

  }
}
