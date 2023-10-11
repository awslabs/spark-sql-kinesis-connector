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

package org.apache.spark.sql.connector.kinesis.it

import java.util.Locale

import org.scalatest.matchers.should.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.kinesis.EnhancedKinesisTestUtils
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisOptions.HDFS_COMMITTER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider.AWS_KINESIS_SHORT_NAME
import org.apache.spark.sql.connector.kinesis.TrimHorizon
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.util.StreamManualClock



@IntegrationTestSuite
class KinesisSinkItSuite
  extends KinesisIntegrationTestBase(EFO_CONSUMER_TYPE)
  with Matchers
  with Logging {
  import testImplicits._

  private def createKinesisWriter(input: DataFrame,
                                  checkpointDir: String,
                                  withOutputMode: Option[OutputMode] = None,
                                  withOptions: Map[String, String] = Map[String, String]())
                                 (withSelectExpr: String*): StreamingQuery = {
  var stream: DataStreamWriter[Row] = null
    var df = input.toDF()
    if (withSelectExpr.nonEmpty) {
      df = df.selectExpr(withSelectExpr: _*)
    }
    stream = df.writeStream
      .format(AWS_KINESIS_SHORT_NAME)
      .option("checkpointLocation", checkpointDir)
      .queryName("kinesisStream")
    withOutputMode.foreach(stream.outputMode(_))
    withOptions.foreach(opt => stream.option(opt._1, opt._2))

    stream.start()
  }


  test("Test write data to Kinesis") {
    val clock = new StreamManualClock

    var writer: StreamingQuery = null

    val input = MemoryStream[String]
    val writerOptions = Map[String, String](
      KinesisOptions.STREAM_NAME -> testUtils.streamName,
      KinesisOptions.ENDPOINT_URL -> testUtils.endpointUrl,
      KinesisOptions.REGION -> testUtils.regionName,
    )

    val checkpointDir = getRandomCheckpointDir
    val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, HDFS_COMMITTER_TYPE)
      .load
      .selectExpr("CAST(data AS STRING)")
      .as[String].map(_.toInt)

    try {
      writer = createKinesisWriter(input.toDF(),
        checkpointDir,
        withOptions = writerOptions)(
        withSelectExpr = s"CAST('1' as STRING) as partitionKey", "value as data")


      testStream(reader)(
        StartStream(Trigger.ProcessingTime(100), clock),
        waitUntilBatchProcessed(clock),
        Execute { query =>
          logInfo("Pushing Data ")
          input.addData("1", "2", "3", "4", "5")
          writer.processAllAvailable()
          Thread.sleep(5000)
        },
        AdvanceManualClock(100),
        waitUntilBatchProcessed(clock),
        CheckAnswer(1, 2, 3, 4, 5),
        StopStream
      )

    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
  }

  test("read from and then write to stream") {
    testUtils.pushData((0 until 100).map(_.toString).toArray, aggregate = false)
    val localTestUtils = new EnhancedKinesisTestUtils(2)
    localTestUtils.createStream()

    val writeStreamName = localTestUtils.streamName

    logInfo(s"writeStreamName is ${writeStreamName}")
    // sleep for 1 s to avoid any concurrency issues
    Thread.sleep(1000.toLong)

    try {
      val checkpointDir = getRandomCheckpointDir


      val reader = createSparkReadStream(consumerType, testUtils, checkpointDir, HDFS_COMMITTER_TYPE)
        .option(KinesisOptions.STARTING_POSITION, TrimHorizon.iteratorType)

      val query = reader.load()
        .selectExpr("CAST(rand() AS STRING) as partitionKey", "CAST(data AS STRING)").as[(String, String)]
        .writeStream
        .format(AWS_KINESIS_SHORT_NAME)
        .outputMode("append")
        .option(KinesisOptions.REGION, localTestUtils.regionName)
        .option(KinesisOptions.STREAM_NAME, writeStreamName)
        .option(KinesisOptions.ENDPOINT_URL, localTestUtils.endpointUrl)
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime("15 seconds"))
        .start()
      query.awaitTermination(60000)
      query.stop()
    }
    finally {
      localTestUtils.deleteStream()
    }
  }

  test("Test write data with bad schema") {
    val input = MemoryStream[String]
    var writer: StreamingQuery = null
    var ex: Exception = null

    val options = Map[String, String](
      KinesisOptions.STREAM_NAME -> testUtils.streamName,
      KinesisOptions.ENDPOINT_URL -> testUtils.endpointUrl,
      KinesisOptions.REGION -> testUtils.regionName,
    )

    try {
      val checkpointDir = getRandomCheckpointDir
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), checkpointDir, withOptions = options)(
          withSelectExpr = "value as partitionKey", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("required attribute 'data' not found"))

    try {
      val checkpointDir = getRandomCheckpointDir
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), checkpointDir, withOptions = options)(
          withSelectExpr = "value as data", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("required attribute 'partitionkey' not found"))
  }

  test("Test write data with valid schema but wrong types") {
    val options = Map[String, String](
      KinesisOptions.STREAM_NAME -> testUtils.streamName,
      KinesisOptions.ENDPOINT_URL -> testUtils.endpointUrl,
      KinesisOptions.REGION -> testUtils.regionName,
    )

    val input = MemoryStream[String]
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      val checkpointDir = getRandomCheckpointDir

      /* partitionKey field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), checkpointDir, withOptions = options)(
          withSelectExpr = s"CAST('1' as INT) as partitionKey", "value as data"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT)
      .contains("partitionkey attribute type must be a string or binarytype"))

    try {
      val checkpointDir = getRandomCheckpointDir

      /* data field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKinesisWriter(input.toDF(), checkpointDir, withOptions = options)(
          withSelectExpr = "value as partitionKey", "CAST(value as INT) as data"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      if (writer != null) {
        writer.stop()
      }
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "data attribute type must be a string or binarytype"))
  }

  test("update and complete output mode not supported") {
    val input = MemoryStream[String]
    Seq("Update", "Complete") foreach { outputMode =>
      val exception = intercept[IllegalStateException] {
        input.toDF()
          .writeStream
          .format(AWS_KINESIS_SHORT_NAME)
          .outputMode(outputMode)
          .option("streamName", "test1")
          .option("endpointUrl", "https://kinesis.us-east-2.amazonaws.com")
          .option("checkpointLocation", "/test1")
          .trigger(Trigger.ProcessingTime("15 seconds"))
          .start()
      }

      exception.getMessage should include(s"OutputMode ${outputMode} not supported.")
    }

  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val input = MemoryStream[String]
      val ex = intercept[IllegalArgumentException] {
      val query = input.toDF()
        .writeStream
          .format(AWS_KINESIS_SHORT_NAME)

        options.foreach { case (k, v) => query.option(k, v) }
        query.start()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testBadOptions()(s"${KinesisOptions.STREAM_NAME} is a required field")
    testBadOptions(KinesisOptions.STREAM_NAME -> "test")(s"${KinesisOptions.REGION} is a required field")
    testBadOptions(KinesisOptions.STREAM_NAME -> "test",
      KinesisOptions.REGION -> "test")( s"${KinesisOptions.ENDPOINT_URL} is a required field")
  }

}
