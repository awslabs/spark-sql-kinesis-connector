package org.apache.spark.sql.connector.kinesis.smoketest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * Smoke test for reading from Kinesis using GetRecords consumer.
 * 
 * Usage:
 *   spark-submit \
 *     --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestGetRecords \
 *     --master local[2] \
 *     --jars target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
 *     smoke-test.jar \
 *     <stream-name> <region> [duration-seconds]
 */
object SmokeTestGetRecords {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SmokeTestGetRecords <stream-name> <region> [duration-seconds]")
      System.exit(1)
    }

    val streamName = args(0)
    val region = args(1)
    val durationSeconds = if (args.length > 2) args(2).toInt else 60

    println(s"Starting smoke test: GetRecords consumer")
    println(s"  Stream: $streamName")
    println(s"  Region: $region")
    println(s"  Duration: $durationSeconds seconds")
    println(s"  Spark Version: ${org.apache.spark.SPARK_VERSION}")
    println(s"  Scala Version: ${scala.util.Properties.versionString}")
    println(s"  Java Version: ${System.getProperty("java.version")}")
    println()

    val spark = SparkSession.builder()
      .appName("Smoke Test - GetRecords")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/kinesis-smoke-test-getrecords")
      .getOrCreate()

    try {
      val kinesis = spark
        .readStream
        .format("aws-kinesis")
        .option("kinesis.region", region)
        .option("kinesis.streamName", streamName)
        .option("kinesis.endpointUrl", s"https://kinesis.$region.amazonaws.com")
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.startingposition", "TRIM_HORIZON")
        .load()

      println("✓ Stream reader created successfully")

      val query = kinesis
        .selectExpr(
          "CAST(data AS STRING) as message",
          "partitionKey",
          "sequenceNumber",
          "approximateArrivalTimestamp"
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

      println("✓ Query started successfully")
      println(s"Running for $durationSeconds seconds...")
      println()

      query.awaitTermination(durationSeconds * 1000L)
      
      println()
      println("Stopping query...")
      query.stop()
      println("✓ Query stopped successfully")

    } catch {
      case e: Exception =>
        println(s"✗ Error during smoke test: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      println("✓ Spark session stopped")
    }

    println()
    println("=== Smoke Test GetRecords: PASSED ===")
  }
}
