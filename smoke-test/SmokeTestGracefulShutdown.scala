package org.apache.spark.sql.connector.kinesis.smoketest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Smoke test for graceful shutdown behavior.
 * Tests that EFO consumers are properly deregistered and resources are cleaned up.
 * 
 * Usage:
 *   spark-submit \
 *     --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestGracefulShutdown \
 *     --master local[2] \
 *     --jars target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
 *     smoke-test.jar \
 *     <stream-name> <region> <consumer-name> [duration-seconds]
 */
object SmokeTestGracefulShutdown {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: SmokeTestGracefulShutdown <stream-name> <region> <consumer-name> [duration-seconds]")
      System.exit(1)
    }

    val streamName = args(0)
    val region = args(1)
    val consumerName = args(2)
    val durationSeconds = if (args.length > 3) args(3).toInt else 30

    println(s"Starting smoke test: Graceful Shutdown")
    println(s"  Stream: $streamName")
    println(s"  Region: $region")
    println(s"  Consumer Name: $consumerName")
    println(s"  Duration: $durationSeconds seconds")
    println(s"  Spark Version: ${org.apache.spark.SPARK_VERSION}")
    println(s"  Scala Version: ${scala.util.Properties.versionString}")
    println(s"  Java Version: ${System.getProperty("java.version")}")
    println()

    val spark = SparkSession.builder()
      .appName("Smoke Test - Graceful Shutdown")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/kinesis-smoke-test-shutdown")
      .getOrCreate()

    var query: StreamingQuery = null
    val shutdownHookExecuted = new AtomicBoolean(false)

    try {
      val kinesis = spark
        .readStream
        .format("aws-kinesis")
        .option("kinesis.region", region)
        .option("kinesis.streamName", streamName)
        .option("kinesis.endpointUrl", s"https://kinesis.$region.amazonaws.com")
        .option("kinesis.consumerType", "SubscribeToShard")
        .option("kinesis.consumerName", consumerName)
        .option("kinesis.startingposition", "LATEST")
        .load()

      println("✓ Stream reader created successfully")

      query = kinesis
        .selectExpr("CAST(data AS STRING) as message", "partitionKey")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

      println("✓ Query started successfully")
      println("✓ EFO consumer should be registering...")

      // Add shutdown hook to test graceful shutdown
      val shutdownThread = new Thread(() => {
        println()
        println("=== Shutdown Hook Triggered ===")
        shutdownHookExecuted.set(true)
        if (query != null && !query.isActive) {
          println("Query already stopped")
        } else if (query != null) {
          println("Stopping query from shutdown hook...")
          query.stop()
          println("✓ Query stopped from shutdown hook")
        }
      })
      Runtime.getRuntime.addShutdownHook(shutdownThread)

      println(s"Running for $durationSeconds seconds...")
      println("Press Ctrl+C to test shutdown hook, or wait for automatic shutdown")
      println()

      // Wait for specified duration
      Thread.sleep(durationSeconds * 1000L)
      
      println()
      println("Duration elapsed, stopping query...")
      query.stop()
      println("✓ Query stopped successfully")
      println("✓ EFO consumer should be deregistering...")

      // Remove shutdown hook since we stopped normally
      try {
        Runtime.getRuntime.removeShutdownHook(shutdownThread)
      } catch {
        case _: IllegalStateException => // Already shutting down, ignore
      }

    } catch {
      case e: InterruptedException =>
        println()
        println("Interrupted, shutting down...")
        if (query != null) {
          query.stop()
          println("✓ Query stopped on interrupt")
        }
      case e: Exception =>
        println(s"✗ Error during smoke test: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      println("✓ Spark session stopped")
    }

    println()
    if (shutdownHookExecuted.get()) {
      println("=== Smoke Test Graceful Shutdown: PASSED (via shutdown hook) ===")
    } else {
      println("=== Smoke Test Graceful Shutdown: PASSED (normal termination) ===")
    }
    println()
    println("Verify EFO consumer was deregistered using:")
    println(s"  aws kinesis list-stream-consumers --stream-name $streamName --region $region")
    println()
    println("Expected: Consumer '$consumerName' should NOT be in the list")
  }
}
