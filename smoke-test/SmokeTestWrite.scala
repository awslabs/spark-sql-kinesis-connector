package org.apache.spark.sql.connector.kinesis.smoketest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Smoke test for writing to Kinesis.
 * 
 * Usage:
 *   spark-submit \
 *     --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestWrite \
 *     --master local[2] \
 *     --jars target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
 *     smoke-test.jar \
 *     <stream-name> <region> [num-records]
 */
object SmokeTestWrite {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SmokeTestWrite <stream-name> <region> [num-records]")
      System.exit(1)
    }

    val streamName = args(0)
    val region = args(1)
    val numRecords = if (args.length > 2) args(2).toInt else 10

    println(s"Starting smoke test: Write to Kinesis")
    println(s"  Stream: $streamName")
    println(s"  Region: $region")
    println(s"  Number of Records: $numRecords")
    println(s"  Spark Version: ${org.apache.spark.SPARK_VERSION}")
    println(s"  Scala Version: ${scala.util.Properties.versionString}")
    println(s"  Java Version: ${System.getProperty("java.version")}")
    println()

    val spark = SparkSession.builder()
      .appName("Smoke Test - Write")
      .master("local[2]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data
      val testData = (1 to numRecords).map { i =>
        (
          s"test-key-$i",
          s"Test message $i from Spark ${org.apache.spark.SPARK_VERSION} at ${System.currentTimeMillis()}"
        )
      }.toDF("partitionKey", "data")

      println(s"✓ Created $numRecords test records")
      println()
      println("Sample records:")
      testData.show(5, truncate = false)
      println()

      // Prepare data with correct schema for Kinesis
      val preparedData = testData.selectExpr("CAST(data AS BINARY) as data", "partitionKey")
      
      // Write to temp directory for file-based streaming
      val tempPath = s"/tmp/kinesis-smoke-test-source-${System.currentTimeMillis()}"
      preparedData.write.json(tempPath)
      
      println(s"✓ Prepared streaming source at: $tempPath")

      // Read as streaming DataFrame from the temp directory
      val streamingData = spark.readStream
        .schema(preparedData.schema)
        .json(tempPath)

      // Write to Kinesis using streaming API
      println("Writing to Kinesis...")
      val checkpointPath = s"/tmp/kinesis-smoke-test-checkpoint-${System.currentTimeMillis()}"
      val query = streamingData
        .writeStream
        .format("aws-kinesis")
        .option("kinesis.region", region)
        .option("kinesis.streamName", streamName)
        .option("kinesis.endpointUrl", s"https://kinesis.$region.amazonaws.com")
        .option("checkpointLocation", checkpointPath)
        .outputMode("append")
        .start()

      // Wait for the write to complete
      query.processAllAvailable()
      query.stop()

      println("✓ Write completed successfully")
      
      // Clean up temp directories
      try {
        import scala.sys.process._
        s"rm -rf $tempPath".!
        s"rm -rf $checkpointPath".!
        println(s"✓ Cleaned up temp directories")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }

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
    println("=== Smoke Test Write: PASSED ===")
    println()
    println("Verify records were written using:")
    println(s"  aws kinesis get-shard-iterator --stream-name $streamName --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --region $region")
    println("  aws kinesis get-records --shard-iterator <iterator-from-previous-command> --region $region")
  }
}
