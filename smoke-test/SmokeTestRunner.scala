package org.apache.spark.sql.connector.kinesis.smoketest

/**
 * Main runner for all smoke tests.
 * Executes all smoke tests in sequence and reports results.
 * 
 * Usage:
 *   spark-submit \
 *     --class org.apache.spark.sql.connector.kinesis.smoketest.SmokeTestRunner \
 *     --master local[2] \
 *     --jars target/spark-streaming-sql-kinesis-connector_2.13-1.4.3-SNAPSHOT.jar \
 *     smoke-test.jar \
 *     <stream-name> <region> <consumer-name>
 */
object SmokeTestRunner {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: SmokeTestRunner <stream-name> <region> <consumer-name>")
      System.exit(1)
    }

    val streamName = args(0)
    val region = args(1)
    val consumerName = args(2)

    println("=" * 80)
    println("SPARK 4.0.0 KINESIS CONNECTOR - SMOKE TEST SUITE")
    println("=" * 80)
    println()
    println(s"Configuration:")
    println(s"  Stream Name: $streamName")
    println(s"  Region: $region")
    println(s"  Consumer Name: $consumerName")
    println(s"  Spark Version: ${org.apache.spark.SPARK_VERSION}")
    println(s"  Scala Version: ${scala.util.Properties.versionString}")
    println(s"  Java Version: ${System.getProperty("java.version")}")
    println()
    println("=" * 80)
    println()

    val results = scala.collection.mutable.ArrayBuffer[(String, Boolean, Option[String])]()

    // Test 1: Write to Kinesis
    println("TEST 1: Write to Kinesis")
    println("-" * 80)
    try {
      SmokeTestWrite.main(Array(streamName, region, "5"))
      results += (("Write to Kinesis", true, None))
      println()
    } catch {
      case e: Exception =>
        results += (("Write to Kinesis", false, Some(e.getMessage)))
        println(s"FAILED: ${e.getMessage}")
        println()
    }

    // Wait a bit for records to be available
    println("Waiting 5 seconds for records to be available...")
    Thread.sleep(5000)
    println()

    // Test 2: Read with GetRecords
    println("TEST 2: Read with GetRecords Consumer")
    println("-" * 80)
    try {
      SmokeTestGetRecords.main(Array(streamName, region, "20"))
      results += (("Read with GetRecords", true, None))
      println()
    } catch {
      case e: Exception =>
        results += (("Read with GetRecords", false, Some(e.getMessage)))
        println(s"FAILED: ${e.getMessage}")
        println()
    }

    // Test 3: Read with EFO
    println("TEST 3: Read with SubscribeToShard (EFO) Consumer")
    println("-" * 80)
    try {
      SmokeTestEFO.main(Array(streamName, region, consumerName, "20"))
      results += (("Read with EFO", true, None))
      println()
    } catch {
      case e: Exception =>
        results += (("Read with EFO", false, Some(e.getMessage)))
        println(s"FAILED: ${e.getMessage}")
        println()
    }

    // Test 4: Graceful Shutdown
    println("TEST 4: Graceful Shutdown")
    println("-" * 80)
    try {
      SmokeTestGracefulShutdown.main(Array(streamName, region, s"$consumerName-shutdown", "15"))
      results += (("Graceful Shutdown", true, None))
      println()
    } catch {
      case e: Exception =>
        results += (("Graceful Shutdown", false, Some(e.getMessage)))
        println(s"FAILED: ${e.getMessage}")
        println()
    }

    // Print summary
    println()
    println("=" * 80)
    println("SMOKE TEST SUMMARY")
    println("=" * 80)
    println()

    val passed = results.count(_._2)
    val failed = results.count(!_._2)

    results.foreach { case (name, success, error) =>
      val status = if (success) "✓ PASSED" else "✗ FAILED"
      println(f"  $status%-10s - $name")
      error.foreach(err => println(s"             Error: $err"))
    }

    println()
    println(s"Total: ${results.size} tests")
    println(s"Passed: $passed")
    println(s"Failed: $failed")
    println()

    if (failed > 0) {
      println("=" * 80)
      println("SMOKE TEST SUITE: FAILED")
      println("=" * 80)
      System.exit(1)
    } else {
      println("=" * 80)
      println("SMOKE TEST SUITE: PASSED")
      println("=" * 80)
    }
  }
}
