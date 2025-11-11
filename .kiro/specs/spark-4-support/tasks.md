# Implementation Plan

- [ ] 1. Update Maven build configuration for Spark 4.0.0
  - Update pom.xml to use Spark 4.0.0, Scala 2.13, and Java 17
  - Update artifact ID to reflect Scala 2.13
  - Verify all dependency versions are compatible
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 1.1 Update core version properties in pom.xml
  - Change `spark.version` from 3.5.5 to 4.0.0
  - Change `scala.binary.version` from 2.12 to 2.13
  - Change `maven.compiler.source` and `maven.compiler.target` from 1.8 to 17
  - Update `artifactId` from `spark-streaming-sql-kinesis-connector_2.12` to `spark-streaming-sql-kinesis-connector_2.13`
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 1.2 Verify and update dependency versions
  - Verify AWS SDK v2 (2.31.11) is compatible with Java 17
  - Verify Jackson (2.18.3) is compatible with Spark 4.0.0
  - Check if any Maven plugin versions need updates for Java 17
  - Update `outputDirectory` and `testOutputDirectory` paths to use scala-2.13
  - _Requirements: 2.5_

- [x] 1.3 Attempt initial build and document compilation errors
  - Run `mvn clean compile` to identify compilation issues
  - Document all compilation errors for systematic resolution
  - Create a list of files that need Scala 2.13 updates
  - _Requirements: 1.1, 2.1_

- [ ] 2. Fix Scala 2.13 compatibility issues in source code
  - Update imports and fix collection API usage
  - Fix type inference issues
  - Address any deprecated API usage
  - _Requirements: 1.1, 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 2.1 Update collection imports across all source files
  - Replace `scala.collection.JavaConverters._` with `scala.jdk.CollectionConverters._`
  - Update any usage of deprecated collection methods
  - Fix implicit conversion issues if any
  - _Requirements: 3.1, 3.5_

- [x] 2.2 Fix compilation errors in core connector files
  - Fix `KinesisV2Table.scala` for Scala 2.13 compatibility
  - Fix `KinesisV2MicrobatchStream.scala` for Scala 2.13 compatibility
  - Fix `KinesisV2ScanBuilder.scala` for Scala 2.13 compatibility
  - Fix `KinesisV2Scan.scala` for Scala 2.13 compatibility
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 2.3 Fix compilation errors in reader/writer components
  - Fix `KinesisV2PartitionReader.scala` for Scala 2.13 compatibility
  - Fix `KinesisV2PartitionReaderFactory.scala` for Scala 2.13 compatibility
  - Fix `KinesisSink.scala` for Scala 2.13 compatibility
  - Fix `KinesisWriter.scala` for Scala 2.13 compatibility
  - Fix `KinesisWriteTask.scala` for Scala 2.13 compatibility
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 2.4 Fix compilation errors in supporting components
  - Fix `ShardSyncer.scala` for Scala 2.13 compatibility
  - Fix `KinesisOptions.scala` for Scala 2.13 compatibility
  - Fix metadata committer files for Scala 2.13 compatibility
  - Fix client files for Scala 2.13 compatibility
  - Fix retrieval components for Scala 2.13 compatibility
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 2.5 Fix Scala 2.13 implicit type annotation warnings
  - Add explicit type annotations to implicit definitions in `KinesisModel.scala`
  - Add explicit type annotations to implicit definitions in `KinesisV2SourceOffset.scala`
  - Add explicit type annotations to implicit definitions in `HDFSMetadataCommitter.scala`
  - Add explicit type annotations to implicit definitions in `DynamodbMetadataCommitter.scala`
  - _Requirements: 3.1, 3.5_

- [ ] 2.6 Address Scala 2.13 deprecation warnings (Future Work)
  - Note: 15 deprecation warnings exist (4 since 2.13.0, 2 since 2.13.2, 9 since 2.13.3)
  - These are warnings only and do not affect functionality
  - Recommended to address in a separate PR after Spark 4.0.0 migration is complete
  - Run `mvn compile -Dscalac.args="-deprecation"` to see detailed locations
  - See `deprecation-warnings-analysis.md` for details
  - _Requirements: 3.5_

- [x] 3. Verify and update Spark 4.0.0 API usage
  - Check DSv2 interface compatibility
  - Update any deprecated Spark API usage
  - Verify internal Spark API availability
  - _Requirements: 1.1, 1.5, 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 3.1 Verify DSv2 interface implementations
  - Check `Table` and `SupportsRead` interfaces in `KinesisV2Table`
  - Check `MicroBatchStream` and `SupportsAdmissionControl` interfaces in `KinesisV2MicrobatchStream`
  - Verify `ScanBuilder`, `Scan`, `InputPartition` interfaces
  - Check `PartitionReader` and `PartitionReaderFactory` interfaces
  - Verify `TableCapability` enum values are still valid
  - _Requirements: 3.2, 3.3, 3.4_

- [x] 3.2 Verify internal Spark API usage
  - Check `org.apache.spark.internal.Logging` is available
  - Check `org.apache.spark.sql.internal.SQLConf` is available
  - Check `org.apache.spark.util.SerializableConfiguration` is available
  - Check `org.apache.spark.util.ThreadUtils` is available
  - Find alternatives if any internal APIs were removed
  - _Requirements: 3.4, 3.5_

- [x] 3.3 Update deprecated API usage
  - Search for and replace any deprecated Spark API calls
  - Update to Spark 4.0.0 recommended alternatives
  - Document any API changes that affect functionality
  - _Requirements: 3.5_

- [x] 4. Update and fix test code for Spark 4.0.0
  - Fix test compilation errors
  - Update test utilities usage
  - Ensure all tests pass
  - _Requirements: 1.1, 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 4.1 Fix test compilation errors for Scala 2.13
  - Update collection imports in test files
  - Fix type inference issues in tests
  - Update any Scala 2.12-specific test code
  - _Requirements: 4.5_

- [x] 4.2 Update test utilities and fixtures
  - Update `KinesisTestUtils.scala` for Spark 4.0.0
  - Update `KinesisIntegrationTestBase.scala` for Spark 4.0.0
  - Verify SparkSession creation in tests works
  - Update any test data generation code
  - _Requirements: 4.3_

- [x] 4.3 Fix and run unit tests
  - Fix `HDFSMetaDataCommitterSuite.scala`
  - Fix `DynamodbMetadataCommitterSuite.scala`
  - Fix `ShardSyncerSuite.scala`
  - Fix `KinesisOptionsSuite.scala`
  - Run all unit tests and verify they pass
  - _Requirements: 4.1, 4.4_

- [x] 4.4 Fix and run integration tests
  - Fix `KinesisSourceItSuite.scala`
  - Fix `KinesisSinkItSuite.scala`
  - Fix `KinesisSourceReshardItSuite.scala`
  - Fix `KinesisSourceCrossAccountItSuite.scala`
  - Fix `EfoShardSubscriberItSuite.scala`
  - Fix other integration test files
  - _Requirements: 4.2, 4.4_

- [ ] 5. Update documentation for Spark 4.0.0
  - Update README.md with new version requirements
  - Update code examples
  - Update artifact names
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 5.1 Update version requirements in README.md
  - Update Spark version to 4.0.0
  - Update Scala version to 2.13
  - Update Java requirement to 17
  - Update "Current version is tested with Spark 3.2 and above" statement
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 5.2 Update artifact names and Maven coordinates
  - Change all `_2.12` references to `_2.13` in README
  - Update Maven dependency example
  - Update S3 jar file paths (e.g., spark-streaming-sql-kinesis-connector_2.13-1.0.0.jar)
  - Update spark-submit examples
  - _Requirements: 5.4_

- [ ] 5.3 Review and update code examples
  - Verify all Scala code examples compile with Spark 4.0.0
  - Update examples if Spark 4.0.0 introduces relevant API changes
  - Test example code snippets
  - _Requirements: 5.5_

- [ ] 5.4 Update build and developer setup instructions
  - Update Java version requirement in Developer Setup section
  - Update any version-specific build instructions
  - Verify build commands work with new versions
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 6. Perform final validation and testing
  - Run complete test suite
  - Build shaded JAR
  - Verify end-to-end functionality
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 6.1 Run complete test suite
  - Execute `mvn clean test` for unit tests
  - Execute integration tests with `-Pintegration-test`
  - Verify all tests pass without errors
  - Document any test failures and resolve them
  - _Requirements: 4.1, 4.2_

- [ ] 6.2 Build and verify shaded JAR
  - Execute `mvn clean package` to build shaded JAR
  - Verify JAR contains all required dependencies
  - Check JAR size is reasonable
  - Verify shaded package relocations are correct
  - _Requirements: 1.1, 2.5_

- [ ] 6.3 Perform smoke testing with sample application
  - Create a simple test application using the connector
  - Test reading from Kinesis with GetRecords consumer
  - Test reading from Kinesis with SubscribeToShard consumer
  - Test writing to Kinesis
  - Verify graceful shutdown works
  - _Requirements: 1.4, 1.5_

- [ ] 6.4 Verify checkpoint compatibility
  - Test reading from Spark 3.5.5 checkpoints (if possible)
  - Verify offset serialization/deserialization works
  - Test HDFS metadata committer
  - Test DynamoDB metadata committer
  - _Requirements: 1.4_

- [ ] 6.5 Final documentation review
  - Review all documentation changes
  - Verify README is accurate and complete
  - Check for any missing updates
  - Verify all links and references are correct
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_
