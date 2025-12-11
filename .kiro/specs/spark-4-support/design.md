# Design Document: Spark 4.0.0 Support

## Overview

This design document outlines the technical approach for upgrading the Spark Structured Streaming Kinesis Connector from Spark 3.5.5 to Spark 4.0.0. The upgrade involves updating build configurations, addressing API changes, and ensuring compatibility with the new Scala 2.13 and Java 17 requirements.

The connector currently uses:
- Spark 3.5.5
- Scala 2.12
- Java 8

The target configuration will be:
- Spark 4.0.0
- Scala 2.13
- Java 17

## Architecture

### Current Architecture
The connector implements Spark's Data Source V2 (DSv2) API for structured streaming:

**Source Components:**
- `KinesisV2Table` - Implements `Table` and `SupportsRead` interfaces
- `KinesisV2MicrobatchStream` - Implements `MicroBatchStream` and `SupportsAdmissionControl`
- `KinesisV2ScanBuilder` - Creates scan configurations
- `KinesisV2Scan` - Defines the scan operation
- `KinesisV2PartitionReader` - Reads data from Kinesis shards
- `KinesisV2PartitionReaderFactory` - Creates partition readers

**Sink Components:**
- `KinesisSink` - Implements legacy `Sink` interface
- `KinesisWriter` - Handles writing to Kinesis
- `KinesisWriteTask` - Task-level write operations

**Supporting Components:**
- `ShardSyncer` - Manages shard discovery and synchronization
- `MetadataCommitter` - Handles checkpoint metadata
- `KinesisClientConsumer` - AWS Kinesis client wrapper

### Architecture Changes for Spark 4.0.0

Based on the Spark 4.0.0 changelog, the DSv2 framework has several enhancements but maintains backward compatibility for most interfaces. Key changes to consider:

1. **DSv2 Partitioning Expressions** - Moved to `functions.partitioning` (SPARK-45965)
2. **TableCatalog API Changes** - `loadTable` now indicates if it's for writing (SPARK-49246)
3. **Conditional Nullification** - Metadata columns in DML operations (SPARK-50820)
4. **Storage Partition Join** - Improvements (SPARK-51938)

The connector's current DSv2 implementation should remain largely compatible, but we need to verify:
- Interface signatures haven't changed
- Deprecated methods are replaced
- New required methods are implemented

## Components and Interfaces

### 1. Build Configuration (pom.xml)

**Changes Required:**

```xml
<!-- Update Spark version -->
<spark.version>4.0.0</spark.version>

<!-- Update Scala version -->
<scala.binary.version>2.13</scala.binary.version>

<!-- Update Java version -->
<maven.compiler.source>17</maven.compiler.source>
<maven.compiler.target>17</maven.compiler.target>

<!-- Update artifact ID -->
<artifactId>spark-streaming-sql-kinesis-connector_2.13</artifactId>
```

**Dependency Updates:**
- All Spark dependencies will automatically use 4.0.0
- Scala standard library will use 2.13
- Verify AWS SDK compatibility with Java 17
- Check Jackson version compatibility (currently 2.18.3)
- Verify all test dependencies work with Scala 2.13

**Potential Issues:**
- Some Maven plugins may need updates for Java 17 compatibility
- Scala 2.13 has stricter type inference and may require code adjustments
- The `scala-maven-plugin` configuration may need updates

### 2. Source Code Compatibility

#### Scala 2.13 Migration

Scala 2.13 introduces several changes from 2.12:

**Collection API Changes:**
- `scala.collection.JavaConverters` is deprecated in favor of `scala.jdk.CollectionConverters`
- Collection methods may have different signatures
- Implicit conversions may behave differently

**Code Locations to Review:**
- `KinesisV2Table.scala` - Uses `JavaConverters.asJava`
- `KinesisV2MicrobatchStream.scala` - Uses collections extensively
- All files using Scala collections

**Expected Changes:**
- Replace deprecated collection methods
- Update implicit conversion imports if needed
- Fix any type inference issues

#### Spark API Compatibility

**DSv2 Interfaces:**

Current implementations:
```scala
class KinesisV2Table extends Table with SupportsRead
class KinesisV2MicrobatchStream extends SupportsAdmissionControl with MicroBatchStream
```

**Verification Needed:**
1. Check if interface signatures changed in Spark 4.0.0
2. Verify all required methods are implemented
3. Check for new optional methods that should be implemented
4. Review deprecated method warnings

**Specific Areas:**

1. **MicroBatchStream Interface:**
   - `latestOffset(Offset, ReadLimit)` - Verify signature
   - `planInputPartitions(Offset, Offset)` - Check return type
   - `commit(Offset)` - Verify behavior expectations

2. **SupportsAdmissionControl Interface:**
   - `reportLatestOffset()` - Verify this interface still exists
   - Check if new methods were added

3. **Table Interface:**
   - `capabilities()` - Verify `TableCapability` enum values
   - `schema()` - Check if signature changed

4. **Sink Interface:**
   - The legacy `Sink` interface may be deprecated
   - Consider migrating to DSv2 sink APIs if needed

#### Internal Spark APIs

The connector uses some internal Spark classes:
- `org.apache.spark.internal.Logging`
- `org.apache.spark.sql.internal.SQLConf`
- `org.apache.spark.util.SerializableConfiguration`
- `org.apache.spark.util.ThreadUtils`

**Risk Assessment:**
- Internal APIs may change between versions
- Need to verify these classes still exist and have same signatures
- May need to find alternatives if APIs were removed

### 3. Test Compatibility

**Test Framework:**
- ScalaTest 3.2.19 (current) should work with Scala 2.13
- Mockito 4.11.0 should work with Java 17
- ScalaCheck 1.18.1 should work with Scala 2.13

**Test Utilities:**
- Spark test JARs (`spark-core`, `spark-catalyst`, `spark-sql` test-jars)
- These will automatically use Spark 4.0.0 versions
- May have API changes in test utilities

**Areas to Verify:**
- `SparkSession` creation in tests
- Test data generation
- Assertion methods
- Mock object creation

### 4. Runtime Behavior Changes

**ANSI SQL Mode:**
Spark 4.0.0 enables ANSI SQL mode by default (SPARK-44444). This may affect:
- Error handling behavior
- Type casting behavior
- NULL handling

**Impact on Connector:**
- The connector primarily passes through data
- Schema is fixed (data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp)
- Minimal impact expected, but should be tested

**Other Runtime Changes:**
- Structured logging framework (SPARK-47240) - May affect log output
- Query optimization changes - Should not affect connector logic
- Execution improvements - May improve performance

## Data Models

### Current Data Models

The connector uses these primary data models:

```scala
case class ShardInfo(
  shardId: String,
  iteratorType: String,
  iteratorPosition: String,
  isLast: Boolean
)

case class ShardOffsets(
  batchId: Long,
  streamName: String,
  shardInfoMap: Map[String, ShardInfo]
)

case class KinesisV2SourceOffset(shardsToOffsets: ShardOffsets) extends Offset

case class KinesisV2InputPartition(
  schema: StructType,
  batchId: Long,
  streamName: String,
  shardInfo: ShardInfo,
  prevShardInfo: Option[ShardInfo]
) extends InputPartition
```

**Schema:**
```scala
StructType(Seq(
  StructField("data", BinaryType),
  StructField("streamName", StringType),
  StructField("partitionKey", StringType),
  StructField("sequenceNumber", StringType),
  StructField("approximateArrivalTimestamp", TimestampType)
))
```

### Data Model Changes

**No changes expected** - These are internal data structures that don't depend on Spark version-specific features. However, we should verify:

1. `StructType` and `StructField` constructors haven't changed
2. `BinaryType`, `StringType`, `TimestampType` are still available
3. Serialization/deserialization of offsets still works
4. JSON parsing for offset recovery is compatible

## Error Handling

### Current Error Handling

The connector has error handling for:
- Kinesis API failures (retries with exponential backoff)
- Missing metadata (fail or warn based on `failOnDataLoss`)
- Shard synchronization issues
- Consumer registration/deregistration failures

### Error Handling Changes

**ANSI Mode Impact:**
With ANSI mode enabled by default, we need to ensure:
1. Type conversions don't throw unexpected errors
2. NULL handling is correct
3. Overflow behavior is acceptable

**Structured Logging:**
Spark 4.0.0 introduces structured logging (SPARK-47240). Consider:
1. Updating log statements to use structured format
2. Adding context to log messages
3. Using appropriate log levels

**Recommendations:**
- Keep existing error handling logic
- Add tests for ANSI mode behavior
- Update logging to structured format (optional enhancement)
- Ensure error messages are clear and actionable

## Testing Strategy

### Unit Tests

**Existing Tests to Verify:**
- `HDFSMetaDataCommitterSuite` - Metadata persistence
- `DynamodbMetadataCommitterSuite` - DynamoDB metadata
- `ShardSyncerSuite` - Shard synchronization logic
- `KinesisOptionsSuite` - Configuration parsing

**Testing Approach:**
1. Run existing unit tests with Spark 4.0.0
2. Fix compilation errors
3. Fix test failures due to API changes
4. Verify test coverage is maintained

### Integration Tests

**Existing Tests to Verify:**
- `KinesisSourceItSuite` - End-to-end source testing
- `KinesisSinkItSuite` - End-to-end sink testing
- `KinesisSourceReshardItSuite` - Resharding scenarios
- `KinesisSourceCrossAccountItSuite` - Cross-account access
- `EfoShardSubscriberItSuite` - Enhanced fan-out

**Testing Approach:**
1. Ensure integration tests compile
2. Run against real Kinesis streams (or LocalStack)
3. Verify all consumer types work (GetRecords, SubscribeToShard)
4. Test metadata committer types (HDFS, DynamoDB)
5. Verify graceful shutdown and consumer deregistration

### Compatibility Testing

**New Test Scenarios:**
1. **Version Compatibility Test:**
   - Verify connector works with Spark 4.0.0 runtime
   - Test with different Spark deployment modes (local, standalone, YARN, K8s)

2. **ANSI Mode Test:**
   - Verify behavior with ANSI mode enabled (default)
   - Test with ANSI mode disabled
   - Verify error handling is consistent

3. **Scala 2.13 Compatibility:**
   - Verify no runtime issues with Scala 2.13 collections
   - Test serialization/deserialization
   - Verify no class loading issues

### Performance Testing

**Baseline Comparison:**
- Compare throughput with Spark 3.5.5 vs 4.0.0
- Measure latency for micro-batches
- Monitor memory usage
- Check CPU utilization

**Expected Outcomes:**
- Performance should be similar or better
- Spark 4.0.0 has various optimizations that may improve performance
- No regressions expected

## Migration Path

### Phase 1: Build Configuration
1. Update `pom.xml` with new versions
2. Attempt compilation
3. Fix immediate compilation errors
4. Verify dependencies resolve correctly

### Phase 2: Code Updates
1. Fix Scala 2.13 compatibility issues
2. Update deprecated API usage
3. Fix type inference issues
4. Update imports if needed

### Phase 3: Test Updates
1. Fix test compilation errors
2. Update test utilities usage
3. Fix test failures
4. Add new tests if needed

### Phase 4: Validation
1. Run full test suite
2. Perform integration testing
3. Test with sample applications
4. Verify documentation is accurate

### Phase 5: Documentation
1. Update README with new versions
2. Update code examples if needed
3. Document any breaking changes
4. Update build instructions

## Risks and Mitigation

### Risk 1: Breaking API Changes
**Likelihood:** Medium  
**Impact:** High  
**Mitigation:**
- Thoroughly review Spark 4.0.0 release notes
- Test all DSv2 interfaces
- Have fallback plan to use compatibility shims if needed

### Risk 2: Scala 2.13 Incompatibilities
**Likelihood:** Medium  
**Impact:** Medium  
**Mitigation:**
- Review Scala 2.13 migration guide
- Fix compilation errors incrementally
- Test thoroughly with Scala 2.13 runtime

### Risk 3: Java 17 Runtime Issues
**Likelihood:** Low  
**Impact:** Medium  
**Mitigation:**
- Verify AWS SDK works with Java 17
- Test with Java 17 JVM
- Check for any JVM-specific issues

### Risk 4: Performance Regression
**Likelihood:** Low  
**Impact:** High  
**Mitigation:**
- Establish performance baseline
- Run performance tests
- Profile if issues are found
- Engage with Spark community if needed

### Risk 5: Internal API Changes
**Likelihood:** Medium  
**Impact:** Medium  
**Mitigation:**
- Minimize use of internal APIs
- Find public API alternatives
- Document any workarounds needed

## Dependencies

### Build Dependencies
- Maven 3.6.3+ (already required)
- Java 17 JDK
- Scala 2.13 compiler

### Runtime Dependencies
- Spark 4.0.0 (provided scope)
- AWS SDK v2 (current: 2.31.11) - Verify Java 17 compatibility
- Jackson 2.18.3 - Verify compatibility
- Netty (transitive) - Verify compatibility

### Test Dependencies
- ScalaTest 3.2.19 - Compatible with Scala 2.13
- Mockito 4.11.0 - Compatible with Java 17
- ScalaCheck 1.18.1 - Compatible with Scala 2.13

## Backward Compatibility

### User-Facing Changes

**Breaking Changes:**
1. Artifact name changes from `_2.12` to `_2.13`
2. Requires Java 17 minimum (was Java 8)
3. Requires Spark 4.0.0 (was 3.5.5)

**Non-Breaking Changes:**
- Configuration options remain the same
- API usage remains the same
- Checkpoint format should remain compatible (needs verification)

### Checkpoint Compatibility

**Critical Consideration:**
Users upgrading from Spark 3.x to 4.x need to be able to resume from existing checkpoints.

**Verification Needed:**
1. Test reading Spark 3.5.5 checkpoints with Spark 4.0.0
2. Verify offset serialization format is compatible
3. Test metadata committer compatibility (HDFS and DynamoDB)

**Recommendation:**
- Document checkpoint compatibility in release notes
- Provide migration guide if incompatibilities are found
- Consider supporting both formats during transition period

## Documentation Updates

### README.md Updates

1. **Version Requirements:**
   - Update Spark version to 4.0.0
   - Update Scala version to 2.13
   - Update Java requirement to 17

2. **Artifact Names:**
   - Change `_2.12` to `_2.13` in all examples
   - Update Maven coordinates
   - Update S3 jar file paths

3. **Build Instructions:**
   - Update Java version requirement
   - Update any version-specific instructions

4. **Code Examples:**
   - Review all code examples
   - Update if Spark 4.0.0 introduces relevant API changes
   - Verify examples compile and run

### Additional Documentation

1. **Migration Guide:**
   - Create guide for users upgrading from Spark 3.x
   - Document breaking changes
   - Provide troubleshooting tips

2. **Release Notes:**
   - Document all changes
   - Highlight breaking changes
   - Include upgrade instructions

3. **Compatibility Matrix:**
   - Document supported versions
   - Clarify Spark, Scala, and Java requirements

## Implementation Notes

### Scala 2.13 Specific Changes

**Collection Conversions:**
```scala
// Scala 2.12
import scala.collection.JavaConverters._

// Scala 2.13 (JavaConverters is deprecated, use CollectionConverters)
import scala.jdk.CollectionConverters._
```

**Parallel Collections:**
Scala 2.13 moved parallel collections to a separate module. If used:
```xml
<dependency>
  <groupId>org.scala-lang.modules</groupId>
  <artifactId>scala-parallel-collections_2.13</artifactId>
  <version>1.0.4</version>
</dependency>
```

**Type Inference:**
Scala 2.13 has improved type inference but may require explicit types in some cases where 2.12 inferred them.

### Java 17 Specific Changes

**Module System:**
Java 17 has stronger module encapsulation. May need to add JVM flags:
```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
```

**Reflection:**
Some reflection operations may require additional permissions.

**Performance:**
Java 17 has performance improvements, especially for:
- Garbage collection (G1GC improvements)
- JIT compilation
- String operations

## Success Criteria

1. **Build Success:**
   - Project compiles without errors
   - All dependencies resolve
   - Shaded JAR builds successfully

2. **Test Success:**
   - All unit tests pass
   - All integration tests pass
   - No test regressions

3. **Functional Success:**
   - Connector reads from Kinesis correctly
   - Connector writes to Kinesis correctly
   - Both consumer types work (GetRecords, EFO)
   - Both metadata committers work (HDFS, DynamoDB)
   - Graceful shutdown works

4. **Performance Success:**
   - No significant performance regression
   - Throughput is maintained or improved
   - Latency is maintained or improved

5. **Documentation Success:**
   - README is updated
   - Examples are updated
   - Migration guide is created
   - Release notes are complete

## Future Considerations

### Spark 4.0.0 New Features

Consider leveraging new Spark 4.0.0 features in future releases:

1. **VARIANT Data Type (SPARK-45827):**
   - Could be useful for flexible Kinesis data handling
   - Consider adding support in future version

2. **SQL Pipe Syntax (SPARK-49555):**
   - May simplify some query patterns
   - Document usage examples

3. **Improved Kubernetes Support (SPARK-49524):**
   - Test connector with Spark on K8s
   - Document any K8s-specific considerations

4. **Built-in XML Support (SPARK-44265):**
   - May be useful for users processing XML in Kinesis
   - Document integration patterns

### Deprecation Planning

1. **Legacy Sink Interface:**
   - Current `KinesisSink` uses legacy `Sink` interface
   - Consider migrating to DSv2 sink APIs
   - Plan deprecation timeline

2. **Internal API Usage:**
   - Minimize reliance on internal Spark APIs
   - Find public API alternatives
   - Reduce upgrade friction for future versions

## Conclusion

The upgrade to Spark 4.0.0 is primarily a dependency and build configuration update with some code adjustments for Scala 2.13 and Java 17 compatibility. The DSv2 API appears to be largely backward compatible, minimizing the code changes required.

The main work involves:
1. Updating build configuration (pom.xml)
2. Fixing Scala 2.13 compatibility issues
3. Verifying and updating Spark API usage
4. Ensuring all tests pass
5. Updating documentation

The risk is moderate, with the main concerns being potential breaking changes in DSv2 interfaces and Scala 2.13 compatibility issues. Thorough testing will be critical to ensure a successful upgrade.
