# Files Requiring Scala 2.13 Updates

## Summary
Total files with issues: 9  
Files with errors (blocking): 6  
Files with warnings only: 3  

## Critical Priority - Files with Compilation Errors

These files must be fixed before the project will compile:

### 1. ShardSyncer.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/ShardSyncer.scala`
- **Errors:** 1
- **Line:** 243
- **Issue:** Buffer to Seq conversion
- **Fix Required:** Add `.toSeq` to convert `Buffer[ShardInfo]` to `Seq[ShardInfo]`

### 2. KinesisClientConsumerImpl.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/client/KinesisClientConsumerImpl.scala`
- **Errors:** 1
- **Line:** 268
- **Issue:** Buffer to Seq conversion
- **Fix Required:** Add `.toSeq` to convert `Buffer[Shard]` to `Seq[Shard]`

### 3. DynamodbMetadataCommitter.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/metadata/dynamodb/DynamodbMetadataCommitter.scala`
- **Errors:** 2
- **Warnings:** 2
- **Lines:** 314, 518 (errors); 91, 94 (warnings)
- **Issues:** 
  - Line 314: Buffer to Seq conversion
  - Line 518: ListBuffer to Seq conversion
  - Line 91: Missing explicit type for implicit Formats
  - Line 94: Missing explicit type for implicit Manifest
- **Fix Required:** 
  - Add `.toSeq` conversions
  - Add explicit type annotations for implicits

### 4. RecordBatch.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/retrieval/RecordBatch.scala`
- **Errors:** 2
- **Lines:** 45, 50
- **Issue:** Buffer to Seq conversion
- **Fix Required:** Add `.toSeq` to convert `Buffer[KinesisUserRecord]` to `Seq[KinesisUserRecord]`

### 5. EfoRecordBatchPublisher.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/retrieval/efo/EfoRecordBatchPublisher.scala`
- **Errors:** 1
- **Line:** 57
- **Issue:** Buffer to Seq conversion
- **Fix Required:** Add `.toSeq` to convert `Buffer[Record]` to `Seq[Record]`

### 6. PollingRecordBatchPublisher.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/retrieval/polling/PollingRecordBatchPublisher.scala`
- **Errors:** 1
- **Line:** 82
- **Issue:** Buffer to Seq conversion
- **Fix Required:** Add `.toSeq` to convert `Buffer[Record]` to `Seq[Record]`

## Medium Priority - Files with Warnings Only

These files will compile but should be fixed to follow Scala 2.13 best practices:

### 7. KinesisModel.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/KinesisModel.scala`
- **Warnings:** 1
- **Line:** 161
- **Issue:** Missing explicit type for implicit Formats
- **Fix Required:** Add explicit type annotation: `implicit val formats: Formats = ...`

### 8. KinesisV2SourceOffset.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/KinesisV2SourceOffset.scala`
- **Warnings:** 1
- **Line:** 53
- **Issue:** Missing explicit type for implicit Formats
- **Fix Required:** Add explicit type annotation: `implicit val formats: Formats = ...`

### 9. HDFSMetadataCommitter.scala
- **Path:** `src/main/scala/org/apache/spark/sql/connector/kinesis/metadata/HDFSMetadataCommitter.scala`
- **Warnings:** 2
- **Lines:** 55, 58
- **Issues:**
  - Line 55: Missing explicit type for implicit Formats
  - Line 58: Missing explicit type for implicit Manifest
- **Fix Required:** 
  - Add explicit type annotation: `implicit val formats: Formats = ...`
  - Add explicit type annotation: `implicit def manifest: Manifest[T] = ...`

## Files by Component

### Core Connector
- ShardSyncer.scala (1 error)

### Client Layer
- KinesisClientConsumerImpl.scala (1 error)

### Metadata Management
- DynamodbMetadataCommitter.scala (2 errors, 2 warnings)
- HDFSMetadataCommitter.scala (2 warnings)

### Data Retrieval
- RecordBatch.scala (2 errors)
- EfoRecordBatchPublisher.scala (1 error)
- PollingRecordBatchPublisher.scala (1 error)

### Offset Management
- KinesisModel.scala (1 warning)
- KinesisV2SourceOffset.scala (1 warning)

## Quick Reference - Error Locations

```
src/main/scala/org/apache/spark/sql/connector/kinesis/
├── ShardSyncer.scala:243 [ERROR]
├── KinesisModel.scala:161 [WARNING]
├── KinesisV2SourceOffset.scala:53 [WARNING]
├── client/
│   └── KinesisClientConsumerImpl.scala:268 [ERROR]
├── metadata/
│   ├── HDFSMetadataCommitter.scala:55,58 [WARNING]
│   └── dynamodb/
│       └── DynamodbMetadataCommitter.scala:91,94,314,518 [ERROR+WARNING]
└── retrieval/
    ├── RecordBatch.scala:45,50 [ERROR]
    ├── efo/
    │   └── EfoRecordBatchPublisher.scala:57 [ERROR]
    └── polling/
        └── PollingRecordBatchPublisher.scala:82 [ERROR]
```

## Estimated Fix Effort

- **Errors (8 total):** ~30 minutes
  - Simple `.toSeq` additions
  - Straightforward fixes
  
- **Warnings (6 total):** ~20 minutes
  - Add explicit type annotations
  - Copy inferred types from warnings

- **Total Estimated Time:** ~50 minutes

## Notes

- All errors are related to Scala 2.13's stricter collection type system
- No Spark API compatibility issues detected in this compilation
- No Java 17 compatibility issues detected
- All fixes are mechanical and low-risk
- After fixing these issues, a full recompilation will be needed to check for any additional issues
