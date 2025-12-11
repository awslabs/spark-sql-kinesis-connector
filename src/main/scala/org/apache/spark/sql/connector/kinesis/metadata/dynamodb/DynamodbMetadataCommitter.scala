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

package org.apache.spark.sql.connector.kinesis.metadata.dynamodb


import java.time.Duration
import java.util
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.util.control.NonFatal

import com.google.common.collect.ImmutableMap
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import software.amazon.awssdk.services.dynamodb.model.Tag
import software.amazon.awssdk.utils.CollectionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.ConnectorDefaultCredentialsProvider
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitter
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter._
import org.apache.spark.sql.connector.kinesis.reportTimeTaken
import org.apache.spark.sql.connector.kinesis.retrieval.AWSExceptionManager

object DynamodbMetadataCommitter {
  val METADATA_PARTITION_KEY = "metadataKey"
  val METADATA_SORT_KEY = "metadataSortKey"
  val METADATA_VALUE = "metadataValue"

  private val METADATA_PARTITION_KEY_TYPE = ScalarAttributeType.S
  private val METADATA_SORT_KEY_TYPE = ScalarAttributeType.S

  private val CONSISTENT_READ = true
}

class DynamodbMetadataCommitter[T <: AnyRef : ClassTag](
      table: String,
      options: KinesisOptions,
      client: Option[DynamoDbAsyncClient] = None)
  extends MetadataCommitter[T] with Logging with Serializable {

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  var newTableCreated = false // only set to true if created a new table. Reuse existing is false
  private val tags: util.Collection[Tag] = DefaultSdkAutoConstructList.getInstance[Tag]

  private val dynamoDBClient = client.getOrElse(
    DynamoDbAsyncClient.builder
    .credentialsProvider(ConnectorDefaultCredentialsProvider().provider)
    .region(Region.of(options.region))
    .build
  )

  private val dynamoDbRequestTimeout: Duration = Duration.ofMinutes(3)

  if (createMetadataTableIfNotExists(BillingMode.PAY_PER_REQUEST)) {
    logInfo(s"Created new DynamoDB table ${table} with pay per request billing mode.")
  }

  val isTableActive: Boolean = waitUntilMetadataTableExists(
    options.waitTableActiveSecondsBetweenPolls,
    options.waitTableActiveTimeoutSeconds)
  if (!isTableActive) {
    throw new DynamodbCommitterException(
      s"${table} is not active after waiting for ${options.waitTableActiveTimeoutSeconds}",
      Some(new IllegalStateException("Creating table timeout")))
  }

  private def createMetadataTableIfNotExists(billingMode: BillingMode): Boolean = {
    val request = createTableRequestBuilder(billingMode).build
    createTableIfNotExists(request)
  }

  private def createTableRequestBuilder(billingMode: BillingMode) = {
    val builder = CreateTableRequest.builder
      .tableName(table)
      .keySchema(getKeySchema)
      .attributeDefinitions(getAttributeDefinitions)
      .tags(tags)
      .billingMode(billingMode)
    builder
  }

  private def getKeySchema: util.Collection[KeySchemaElement] = {
    val keySchema: util.List[KeySchemaElement] = new util.ArrayList[KeySchemaElement]
    keySchema.add(KeySchemaElement.builder.attributeName(METADATA_PARTITION_KEY).keyType(KeyType.HASH).build)
    keySchema.add(KeySchemaElement.builder.attributeName(METADATA_SORT_KEY).keyType(KeyType.RANGE).build)
    keySchema
  }

  private def getAttributeDefinitions: util.Collection[AttributeDefinition] = {
    val definitions = new util.ArrayList[AttributeDefinition]
    definitions.add(AttributeDefinition.builder.attributeName(METADATA_PARTITION_KEY).attributeType(METADATA_PARTITION_KEY_TYPE).build)
    definitions.add(AttributeDefinition.builder.attributeName(METADATA_SORT_KEY).attributeType(METADATA_SORT_KEY_TYPE).build)
    definitions
  }

  private def convertAndRethrowExceptions(message: String, e: Throwable): DynamodbCommitterException = {
    e match {
      case pe: ProvisionedThroughputExceededException =>
        throw new DynamodbCommitterProvisionedThroughpuException(s"${message} - Provisioned Throughput " +
          s"on the dynamodb table has been exceeded", Some(pe))
      case re: ResourceNotFoundException =>
        throw new DynamodbCommitterInvalidStateException(s"${message} - Resource not found", Some(re))
      case _ => new DynamodbCommitterException(message, Some(e))
    }
  }


  private def resolveOrCancelFuture[V](future: Future[V], timeout: Duration): V = {

    val exceptionManager = createExceptionManager
    exceptionManager.add(classOf[ResourceInUseException], (t: ResourceInUseException) => t)
    exceptionManager.add(classOf[LimitExceededException], (t: LimitExceededException) => t)
    exceptionManager.add(classOf[ResourceNotFoundException], (t: ResourceNotFoundException) => t)
    exceptionManager.add(classOf[ConditionalCheckFailedException], (t: ConditionalCheckFailedException) => t)
    exceptionManager.add(classOf[ProvisionedThroughputExceededException], (t: ProvisionedThroughputExceededException) => t)

    try {
      future.get(timeout.toMillis, TimeUnit.MILLISECONDS)
    } catch {
      case ee: ExecutionException => throw exceptionManager.apply(ee.getCause)
      case ie: InterruptedException =>
        throw new DynamodbCommitterException(
          s"Got InterruptedException for table ${table}.", Some(ie)
        )
      case te: TimeoutException =>
        future.cancel(true)
        throw te
    }
  }

  private def createTableIfNotExists(request: CreateTableRequest): Boolean = {
    try {
      if (getTableStatus != null) return newTableCreated
    }
    catch {
      case de: DynamodbCommitterException =>
        // Something went wrong with DynamoDB
        logError(s"Failed to get table status for ${table}", de)
        throw de
    }

    retryOrTimeout("createTableIfNotExists") {
      try {
        resolveOrCancelFuture(dynamoDBClient.createTable(request), dynamoDbRequestTimeout)
        newTableCreated = true
      } catch {
        case _: ResourceInUseException =>
          logInfo(s"table ${table} already exists.")
        case le: LimitExceededException =>
          throw new DynamodbCommitterProvisionedThroughpuException(s"Capacity exceeded when creating table ${table}", Some(le))
        case te: TimeoutException =>
          throw new DynamodbCommitterException(s"Get timeout exception in createTableIfNotExists(${table}).", Some(te))
        case dbe: DynamoDbException =>
          throw new DynamodbCommitterException(s"Got Exception for table ${table}.", Some(dbe))
      }
    }

    newTableCreated

  }

  private def metadataTableExists: Boolean = {
    val tableStatus = getTableStatus
    (TableStatus.ACTIVE == tableStatus) || (TableStatus.UPDATING == tableStatus)
  }

  private def getTableStatus: TableStatus = {
    val request = DescribeTableRequest.builder.tableName(table).build

    val result: DescribeTableResponse = retryOrTimeout("createTableIfNotExists") {
      try {
        resolveOrCancelFuture(dynamoDBClient.describeTable(request), dynamoDbRequestTimeout)
      }
      catch {
        case _: ResourceNotFoundException =>
          logInfo(s"Got ResourceNotFoundException for table ${table}.")
          null
        case e@(_: DynamoDbException | _: TimeoutException) =>
          throw convertAndRethrowExceptions(s"Got Exception for table ${table}", e)
      }
    }

    if (result != null) {
      val tableStatus: TableStatus = result.table.tableStatus
      logInfo(s"Metadata table exists and is in status ${tableStatus}")
      tableStatus
    } else null

  }

  private def waitUntilMetadataTableExists(secondsBetweenPolls: Long, timeoutSeconds: Long): Boolean = {
    var sleepTimeRemaining = TimeUnit.SECONDS.toMillis(timeoutSeconds)
    while (!metadataTableExists) {
      if (sleepTimeRemaining <= 0) return false
      val timeToSleepMillis = Math.min(TimeUnit.SECONDS.toMillis(secondsBetweenPolls), sleepTimeRemaining)
      sleepTimeRemaining -= timeToSleepMillis
      Thread.sleep(timeToSleepMillis)
    }
    if (newTableCreated) {
      logInfo("Metadata table was recently created.")
    }
    true
  }

  /**
   * Store the metadata for the specified batchId and shardId.
   * return `true` if successful.
   * If the metadata has already been stored, this method will return `false`.
   */
  override def add(batchId: Long, shardId: String, metadata: T): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a shard commit log")

    logInfo(s"Adding batchId ${batchId}, shardId ${shardId} with metadata ${metadata} to table ${table}")
    var addResult = true

    retryOrTimeout(s"add for batchId ${batchId}, shardId ${shardId}") {
      try {
        val partitionKey = batchIdToKey(batchId)
        val sortKey = shardId
        val data = serialize(metadata)

        val request = PutItemRequest.builder
          .tableName(table)
          .item(toDynamoItem(partitionKey, sortKey, data))
          .conditionExpression(s"attribute_not_exists(${METADATA_PARTITION_KEY})")
          .build

        resolveOrCancelFuture(dynamoDBClient.putItem(request), dynamoDbRequestTimeout)
        logDebug(s"Added batchId ${batchId}, shardId ${shardId} with metadata ${metadata}")

      } catch {
        case _: ConditionalCheckFailedException =>
          logInfo(s"Did not add batchId ${batchId}, shardId ${shardId} because it already exists.")
          addResult = false
        case e@(_: DynamoDbException | _: TimeoutException) =>
          throw convertAndRethrowExceptions(s"Got Exception when adding " +
            s"batchId ${batchId}, shardId ${shardId} with metadata ${metadata}.", e)
      }
    }

    addResult

  }

  override def exists(batchId: Long): Boolean = {
    logInfo(s"exists check from table ${table} for batchId ${batchId}")
    val result = retryOrTimeout(s"exists for batchId ${batchId}") {
      queryDataByBatchId(batchId, fromDynamoItem, Some(1))
    }
    logDebug(s"exists result length ${result.length} from table ${table} for batchId ${batchId}")
    result.nonEmpty
  }

  override def get(batchId: Long): Seq[T] = {
    logInfo(s"get data from table ${table} for batchId ${batchId}")
    val result = retryOrTimeout(s"get for batchId ${batchId}") {
      queryDataByBatchId(batchId, fromDynamoItem)
    }
    logDebug(s"get result size ${result.length} from table ${table} for batchId ${batchId}")
    result.toSeq
  }

  override def purgeBefore(thresholdBatchId: Long): Unit = {
    val batchIds = getDistinctPKs.map(pk => keyToBatchId(pk))

    for (batchId <- batchIds if batchId < thresholdBatchId) {
      logInfo(s"Removing metadata batchId: $batchId")
      delete(batchId)
    }
  }

  override def purge(numVersionsToRetain: Int): Unit = {
    val sortedBatchIds = getDistinctPKs
      .map(pk => keyToBatchId(pk))
      .sorted

    if (sortedBatchIds.isEmpty) return

    // Find the batches to delete
    val maxBatchId = sortedBatchIds.last
    val minBatchId = sortedBatchIds.head
    val minBatchIdToRetain =
      math.max(minBatchId, maxBatchId - numVersionsToRetain + 1)

    if (minBatchIdToRetain == minBatchId) {
      logInfo(s"Batches present: (min $minBatchId, max $maxBatchId)," +
        s"minBatchIdToRetain equals to minBatchId ${minBatchIdToRetain}. Do nothing.")
      return
    }

    logInfo(
      s"Batches present: (min $minBatchId, max $maxBatchId), " +
        s"remove all batches older than $minBatchIdToRetain to retain last " +
        s"$numVersionsToRetain versions")

    sortedBatchIds.takeWhile(_ < minBatchIdToRetain).toSet[Long].foreach { batchId =>
      delete(batchId)
      logDebug(s"Removed metadata batchId: $batchId")
    }

  }
  override def delete(batchId: Long): Boolean = {
    logInfo(s"Deleting batchId ${batchId} from table ${table}")

    val allItemsOfBatchId = retryOrTimeout(s"delete by batch id: scan data for batchId ${batchId}") {
      queryDataByBatchId(batchId, (t: util.Map[String, AttributeValue]) => t )
    }

    if (allItemsOfBatchId.nonEmpty) {
      allItemsOfBatchId.foreach { item =>
        val shardId = getDynamoString(item, METADATA_SORT_KEY)
        try {
          delete(batchId, shardId)
        } catch {
          case ie: InterruptedException =>
            throw new DynamodbCommitterException(
              s"Got InterruptedException for table ${table} batchId ${batchId}.", Some(ie)
            )

          case NonFatal(e) =>
            // delete is best effort. Log the error and then continue to next
            logError(s"delete by batch id: Got Exception when delete table ${table} batchId ${batchId} shardid ${shardId}", e)
        }
      }

      logDebug(s"Deleted batchId ${batchId} from table ${table}.")
      true
    } else {
      logInfo(s"Table ${table} BatchId ${batchId} doesn't exist.")
      false
    }


  }


  override def get(batchId: Long, shardId: String): Option[T] = {
    logInfo(s"get data from table ${table} for batchId ${batchId}, shardId ${shardId}")

    val keyValues = getKeyValues(batchIdToKey(batchId), shardId)

    val getRequest = GetItemRequest.builder
      .tableName(table)
      .key(keyValues)
      .consistentRead(CONSISTENT_READ)
      .build

    retryOrTimeout(s"get for batchId ${batchId} shardId ${shardId}") {
      try {
        val dynamoRecord = resolveOrCancelFuture(dynamoDBClient.getItem(getRequest), dynamoDbRequestTimeout)
          .item
        if (CollectionUtils.isNullOrEmpty(dynamoRecord)) {
          logInfo("No item found for batchId ${batchId}, shardId ${shardId}")
          None
        }
        else {
          val data = fromDynamoItem(dynamoRecord)
          Some(data)
        }
      }
      catch {
        case e@(_: DynamoDbException | _: TimeoutException) =>
          throw convertAndRethrowExceptions(s"Got Exception when get batchId ${batchId}, shardId ${shardId}", e)
      }
    }
  }

  override def delete(batchId: Long, shardId: String): Boolean = {
    logInfo(s"delete data from table ${table} for batchId ${batchId}, shardId ${shardId}")

    val keyValues = getKeyValues(batchIdToKey(batchId), shardId)

    val deleteRequest = DeleteItemRequest.builder
      .tableName(table)
      .key(keyValues)
      .conditionExpression(s"attribute_exists(${METADATA_PARTITION_KEY})")
      .build

    var deleteResult = true

    retryOrTimeout(s"delete for batchId ${batchId} shardId ${shardId}") {
      try {
        resolveOrCancelFuture(dynamoDBClient.deleteItem(deleteRequest), dynamoDbRequestTimeout)
      }
      catch {
        case _: ConditionalCheckFailedException =>
          logInfo(s"Did not delete batchId ${batchId}, shardId ${shardId} because it not exists.")
          deleteResult = false
        case e@(_: DynamoDbException | _: TimeoutException) =>
          throw convertAndRethrowExceptions(s"Got Exception when get batchId ${batchId}, shardId ${shardId}", e)
      }
    }

    deleteResult
  }

  private def getKeyValues(partitionKey: String, sortKey: String): util.HashMap[String, AttributeValue] = {
    val keyValues = new util.HashMap[String, AttributeValue]()
    keyValues.put(METADATA_PARTITION_KEY, AttributeValue.builder.s(partitionKey).build)
    keyValues.put(METADATA_SORT_KEY, AttributeValue.builder.s(sortKey).build)
    keyValues
  }

  // this method is from https://github.com/aws-samples/aws-dynamodb-examples/tree/master/PrintDistinctPKs/Printer/java
  private def getDistinctPKs: Seq[String] = {

    val result = mutable.ListBuffer.empty[String]

    // We need to create a string that is encoded in UTF-8 to 1024 bytes of the highest
    // code point.  This is 256 code points.  Each code point is a 4 byte value in UTF-8.
    // In Java, the code point needs to be specified as a surrogate pair of characters, thus
    // 512 characters.
    val sb = new StringBuilder(512)
    for (i <- 0 until 256) {
      sb.append("\uDBFF\uDFFF")
    }
    val maxSortKeyValueS = sb.toString
    val maxSortKeyValueN = "9.9999999999999999999999999999999999999E+125"
    val maxBytes = new Array[Byte](1024)
    util.Arrays.fill(maxBytes, 0xFF.toByte)
    val maxSortKeyValueB = SdkBytes.fromByteArray(maxBytes)
    var lastEvaluatedKey: util.Map[String, AttributeValue] = null

    breakable {
      while (true) {
        try {
          val scanRequest = ScanRequest.builder
            .tableName(table)
            .limit(1)
            .exclusiveStartKey(lastEvaluatedKey)
            .projectionExpression(METADATA_PARTITION_KEY)
            .build

          val response = resolveOrCancelFuture(dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout)
          if (!response.items.isEmpty) {
            result += response.items.get(0).get(METADATA_PARTITION_KEY).s
          }
          if (!response.hasLastEvaluatedKey) {
            break
          }
          lastEvaluatedKey = response.lastEvaluatedKey
          var maxSortKeyValue: AttributeValue = null
          METADATA_SORT_KEY_TYPE match {
            case ScalarAttributeType.S =>
              maxSortKeyValue = AttributeValue.builder.s(maxSortKeyValueS).build

            case ScalarAttributeType.N =>
              maxSortKeyValue = AttributeValue.builder.n(maxSortKeyValueN).build

            case ScalarAttributeType.B =>
              maxSortKeyValue = AttributeValue.builder.b(maxSortKeyValueB).build

            case _ =>
              throw new RuntimeException("Unsupported sort key type: " + METADATA_SORT_KEY_TYPE)
          }
          lastEvaluatedKey = new util.HashMap[String, AttributeValue](lastEvaluatedKey)
          lastEvaluatedKey.put(METADATA_SORT_KEY, maxSortKeyValue)
        } catch {
          case e@(_: DynamoDbException | _: TimeoutException) =>
            throw convertAndRethrowExceptions(s"Get exception for table ${table} when getDistinctPKs", e)
        }
      }
    }
    result.toSeq
  }

  // limit: specify the approximate number of items to return. It is a hint not a hard limit.
  private def queryDataByBatchId[U](batchId: Long,
                                   converter: util.Map[String, AttributeValue] => U,
                                   limit: Option[Int] = None) = reportTimeTaken("queryDataByBatchId") {
    var queryRequestBuilder = QueryRequest.builder.tableName(table)

    val expressionAttributeValues = ImmutableMap.of(
      ":p", AttributeValue.builder.s(batchIdToKey(batchId)).build
    )
    queryRequestBuilder = queryRequestBuilder
      .consistentRead(CONSISTENT_READ)
      .keyConditionExpression(s"${METADATA_PARTITION_KEY}= :p")
      .expressionAttributeValues(expressionAttributeValues)

    if (limit.isDefined) {
      logInfo(s"apply query limit ${limit.get}.")
      queryRequestBuilder = queryRequestBuilder.limit(limit.get)
    }

    var queryRequest = queryRequestBuilder.build

    try {
      logInfo(s"queryDataByBatchId- started from table ${table} " +
        s"for batchId ${batchId} with limit ${limit}.")
      var queryResult = resolveOrCancelFuture(dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout)
      val result = new util.ArrayList[U]
      while (queryResult != null) {
        for (item <- queryResult.items.asScala) {
          result.add(converter(item))
        }
        val lastEvaluatedKey = queryResult.lastEvaluatedKey
        if (CollectionUtils.isNullOrEmpty(lastEvaluatedKey)) {
          // Signify that we're done.
          queryResult = null
          logInfo(s"queryDataByBatchId- lastEvaluatedKey was null - scan finished for batchId ${batchId}.")
        } else if (limit.isDefined && result.size() >= limit.get) {
          queryResult = null
          logDebug(s"queryDataByBatchId- result size ${result.size()} >= limit ${limit.get} " +
            s"- scan finished for batchId ${batchId}.")
        } else {
          // Make another request, picking up where we left off.
          queryRequest = queryRequest.toBuilder.exclusiveStartKey(lastEvaluatedKey).build
          logDebug(s"queryDataByBatchId- lastEvaluatedKey was ${lastEvaluatedKey}, continuing query for batchId ${batchId}.")
          queryResult = resolveOrCancelFuture(dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout)
        }
      }
      logInfo(s"queryDataByBatchId- Listed ${result.size} item(s) from table ${table} " +
        s"for batchId ${batchId} with limit ${limit}.")
      result.asScala
    } catch {
      case e@(_: DynamoDbException | _: TimeoutException) =>
        throw convertAndRethrowExceptions(s"Get exception for table ${table} batchId ${batchId}", e)
    }

  }

  protected def serialize(metadata: T): String = {
    Serialization.write(metadata)
  }

  protected def deserialize(input: String): T = {
    Serialization.read[T](org.json4s.StringInput(input))
  }

  private def createExceptionManager = {
    val exceptionManager = new AWSExceptionManager
    exceptionManager.add(classOf[DynamoDbException], (t: DynamoDbException) => t)
    exceptionManager
  }

  protected def toDynamoItem(p: String, s: String, data: String): util.Map[String, AttributeValue] = {
    val result = new util.HashMap[String, AttributeValue]
    result.put(METADATA_PARTITION_KEY, createAttributeValue(p))
    result.put(METADATA_SORT_KEY, createAttributeValue(s))
    result.put(METADATA_VALUE, createAttributeValue(data))
    result
  }

  protected def fromDynamoItem(dynamoItem: util.Map[String, AttributeValue]): T = {
    deserialize(getDynamoString(dynamoItem, METADATA_VALUE))
  }

  private def getDynamoString(dynamoItem: util.Map[String, AttributeValue], key: String): String = {
    val av = dynamoItem.get(key)
    if (av == null) null
    else av.s
  }

  protected def createAttributeValue(stringValue: String): AttributeValue = {
    if (stringValue == null || stringValue.isEmpty) {
      throw new IllegalArgumentException("String attributeValues cannot be null or empty.")
    }
    AttributeValue.builder.s(stringValue).build
  }

  protected def batchIdToKey(batchId: Long): String = {
    batchId.toString
  }

  protected def keyToBatchId(key: String): Long = {
    key.toLong
  }

  /** Helper method to retry DynamoDB API request with exponential backoff and timeouts */
  private def retryOrTimeout[U](message: String)(body: => U): U = {

    var retryCount = 0
    var result: Option[U] = None
    var lastError: Throwable = null
    var waitTimeInterval = options.metadataRetryIntervalsMs

    def isMaxRetryDone = retryCount >= options.metadataNumRetries

    while (result.isEmpty && !isMaxRetryDone) {
      if (retryCount > 0) { // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval = scala.math.min(waitTimeInterval * 2, options.metadataMaxRetryIntervalMs)
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case pe: DynamodbCommitterProvisionedThroughpuException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", pe)
            case _ => throw t
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      throw new IllegalStateException(
        s"Gave up after $retryCount retries while $message, last exception: ", lastError)
    }
  }

}
