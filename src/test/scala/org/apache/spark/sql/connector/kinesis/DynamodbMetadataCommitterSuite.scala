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

package org.apache.spark.sql.connector.kinesis

import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.language.implicitConversions

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.QueryResponse
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.TableDescription

import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbCommitterException
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbCommitterInvalidStateException
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter.METADATA_PARTITION_KEY
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter.METADATA_SORT_KEY
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter.METADATA_VALUE

class DynamodbMetadataCommitterSuite extends KinesisTestBase {

  val tableName = "testtable"
  val waitTableActiveTimeoutSeconds = 6
  val waitTableActiveSecondsBetweenPolls = 2

  val batchId = 1
  val shardId = "001"
  val metadata = "test1"

  private implicit val formats: org.json4s.Formats = Serialization.formats(NoTypeHints)

  test("Dynamodb Committer init describeTable timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val te = new TimeoutException("Timeout")

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)
    val exception = intercept[DynamodbCommitterException] {
      new DynamodbMetadataCommitter(tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    }

    exception.getMessage should include (s"Got Exception for table ${tableName}")
    verify(mockDescribeTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).describeTable(any(classOf[DescribeTableRequest]))
  }

  test("Dynamodb Committer init createTable timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val mockCreateTableFuture = mock(classOf[CompletableFuture[CreateTableResponse]])

    val te = new TimeoutException("Timeout")
    val rnfe = ResourceNotFoundException.builder().message("resource not found").build

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)
    when(mockClient.createTable(any(classOf[CreateTableRequest]))).thenReturn(mockCreateTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rnfe))

    when(mockCreateTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val exception = intercept[DynamodbCommitterException] {
      new DynamodbMetadataCommitter(tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    }

    exception.getMessage should include(s"Get timeout exception in createTableIfNotExists(${tableName}).")
    verify(mockDescribeTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockCreateTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).describeTable(any(classOf[DescribeTableRequest]))
    verify(mockClient, times(1)).createTable(any(classOf[CreateTableRequest]))
  }

  test("Dynamodb Committer init createTable InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val mockCreateTableFuture = mock(classOf[CompletableFuture[CreateTableResponse]])

    val ie = new InterruptedException("interrupted")
    val rnfe = ResourceNotFoundException.builder().message("resource not found").build

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)
    when(mockClient.createTable(any(classOf[CreateTableRequest]))).thenReturn(mockCreateTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rnfe))
    when(mockCreateTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val exception = intercept[DynamodbCommitterException] {
      new DynamodbMetadataCommitter(tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}")
    verify(mockDescribeTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockCreateTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).describeTable(any(classOf[DescribeTableRequest]))
    verify(mockClient, times(1)).createTable(any(classOf[CreateTableRequest]))
  }

  test("Dynamodb Committer init createTable ResourceInUseException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val mockCreateTableFuture = mock(classOf[CompletableFuture[CreateTableResponse]])

    val rnfe = ResourceNotFoundException.builder().message("resource not found").build
    val riue = ResourceInUseException.builder().message("resource in use").build

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)
    when(mockClient.createTable(any(classOf[CreateTableRequest]))).thenReturn(mockCreateTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rnfe))
      .thenReturn(
        DescribeTableResponse.builder().table(
          TableDescription.builder()
            .tableName(tableName)
            .tableStatus("ACTIVE")
            .build
        )
      .build

      )
    when(mockCreateTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(riue))

    val committer = new DynamodbMetadataCommitter(tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    committer.newTableCreated shouldBe false

    verify(mockDescribeTableFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockCreateTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(2)).describeTable(any(classOf[DescribeTableRequest]))
    verify(mockClient, times(1)).createTable(any(classOf[CreateTableRequest]))
  }

  test("Dynamodb Committer init createTable LimitExceededException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val mockCreateTableFuture = mock(classOf[CompletableFuture[CreateTableResponse]])

    val rnfe = ResourceNotFoundException.builder().message("resource not found").build
    val lee = LimitExceededException.builder().message("limit exceeded").build

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)
    when(mockClient.createTable(any(classOf[CreateTableRequest]))).thenReturn(mockCreateTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rnfe))
      .thenReturn(
        DescribeTableResponse.builder().table(
          TableDescription.builder()
            .tableName(tableName)
            .tableStatus("ACTIVE")
            .build
        ).build
    )
    when(mockCreateTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(lee))
      .thenReturn(
        CreateTableResponse.builder().tableDescription(
          TableDescription.builder()
            .tableName(tableName)
            .build
        ).build
    )

    val committer = new DynamodbMetadataCommitter(tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    committer.newTableCreated shouldBe true

    verify(mockDescribeTableFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockCreateTableFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(2)).describeTable(any(classOf[DescribeTableRequest]))
    verify(mockClient, times(2)).createTable(any(classOf[CreateTableRequest]))
  }

  test("Dynamodb Committer init createTable table is not active") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    val mockCreateTableFuture = mock(classOf[CompletableFuture[CreateTableResponse]])

    val rnfe = ResourceNotFoundException.builder().message("resource not found").build

    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)
    when(mockClient.createTable(any(classOf[CreateTableRequest]))).thenReturn(mockCreateTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rnfe))
      .thenReturn(
        DescribeTableResponse.builder().table(
          TableDescription.builder()
            .tableName(tableName)
            .tableStatus("DELETING")
            .build
        ).build
      )
    when(mockCreateTableFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenReturn(
        CreateTableResponse.builder().tableDescription(
          TableDescription.builder()
            .tableName(tableName)
            .build
        ).build
      )

    val exception = intercept[DynamodbCommitterException] {
      new DynamodbMetadataCommitter(tableName,
        DEFAULT_KINESIS_OPTIONS.copy(
          waitTableActiveTimeoutSeconds = waitTableActiveTimeoutSeconds,
          waitTableActiveSecondsBetweenPolls = waitTableActiveSecondsBetweenPolls
        ),
        Some(mockClient))
    }

    exception.getMessage should include(s"${tableName} is not active after waiting for ${waitTableActiveTimeoutSeconds}")

    verify(mockDescribeTableFuture, times(5)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(5)).describeTable(any(classOf[DescribeTableRequest]))
    verify(mockCreateTableFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).createTable(any(classOf[CreateTableRequest]))
  }

  test("Dynamodb Committer add metadata with timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockPutItemFuture = mock(classOf[CompletableFuture[PutItemResponse]])

    val te = new TimeoutException("Timeout")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.putItem(any(classOf[PutItemRequest]))).thenReturn(mockPutItemFuture)

    when(mockPutItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val exception = intercept[DynamodbCommitterException] {
      committer.add(batchId, shardId, metadata)
    }

    exception.getMessage should include (s"Got Exception when adding " +
      s"batchId ${batchId}, shardId ${shardId} with metadata ${metadata}.")

    verify(mockPutItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).putItem(any(classOf[PutItemRequest]))
  }

  test("Dynamodb Committer add metadata with InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockPutItemFuture = mock(classOf[CompletableFuture[PutItemResponse]])

    val ie = new InterruptedException("interrupted")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.putItem(any(classOf[PutItemRequest]))).thenReturn(mockPutItemFuture)

    when(mockPutItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterException] {
      committer.add(batchId, shardId, metadata)
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}.")

    verify(mockPutItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).putItem(any(classOf[PutItemRequest]))
  }

  test("Dynamodb Committer add metadata with ProvisionedThroughputExceededException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockPutItemFuture = mock(classOf[CompletableFuture[PutItemResponse]])

    val ptee = ProvisionedThroughputExceededException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.putItem(any(classOf[PutItemRequest]))).thenReturn(mockPutItemFuture)

    when(mockPutItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(ptee))
      .thenReturn(
        PutItemResponse.builder()
          .build()
      )

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val result = committer.add(batchId, shardId, metadata)

    result shouldBe true

    verify(mockPutItemFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(2)).putItem(any(classOf[PutItemRequest]))
  }

  test("Dynamodb Committer add metadata with ConditionalCheckFailedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockPutItemFuture = mock(classOf[CompletableFuture[PutItemResponse]])

    val ccfe = ConditionalCheckFailedException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.putItem(any(classOf[PutItemRequest]))).thenReturn(mockPutItemFuture)

    when(mockPutItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(ccfe))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val result = committer.add(batchId, shardId, metadata)

    result shouldBe false

    verify(mockPutItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).putItem(any(classOf[PutItemRequest]))
  }

  test("Dynamodb Committer add meta with ResourceNotFoundException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockPutItemFuture = mock(classOf[CompletableFuture[PutItemResponse]])

    val rne = ResourceNotFoundException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.putItem(any(classOf[PutItemRequest]))).thenReturn(mockPutItemFuture)

    when(mockPutItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rne))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterInvalidStateException] {
      committer.add(batchId, shardId, metadata)
    }

    exception.getMessage should include("Resource not found")

    verify(mockPutItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).putItem(any(classOf[PutItemRequest]))
  }

  test("Dynamodb Committer get batch and shard with timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockGetItemFuture = mock(classOf[CompletableFuture[GetItemResponse]])

    val te = new TimeoutException("Timeout")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.getItem(any(classOf[GetItemRequest]))).thenReturn(mockGetItemFuture)

    when(mockGetItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val exception = intercept[DynamodbCommitterException] {
      committer.get(batchId, shardId)
    }

    exception.getMessage should include(s"Got Exception when get batchId ${batchId}, shardId ${shardId}")

    verify(mockGetItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).getItem(any(classOf[GetItemRequest]))
  }

  test("Dynamodb Committer get batch and shard with InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockGetItemFuture = mock(classOf[CompletableFuture[GetItemResponse]])

    val ie = new InterruptedException("interrupted")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.getItem(any(classOf[GetItemRequest]))).thenReturn(mockGetItemFuture)

    when(mockGetItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterException] {
      committer.get(batchId, shardId)
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}.")

    verify(mockGetItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).getItem(any(classOf[GetItemRequest]))
  }

  test("Dynamodb Committer get batch and shard with ResourceNotFoundException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockGetItemFuture = mock(classOf[CompletableFuture[GetItemResponse]])

    val rne = ResourceNotFoundException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.getItem(any(classOf[GetItemRequest]))).thenReturn(mockGetItemFuture)

    when(mockGetItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rne))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterInvalidStateException] {
      committer.get(batchId, shardId)
    }

    exception.getMessage should include("Resource not found")

    verify(mockGetItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).getItem(any(classOf[GetItemRequest]))
  }

  test("Dynamodb Committer get batch with timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])

    val te = new TimeoutException("Timeout")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    when(mockQueryFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val exception = intercept[DynamodbCommitterException] {
      committer.get(batchId)
    }

    exception.getMessage should include(s"Get exception for table ${tableName} batchId ${batchId}")

    verify(mockQueryFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).query(any(classOf[QueryRequest]))
  }

  test("Dynamodb Committer get batch with InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])

    val ie = new InterruptedException("interrupted")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    when(mockQueryFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterException] {
      committer.get(batchId)
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}.")

    verify(mockQueryFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).query(any(classOf[QueryRequest]))
  }

  test("Dynamodb Committer get batch with ResourceNotFoundException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])

    val rne = ResourceNotFoundException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    when(mockQueryFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rne))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterInvalidStateException] {
      committer.get(batchId)
    }

    exception.getMessage should include("Resource not found")

    verify(mockQueryFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).query(any(classOf[QueryRequest]))
  }

  def mockSuccessDescribeTable(mockClient: DynamoDbAsyncClient): Unit = {
    val mockDescribeTableFuture = mock(classOf[CompletableFuture[DescribeTableResponse]])
    when(mockClient.describeTable(any(classOf[DescribeTableRequest]))).thenReturn(mockDescribeTableFuture)

    when(mockDescribeTableFuture.get(anyLong, any(classOf[TimeUnit])))

      .thenReturn(
        DescribeTableResponse.builder().table(
          TableDescription.builder()
            .tableName(tableName)
            .tableStatus("ACTIVE")
            .build
        ).build
      )
  }

  def mockQueryData(mockQueryFuture: CompletableFuture[QueryResponse]): Unit = {
    val lastEvaluatedKey: util.Map[String, AttributeValue] = new util.HashMap[String, AttributeValue]
    val item1: util.Map[String, AttributeValue] = new util.HashMap[String, AttributeValue]
    val item2: util.Map[String, AttributeValue] = new util.HashMap[String, AttributeValue]
    lastEvaluatedKey.put("Test", AttributeValue.builder.s("test").build)

    item1.put(METADATA_PARTITION_KEY, AttributeValue.builder.s(batchId.toString).build)
    item1.put(METADATA_SORT_KEY, AttributeValue.builder.s(shardId).build)
    item1.put(METADATA_VALUE, AttributeValue.builder.s(Serialization.write(metadata)).build)
    item2.put(METADATA_PARTITION_KEY, AttributeValue.builder.s(batchId.toString).build)
    item2.put(METADATA_SORT_KEY, AttributeValue.builder.s("0002").build)
    item2.put(METADATA_VALUE, AttributeValue.builder.s(Serialization.write("test2")).build)

    when(mockQueryFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenReturn(
        QueryResponse.builder()
          .lastEvaluatedKey(lastEvaluatedKey)
          .items(item1)
          .build()
      )
      .thenReturn(
        QueryResponse.builder()
          .items(item2)
          .build()
      )
  }

  test("Dynamodb Committer get batch successfully") {

    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])

    mockSuccessDescribeTable(mockClient)
    mockQueryData(mockQueryFuture)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val result = committer.get(batchId)

    result.length shouldBe 2
    result.head shouldBe metadata
    result(1) shouldBe "test2"


    verify(mockQueryFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(2)).query(any(classOf[QueryRequest]))
  }

  test("Dynamodb Committer exist should only query once") {


    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])

    mockSuccessDescribeTable(mockClient)
    mockQueryData(mockQueryFuture)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val result = committer.exists(batchId)

    result shouldBe true


    verify(mockQueryFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).query(any(classOf[QueryRequest]))
  }

  test("Dynamodb Committer delete batch and shard with timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDeleteItemFuture = mock(classOf[CompletableFuture[DeleteItemResponse]])

    val te = new TimeoutException("Timeout")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.deleteItem(any(classOf[DeleteItemRequest]))).thenReturn(mockDeleteItemFuture)

    when(mockDeleteItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val exception = intercept[DynamodbCommitterException] {
      committer.delete(batchId, shardId)
    }

    exception.getMessage should include(s"Got Exception when get batchId ${batchId}, shardId ${shardId}")

    verify(mockDeleteItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).deleteItem(any(classOf[DeleteItemRequest]))
  }

  test("Dynamodb Committer delete batch and shard with InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDeleteItemFuture = mock(classOf[CompletableFuture[DeleteItemResponse]])

    val ie = new InterruptedException("interrupted")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.deleteItem(any(classOf[DeleteItemRequest]))).thenReturn(mockDeleteItemFuture)

    when(mockDeleteItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterException] {
      committer.delete(batchId, shardId)
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}.")

    verify(mockDeleteItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).deleteItem(any(classOf[DeleteItemRequest]))
  }

  test("Dynamodb Committer delete batch and shard with ResourceNotFoundException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDeleteItemFuture = mock(classOf[CompletableFuture[DeleteItemResponse]])

    val rne = ResourceNotFoundException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.deleteItem(any(classOf[DeleteItemRequest]))).thenReturn(mockDeleteItemFuture)

    when(mockDeleteItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rne))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterInvalidStateException] {
      committer.delete(batchId, shardId)
    }

    exception.getMessage should include("Resource not found")

    verify(mockDeleteItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).deleteItem(any(classOf[DeleteItemRequest]))
  }

  test("Dynamodb Committer delete batch and shard with ConditionalCheckFailedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockDeleteItemFuture = mock(classOf[CompletableFuture[DeleteItemResponse]])

    val ccfe = ConditionalCheckFailedException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.deleteItem(any(classOf[DeleteItemRequest]))).thenReturn(mockDeleteItemFuture)

    when(mockDeleteItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(ccfe))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val result = committer.delete(batchId, shardId)

    result shouldBe false

    verify(mockDeleteItemFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).deleteItem(any(classOf[DeleteItemRequest]))
  }

  test("Dynamodb Committer delete batch should ignore errors") {

    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockQueryFuture = mock(classOf[CompletableFuture[QueryResponse]])
    val mockDeleteItemFuture = mock(classOf[CompletableFuture[DeleteItemResponse]])

    val te = new TimeoutException("Timeout")


    when(mockClient.deleteItem(any(classOf[DeleteItemRequest]))).thenReturn(mockDeleteItemFuture)

    when(mockDeleteItemFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    mockSuccessDescribeTable(mockClient)
    mockQueryData(mockQueryFuture)
    when(mockClient.query(any(classOf[QueryRequest]))).thenReturn(mockQueryFuture)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val result = committer.delete(batchId)

    result shouldBe true


    verify(mockQueryFuture, times(2)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(2)).query(any(classOf[QueryRequest]))
    verify(mockClient, times(2)).deleteItem(any(classOf[DeleteItemRequest]))
  }

  test("Dynamodb Committer purge with timeout exception") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockScanFuture = mock(classOf[CompletableFuture[ScanResponse]])

    val te = new TimeoutException("Timeout")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.scan(any(classOf[ScanRequest]))).thenReturn(mockScanFuture)

    when(mockScanFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(te)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))
    val exception = intercept[DynamodbCommitterException] {
      committer.purge(1)
    }

    exception.getMessage should include(s"Get exception for table ${tableName} when getDistinctPKs")

    verify(mockScanFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).scan(any(classOf[ScanRequest]))
  }

  test("Dynamodb Committer purge with InterruptedException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockScanFuture = mock(classOf[CompletableFuture[ScanResponse]])

    val ie = new InterruptedException("interrupted")

    mockSuccessDescribeTable(mockClient)
    when(mockClient.scan(any(classOf[ScanRequest]))).thenReturn(mockScanFuture)

    when(mockScanFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(ie)

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterException] {
      committer.purge(1)
    }

    exception.getMessage should include(s"Got InterruptedException for table ${tableName}")

    verify(mockScanFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).scan(any(classOf[ScanRequest]))
  }

  test("Dynamodb Committer purge with ResourceNotFoundException") {
    val mockClient = mock(classOf[DynamoDbAsyncClient])
    val mockScanFuture = mock(classOf[CompletableFuture[ScanResponse]])

    val rne = ResourceNotFoundException.builder().build()

    mockSuccessDescribeTable(mockClient)
    when(mockClient.scan(any(classOf[ScanRequest]))).thenReturn(mockScanFuture)

    when(mockScanFuture.get(anyLong, any(classOf[TimeUnit])))
      .thenThrow(new ExecutionException(rne))

    val committer = new DynamodbMetadataCommitter[String](tableName, DEFAULT_KINESIS_OPTIONS, Some(mockClient))

    val exception = intercept[DynamodbCommitterInvalidStateException] {
      committer.purge(1)
    }

    exception.getMessage should include("Resource not found")

    verify(mockScanFuture, times(1)).get(anyLong, any(classOf[TimeUnit]))
    verify(mockClient, times(1)).scan(any(classOf[ScanRequest]))
  }
}

