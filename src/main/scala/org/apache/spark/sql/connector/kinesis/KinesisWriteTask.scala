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

import java.nio.ByteBuffer
import java.util.Locale
import scala.util.Try
import com.amazonaws.services.kinesis.producer.KinesisProducer
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.MoreExecutors
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StringType

class KinesisWriteTask(caseInsensitiveParams: Map[String, String],
                      inputSchema: Seq[Attribute]) extends Logging {
  private var producer: KinesisProducer = _
  private val projection = createProjection
  private val streamName = caseInsensitiveParams(
    KinesisOptions.STREAM_NAME.toLowerCase(Locale.ROOT)
  )

  private val flushWaitTimeMills = Try(caseInsensitiveParams.getOrElse(
    KinesisOptions.SINK_FLUSH_WAIT_TIME_MILLIS.toLowerCase(Locale.ROOT),
    KinesisOptions.DEFAULT_SINK_FLUSH_WAIT_TIME_MILLIS
  ).toLong).getOrElse {
    throw new IllegalArgumentException(
      s"${KinesisOptions.SINK_FLUSH_WAIT_TIME_MILLIS} has to be a positive integer")
  }


  private var failedWrite: Throwable = _

  def execute(iterator: Iterator[InternalRow]): Unit = {
    producer = CachedKinesisProducer.getOrCreate(caseInsensitiveParams)
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      val projectedRow = projection(currentRow)
      val partitionKey = projectedRow.getString(0)
      val data = projectedRow.getBinary(1)
      val explicitHashKey = projectedRow.getString(2)

      sendData(partitionKey, explicitHashKey, data)
    }
  }

  def sendData(partitionKey: String, explicitHashKey: String, data: Array[Byte]): String = {
    var sentSeqNumbers = new String

    val future = explicitHashKey match {
      case "" => producer.addUserRecord(streamName, partitionKey, ByteBuffer.wrap(data))
      case _ => producer.addUserRecord(streamName, partitionKey, explicitHashKey, ByteBuffer.wrap(data))
    }

    val kinesisCallBack = new FutureCallback[UserRecordResult]() {

      override def onFailure(t: Throwable): Unit = {
        if (failedWrite == null && t!= null) {
          failedWrite = t
          logError(s"Writing to  $streamName failed due to ${t.getCause}")
        }
      }

      override def onSuccess(result: UserRecordResult): Unit = {
        val shardId = result.getShardId
        sentSeqNumbers = result.getSequenceNumber
      }
    }
    Futures.addCallback(future, kinesisCallBack, MoreExecutors.directExecutor())

    sentSeqNumbers
  }

  private def flushRecordsIfNecessary(): Unit = {
    if (producer != null) {
      while (producer.getOutstandingRecordsCount > 0) {
        try {
          producer.flush()
          Thread.sleep(flushWaitTimeMills)
        } catch {
          case e: InterruptedException =>
          // Do Nothing
        } finally {
          checkForErrors()
        }
      }
    }
  }

  def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  def close(): Unit = {
    checkForErrors()
    flushRecordsIfNecessary()
    checkForErrors()
    producer = null
  }

  private def createProjection: UnsafeProjection = {

    val partitionKeyExpression = inputSchema
      .find(_.name == KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME}' not found"))

    partitionKeyExpression.dataType match {
      case StringType | BinaryType => // ok
      case t =>
        throw new IllegalStateException(s"${KinesisWriter.PARTITION_KEY_ATTRIBUTE_NAME} " +
          "attribute type must be a String or BinaryType")
    }

    val dataExpression = inputSchema.find(_.name == KinesisWriter.DATA_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${KinesisWriter.DATA_ATTRIBUTE_NAME}' not found")
    )

    dataExpression.dataType match {
      case StringType | BinaryType => // ok
      case t =>
        throw new IllegalStateException(s"${KinesisWriter.DATA_ATTRIBUTE_NAME} " +
          "attribute type must be a String or BinaryType")
    }

    // check if explicitHashKey is in the inputSchema and use empty string otherwise
    val explicitHashKeyExpression = inputSchema
      .find(_.name == KinesisWriter.EXPLICIT_HASH_KEY_ATTRIBUTE_NAME).getOrElse(Literal(""))

    explicitHashKeyExpression.dataType match {
      case StringType | BinaryType => // ok
      case t =>
        throw new IllegalStateException(s"${KinesisWriter.EXPLICIT_HASH_KEY_ATTRIBUTE_NAME} " +
          "attribute type must be a String or BinaryType")
    }

    UnsafeProjection.create(
      Seq(
        Cast(partitionKeyExpression, StringType),
        Cast(dataExpression, StringType),
        Cast(explicitHashKeyExpression, StringType)
      ),
      inputSchema)
  }
}
