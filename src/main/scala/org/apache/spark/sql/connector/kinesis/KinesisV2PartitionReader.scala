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

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import software.amazon.awssdk.services.kinesis.model.Shard
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitter
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitterFactory
import org.apache.spark.sql.connector.kinesis.retrieval.DataReceiver
import org.apache.spark.sql.connector.kinesis.retrieval.KinesisUserRecord
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisher
import org.apache.spark.sql.connector.kinesis.retrieval.RecordBatchPublisherFactory
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber
import org.apache.spark.sql.connector.kinesis.retrieval.ShardConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.StreamShard
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator
import org.apache.spark.util.SerializableConfiguration

class KinesisV2PartitionReader (schema: StructType,
                                sourcePartition: KinesisV2InputPartition,
                                streamName: String,
                                endpointUrl: String,
                                consumerArn: String,
                                checkpointLocation: String,
                                batchId: Long,
                                kinesisOptions: KinesisOptions,
                                conf: SerializableConfiguration
                               )
  extends PartitionReader[InternalRow]
    with DataReceiver
    with Logging {

  logInfo(s"KinesisV2PartitionReader for batch ${sourcePartition.batchId} with " +
    s"sourcePartition start - ${sourcePartition.startShardInfo}, stop - ${sourcePartition.stopShardInfo}")
  
  private val kinesisShardId = sourcePartition.startShardInfo.shardId
  private val startTimestamp: Long = System.currentTimeMillis
  
  val kinesisStreamShard: StreamShard = StreamShard(streamName, Shard.builder().shardId(kinesisShardId).build())
  val kinesisPosition: KinesisPosition = KinesisPosition.make(sourcePartition.startShardInfo.iteratorType,
    sourcePartition.startShardInfo.iteratorPosition,
    sourcePartition.startShardInfo.subSequenceNumber,
    sourcePartition.startShardInfo.isLast
  )

  val kinesisReader: KinesisClientConsumer = KinesisClientFactory.createConsumer(
    kinesisOptions,
    streamName,
    endpointUrl
  )

  val metadataCommitter: MetadataCommitter[ShardInfo] = MetadataCommitterFactory.createMetadataCommitter(
    kinesisOptions, checkpointLocation, Some(conf)
  )

  private[this] val errorRef: AtomicReference[Throwable] = new AtomicReference[Throwable]
  
  var lastReadTimeMs: Long = 0
  var lastReadSequenceNumber: SequenceNumber = _
  var numRecordRead: Long = 0

  val closed: AtomicBoolean = new AtomicBoolean(false)

  val hasShardClosed = new AtomicBoolean(false)

  private val queueCapacity = kinesisOptions.dataQueueCapacity
  private val dataQueueWaitTimeout = kinesisOptions.dataQueueWaitTimeout
  private val maxDataQueueEmptyCount = kinesisOptions.maxDataQueueEmptyCount

  private val dataQueue: BlockingQueue[KinesisUserRecord] = new ArrayBlockingQueue[KinesisUserRecord](queueCapacity);

  val recordBatchPublisher: RecordBatchPublisher = RecordBatchPublisherFactory.create(
    kinesisPosition,
    consumerArn,
    kinesisStreamShard,
    kinesisReader,
    kinesisOptions,
    isRunning
  )

  val shardConsumersExecutor: ExecutorService = createShardConsumersThreadPool("KinesisV2PartitionReader")

  val shardConsumer = new ShardConsumer(this, recordBatchPublisher)

  shardConsumersExecutor.submit(shardConsumer)

  private def hasTimeForMoreRecords(currentTimestamp: Long): Boolean = {
    // always return true if kinesisOptions.maxFetchTimePerShardSec is None
    kinesisOptions.maxFetchTimePerShardSec.forall { maxFetchTimePerShardSec =>
      (currentTimestamp - startTimestamp) < (maxFetchTimePerShardSec * 1000L)
    }
  }
  private def createShardConsumersThreadPool(taskName: String): ExecutorService = Executors.newCachedThreadPool(
    new ThreadFactory() {
      final private val threadCount = new AtomicLong(0)

      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable)
        thread.setName("shardConsumers-" + taskName + "-thread-" + threadCount.getAndIncrement)
        thread.setDaemon(true)
        thread
      }
  })

  override def isRunning: Boolean = !closed.get()

  // updateState currently doing nothing
  override def updateState(streamShard: StreamShard, sequenceNumber: SequenceNumber): Unit = {}

  override def enqueueRecord(streamShard: StreamShard, record: KinesisUserRecord): Boolean = {

    logDebug(s"enqueueRecord ${streamShard}")

    if (!isRunning) return false

    val putResult = dataQueue.offer(record, dataQueueWaitTimeout.getSeconds, TimeUnit.SECONDS)

    if (putResult) {
      if (KinesisUserRecord.nonEmptyUserRecord(record)) {
        updateState(streamShard, record.sequenceNumber)
      } else {
        logDebug(s"put empty record with millisBehindLatest ${record.millisBehindLatest} to ${streamShard}'s data queue'")
      }
    } else {
      logWarning(s"fail to enqueue record for ${streamShard}")
    }

    putResult
  }

   // Called by created threads to pass on errors. Only the first thrown error is set.
  override def stopWithError(throwable: Throwable): Unit = {
    logError(s"stopWithError for ${sourcePartition.startShardInfo}:", throwable)
    
    if (this.errorRef.compareAndSet(null, throwable)) {
      logInfo(s"stopWithError set errorRef")
    }
  }

  val underlying: Iterator[InternalRow] = new NextIterator[InternalRow]() {
    var fetchNext = true
    var lastEmptyCnt = 0

    logDebug(s"NextIterator constructor")

    private def reachStopShardInfo(userRecord: KinesisUserRecord, stopShardInfo: Option[ShardInfo]): Boolean = {
      logDebug(s"reachStopShardInfo with userRecord ${userRecord.sequenceNumber}," +
        s" inputPartition stopShardInfo ${stopShardInfo}")

      if(stopShardInfo.isEmpty) false
      else if (!userRecord.fromAggregated
        && stopShardInfo.get.iteratorType == AfterSequenceNumber.iteratorType) {
        userRecord.sequenceNumber.sequenceNumber == stopShardInfo.get.iteratorPosition
      }
      else if (userRecord.fromAggregated
        && stopShardInfo.get.iteratorType == AfterSequenceNumber.iteratorType) {
        (userRecord.sequenceNumber.sequenceNumber == stopShardInfo.get.iteratorPosition) &&
          (userRecord.sequenceNumber.subSequenceNumber == stopShardInfo.get.subSequenceNumber)
      }
      else false
    }

    override def getNext(): InternalRow = {

      logDebug(s"NextIterator getNext()")
      
      var emptyCnt = lastEmptyCnt
      var fetchedRecord: Option[KinesisUserRecord] = None

      def pollNextUserRecord(): Unit = {
        val userRecord = dataQueue.poll(dataQueueWaitTimeout.getSeconds, TimeUnit.SECONDS)
        if (userRecord == null) {
          logDebug(s"getNext emptyCnt ${emptyCnt}")
          emptyCnt += 1
          if (emptyCnt >= maxDataQueueEmptyCount) {
            logInfo(s"getNext emptyCnt ${emptyCnt} >= ${maxDataQueueEmptyCount}. Stop fetchNext.")
            fetchNext = false
          }
        } else if (KinesisUserRecord.shardEndUserRecord(userRecord)) {
          logInfo(s"Got shard end user record for ${kinesisStreamShard}")
          hasShardClosed.set(true)
          fetchNext = false
        } else if (KinesisUserRecord.emptyUserRecord(userRecord)) {
          logInfo(s"Got empty user record with millisBehindLatest ${userRecord.millisBehindLatest} for ${kinesisPosition}")

          if (userRecord.millisBehindLatest > 0) {
            // when the stream not receiving new data for a long time, there can be real data events
            // after the empty ones, reset the counter
            lastEmptyCnt = 0
            emptyCnt = 0
          }

        } else {
          if (userRecord.data.length > 0) {
            lastEmptyCnt = 0
            emptyCnt = 0

            fetchedRecord = Some(userRecord)
            lastReadTimeMs = System.currentTimeMillis()
            logDebug(s"Milli secs behind is ${userRecord.millisBehindLatest}")

            if (
            // this check assumes the records in dataQueue is in order
              reachStopShardInfo(userRecord, sourcePartition.stopShardInfo)
            ) {
              logInfo(s"stopShardInfo reached for shard ${kinesisShardId}" +
                s" with userRecord ${userRecord.sequenceNumber}," +
                s" inputPartition stopShardInfo ${sourcePartition.stopShardInfo}")
              fetchNext = false
            }
            else if (userRecord.millisBehindLatest.longValue() == 0
              && userRecord.isLastSubSequence
              && dataQueue.size() == 0
            ) {
              // wait for one more loop before stop fetching
              lastEmptyCnt = Math.max(maxDataQueueEmptyCount - 1, 0)
            }
          }
          else {
            logError(s"Got userRecord with zero data length ${userRecord}. Not supposed to reach here.")
            fetchNext = false
          }
        }
      }

      while (fetchedRecord.isEmpty && fetchNext)  {
        val currentTimestamp: Long = System.currentTimeMillis
        if(hasTimeForMoreRecords(currentTimestamp)) {
          pollNextUserRecord()
        } else {
          logInfo(s"Max fetch time reached at shard ${kinesisShardId}: current ${currentTimestamp}, start ${startTimestamp}")
          fetchNext = false
        }
      }

      val throwable = errorRef.get()
      if (throwable != null) {
        throw new RuntimeException("stopWithError rethrow in getNext", throwable)
      }
      
      if (fetchedRecord.isEmpty) {
        logInfo(s"Fetch completed for batch ${batchId}/shard ${kinesisShardId}, ${lastReadSequenceNumber}. Number of records read: ${numRecordRead}.")
        finished = true
        null
      } else {
        val record = fetchedRecord.get
        numRecordRead +=1
        if (numRecordRead >= kinesisOptions.maxFetchRecordsPerShard) {
          logInfo(s"Number of records read:${numRecordRead} reached maxFetchRecordsPerShard:${kinesisOptions.maxFetchRecordsPerShard}" +
            s" at shard ${kinesisShardId}, ${record.sequenceNumber}.")
          fetchNext = false
        } else if ((numRecordRead % 1000) == 0) {
          logInfo(s"Number of records read:${numRecordRead} at shard ${kinesisShardId}, ${record.sequenceNumber}.")
        }
        
        lastReadSequenceNumber = record.sequenceNumber

        InternalRow.fromSeq(schema.fieldNames.map {
          case "streamName" => UTF8String.fromString(streamName)
          case "partitionKey" => UTF8String.fromString(record.partitionKey)
          case "sequenceNumber" => UTF8String.fromString(record.sequenceNumber.sequenceNumber)
          case "subSequenceNumber" => UTF8String.fromString(record.sequenceNumber.subSequenceNumber.toString)
          case "approximateArrivalTimestamp" => ChronoUnit.MICROS.between(Instant.EPOCH, record.approximateArrivalTimestamp)
          case "data" => record.data
          case name =>
            throw new UnsupportedOperationException("Unsupported field name in schema " + name)
        }.toSeq)
      }
    }

    override protected def close(): Unit = synchronized {
      logInfo(s"Close ${sourcePartition.startShardInfo} underlying iterator  ")
    }
  }

  override def next(): Boolean = {
    logDebug("KinesisV2PartitionReader.next")
    underlying.hasNext
  }

  override def get(): InternalRow = {
    logDebug("KinesisV2PartitionReader.get")
    underlying.next()
  }

  override def close(): Unit = {
    logInfo(s"Start to close ${sourcePartition.startShardInfo}  current value of closed=${closed}")
    if(closed.compareAndSet(false, true)) {
      // clear the queue to unblock enqueue operations
      dataQueue.clear()

      shutdownAndAwaitTermination(shardConsumersExecutor)

      tryAndIgnoreError("close kinesis reader")(kinesisReader.close())

      logInfo(s"[${Thread.currentThread().getName}] KinesisV2PartitionReader ${sourcePartition.startShardInfo} close done")
    }
  }

  def updateMetadata(taskContext: TaskContext): Unit = {

    // if lastReadSequenceNumber exists, use AfterSequenceNumber for next Iterator
    // else use the same iterator information which was given to the RDD

    val shardInfo: ShardInfo =
      if (hasShardClosed.get) {
        new ShardInfo(sourcePartition.startShardInfo.shardId,
          new ShardEnd())
      }
      else if (lastReadSequenceNumber != null) {
          new ShardInfo(
            sourcePartition.startShardInfo.shardId,
            new AfterSequenceNumber(lastReadSequenceNumber.sequenceNumber,
              lastReadSequenceNumber.subSequenceNumber,
              lastReadSequenceNumber.isLast))
      }
      else {
        logInfo(s"No Records were processed in this batch for ${sourcePartition.startShardInfo}")
        sourcePartition.startShardInfo
      }

    logInfo(s"Batch $batchId : metadataCommitter adding shard position for ${kinesisShardId}, shardInfo ${shardInfo}")

    metadataCommitter.add(batchId, kinesisShardId, shardInfo)
  }
  
  TaskContext.get().addTaskCompletionListener [Unit]{ taskContext: TaskContext =>
    logInfo(s"Complete Task for taskAttemptId ${taskContext.taskAttemptId()}, partitionId ${taskContext.partitionId()}")
    val throwable = errorRef.get()
    if (throwable == null) {
      updateMetadata(taskContext)
    } else {
      logWarning("skip updateMetadata as stopped with error")
    }
  }
}
