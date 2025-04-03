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

import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.client.KinesisClientFactory
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitter
import org.apache.spark.sql.connector.kinesis.metadata.MetadataCommitterFactory
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.ThreadUtils.newDaemonSingleThreadScheduledExecutor

class KinesisV2MicrobatchStream (
        schema: StructType,
        options: KinesisOptions,
        checkpointLocation: String)
  extends SupportsAdmissionControl with Logging with MicroBatchStream {

  val failOnDataLoss: Boolean = options.failOnDataLoss

  private val isEfoConsumer: Boolean = {
    options.consumerType == EFO_CONSUMER_TYPE
  }

  lazy val initialPosition: InitialKinesisPosition = InitialKinesisPosition.getKinesisPosition(options)

  private var maintenanceTask: Option[MaintenanceTask] = None

  private val kinesisClient: KinesisClientConsumer = {
    KinesisClientFactory.createConsumer(options, options.streamName, options.endpointUrl)
  }

  private val consumerArn = if (isEfoConsumer) {
    KinesisUtils.registerStreamConsumer(
      kinesisClient,
      options.consumerName.get
    )
  } else ""

  private var currentShardOffsets: Option[ShardOffsets] = None

  // default is using the same value as SQLConf
  private val minBatchesToRetain = {
    val sqlConf = SQLConf.get
    options.minBatchesToRetain.getOrElse(sqlConf.minBatchesToRetain)
  }

  private val maintenanceIntervalMs = options.maintenanceTaskIntervalSec * 1000
  private val describeShardInterval: Long = options.describeShardIntervalMs

  private var latestDescribeShardTimestamp: Long = -1L

  private val hadoopConf = new SerializableConfiguration(
    SparkSession.getActiveSession.map { s =>
      logInfo(s"KinesisV2MicrobatchStream use sparkContext.hadoopConfiguration")
      s.sparkContext.hadoopConfiguration
    }.getOrElse {
      logInfo(s"KinesisV2MicrobatchStream create default Configuration")
      new Configuration()
    })

  private val metadataCommitter: MetadataCommitter[ShardInfo] = {

    MetadataCommitterFactory.createMetadataCommitter(
      options, checkpointLocation, Some(hadoopConf)
    )
  }

  private val maxParallelThreads = options.checkNewRecordThreads

  /** Makes an API call to get one record for a shard. Return true if the call is successful  */
  private def hasNewData(shardInfo: ShardInfo): Boolean = {
    logDebug(s"hasNewData shardInfo ${shardInfo}")
    val shardIterator = kinesisClient.getShardIterator(
      shardInfo.shardId,
      shardInfo.iteratorType,
      shardInfo.iteratorPosition)

    def getRecordsResponseInfo(shardIterator: String): (Int, Long, String) = {
      val records = kinesisClient.getKinesisRecords(shardIterator, 1)
      val recordsSize = records.records.size()
      val millisBehindLatest = records.millisBehindLatest.longValue()
      logDebug(s"hasNewData recordsSize ${recordsSize}, millisBehindLatest ${millisBehindLatest}")
      (recordsSize, millisBehindLatest, records.nextShardIterator)
    }
    var (recordsSize, millisBehindLatest, nextShardIterator) = getRecordsResponseInfo(shardIterator)
    while (recordsSize == 0 && millisBehindLatest > 0 && nextShardIterator != null) {
      logDebug(s"hasNewData using iterator ${nextShardIterator} to catch up")
      val (recordsSizeCatchUp, millisBehindLatestCatchUp, nextShardIteratorCatchUp) =
        getRecordsResponseInfo(nextShardIterator)
      recordsSize = recordsSizeCatchUp
      millisBehindLatest = millisBehindLatestCatchUp
      nextShardIterator = nextShardIteratorCatchUp
    }
    // Return true if we can get back a record. Or if we have not reached the end of the stream
    val result = (recordsSize > 0 || millisBehindLatest > 0)
    logInfo(s"hasNewData result ${result} for shardId ${shardInfo.shardId}")
    result
  }

  private def hasUnfinishedAggregateRecord(shardsInfo: Array[ShardInfo]): Boolean = {
    val result = shardsInfo.exists(!_.isLast)
    logInfo(s"hasUnfinishedAggregateRecord ${result}")
    result
  }

  private def canCreateNewBatch(shardsInfo: Array[ShardInfo]): Boolean = {

    val evalPool = getFixedUninterruptibleThreadPoolFromCache("canCreateNewBatch", maxParallelThreads)
    val hasRecords = new AtomicBoolean(false)


    val futures = new ArrayBuffer[Future[Boolean]](maxParallelThreads)

    breakable {
      (0 to (shardsInfo.length / maxParallelThreads)).foreach { loop =>
        (0 until maxParallelThreads).foreach { i =>
          val shardIndex = loop * maxParallelThreads + i

          if (shardIndex < shardsInfo.length) {
            futures += (evalPool.submit(new Callable[Boolean]() {
              override def call: Boolean = {
                val shard = shardsInfo(shardIndex)
                try {
                  if (!hasRecords.get() && hasNewData(shard)) {
                    hasRecords.set(true)
                  }
                } catch {
                  case NonFatal(e) =>
                    logWarning(s"Error in check new data for ${shard}", e) // Ignore errors
                }

                hasRecords.get()

              }
            }))
          }
        }

        futures.foreach { f =>
           tryAndIgnoreError("canCreateNewBatch future get") { f.get }
        }

        // if one shard has new records, no need to check others
        if (hasRecords.get()) {
          break
        } else {
          futures.clear()
        }
      }
    }

    logInfo(s"Can create new batch = ${hasRecords.get()}")
    hasRecords.get()
  }

  private def hasShardEndAsOffset(shardInfo: Seq[ShardInfo]): Boolean = {
    val result = shardInfo.exists {
      s: (ShardInfo) => (s.iteratorType == ShardEnd.iteratorType)
    }
    logInfo(s"hasShardEndAsOffset ${result}")
    result
  }

  private def getBatchShardsInfo(batchId: Long): Map[String, ShardInfo] = {
    if (batchId < 0) {
      logInfo(s"This is the first batch. Returning Empty sequence")
      Map.empty
    } else {
      if (metadataCommitter.exists(batchId)) {
        logInfo(s"getBatchShardsInfo for batchId $batchId")
        val shardsInfo = metadataCommitter.get(batchId)
        logDebug(s"Shard Info is ${shardsInfo}")
        shardsInfo.map(s => s.shardId -> s).toMap
      }
      else {
        logInfo(s"getBatchShardsInfo for new batchId $batchId")
        Map.empty
      }

    }
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def reportLatestOffset(): Offset = {
    logDebug(s"reportLatestOffset is $currentShardOffsets")

    currentShardOffsets match {
      case None => KinesisV2SourceOffset(new ShardOffsets(-1L, options.streamName))
      case Some(cso) => KinesisV2SourceOffset(cso)
    }
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = synchronized {
    logDebug(s"get latestOffset with start Offset ${start}")
    val prevBatchId = start.asInstanceOf[KinesisV2SourceOffset].shardsToOffsets.batchId
    logInfo(s"get latestOffset prevBatchId ${prevBatchId}")
    val prevShardsInfo = getBatchShardsInfo(prevBatchId)
    logDebug(s"get latestOffset prevShardsInfo ${prevShardsInfo}")
    if (prevBatchId >= 0 && prevShardsInfo.isEmpty) {
      throw new IllegalStateException(s"Unable to fetch " +
        s"committed metadata from previous batch id ${prevBatchId}. Some data may have been missed")
    }

    // need to merge start and prevShardsInfo to ensure no shard missing when metadataLog has only partial data
    val mergedPrevShardsInfo = start.asInstanceOf[KinesisV2SourceOffset]
      .shardsToOffsets
      .shardInfoMap.values.map { s =>
        getShardInfoByShardId(prevShardsInfo, s.shardId).getOrElse(s)
       }.toSeq
    logDebug(s"mergedPrevShardsInfo ${mergedPrevShardsInfo}")

    val latestShardInfo: Array[ShardInfo] = {
      if (prevBatchId < 0
        || latestDescribeShardTimestamp == -1
        || ((latestDescribeShardTimestamp + describeShardInterval) < System.currentTimeMillis())) {
        val latestShards = kinesisClient.getShards
        latestDescribeShardTimestamp = System.currentTimeMillis()
        ShardSyncer.getLatestShardInfo(latestShards, mergedPrevShardsInfo,
          initialPosition, failOnDataLoss)
      } else {
        mergedPrevShardsInfo
      }
    }.toArray

    if (!options.avoidEmptyBatches
      || prevBatchId < 0 // new job started without previous checkpoint
      || currentShardOffsets.isEmpty // job started resuming from previous checkpoint
      || hasUnfinishedAggregateRecord(latestShardInfo)
      || hasShardEndAsOffset(latestShardInfo)
      || ShardSyncer.hasNewShards(mergedPrevShardsInfo, latestShardInfo)
      || ShardSyncer.hasDeletedShards(mergedPrevShardsInfo, latestShardInfo)
      || canCreateNewBatch(latestShardInfo)

    ) {
      currentShardOffsets = Some(
        new ShardOffsets(prevBatchId + 1, options.streamName,
                latestShardInfo.filter(_.iteratorType != ShardEnd.iteratorType)
      ))
    } else {
      logInfo(s"Offsets are unchanged since ${KinesisOptions.AVOID_EMPTY_BATCHES} is enabled")
    }


    logInfo(s"currentShardOffsets is ${currentShardOffsets}")
    currentShardOffsets match {
      case None => KinesisV2SourceOffset(new ShardOffsets(-1L, options.streamName))
      case Some(cso) => KinesisV2SourceOffset(cso)
    }
  }

  def getShardInfoByShardId(shardsInfo: Map[String, ShardInfo], shardId: String): Option[ShardInfo] = {
    logDebug(s"getShardInfoByShardId, shardsInfo ${shardsInfo}, shardId ${shardId}")
    val findResult = shardsInfo.get(shardId)
    logDebug(s"getShardInfoByShardId findResult ${findResult} for shardId ${shardId}")
    findResult
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {

    // There are 2 scenarios
    // 1. metadataLog(end.batchId) doesn't exist: normal next batch, use end to create InputPartitions.
    // 2. metadataLog(end.batchId) exist (include both partial and full exists): rerun uncommitted batch
    // For scenario 2, use the shardIds from metadataLog to create InputPartitions. so the restored batch will only process the shards in metadataLog
    // For both scenarios, start's shard info is not really used except the assert
    logInfo(s"planInputPartitions is start $start, end $end")
    val currBatchShardOffset = KinesisV2SourceOffset.getShardOffsets(end)
    val currBatchId = currBatchShardOffset.batchId
    val prevBatchId: Long = if (start != null) {
      KinesisV2SourceOffset.getShardOffsets(start).batchId
    } else {
      -1.toLong
    }
    logInfo(s"prevBatchId $prevBatchId, currBatchId $currBatchId")
    assert(prevBatchId <= currBatchId)

    val currBatchShardsInfo = getBatchShardsInfo(currBatchId)

    val partitions = currBatchShardOffset.shardInfoMap.values.toSeq.filter {
      s: (ShardInfo) =>
        // filter out those shardInfos for which ShardIterator is shard_end
        if (s.iteratorType == ShardEnd.iteratorType) false
        else if (currBatchShardsInfo.isEmpty) true
        else {
          val result = getShardInfoByShardId(currBatchShardsInfo, s.shardId)
          result.exists(s => s.iteratorType == AtSequenceNumber.iteratorType
            || s.iteratorType == AfterSequenceNumber.iteratorType
            || s.iteratorType == ShardEnd.iteratorType
          )
        }
      }
      .sortBy(_.shardId)
      .map( shardInfoOffset => KinesisV2InputPartition(
        schema,
        currBatchId,
        options.streamName,
        shardInfoOffset,
        getShardInfoByShardId(currBatchShardsInfo, shardInfoOffset.shardId)
      ))
    logInfo(s"planInputPartitions produces ${partitions.length} shard(s).")

    // currentShardOffsets is empty when restore from uncommitted
    if (currentShardOffsets.isEmpty) {
      currentShardOffsets = Some(currBatchShardOffset)
    }

    partitions.toArray
  }
  
  override def createReaderFactory(): PartitionReaderFactory = new KinesisV2PartitionReaderFactory(schema,
    options.streamName,
    options.endpointUrl,
    consumerArn,
    checkpointLocation,
    currentShardOffsets.getOrElse(new ShardOffsets(-1L, options.streamName)).batchId,
    options,
    hadoopConf
  )

  override def initialOffset(): Offset = {
    logInfo(s"initialOffset for ${options.streamName}")
    KinesisV2SourceOffset(new ShardOffsets(-1L, options.streamName))
  }


  override def deserializeOffset(json: String): Offset = {
    KinesisV2SourceOffset(json)
  }

  override def commit(end: Offset): Unit = {
    logDebug(s"Committing end Offset ${end}")

    if(maintenanceTask.isEmpty || !maintenanceTask.get.isRunning) {
      maintenanceTask = Some(new MaintenanceTask(
        maintenanceIntervalMs,
        task = {
          doMaintenance()
        },
        onError = {
          logInfo("Metadata maintenance task got error")
        }
      ))
    }
  }

  private def doMaintenance(): Unit = {
    logInfo("Metadata maintenance task started")

    reportTimeTaken("doMaintenance") {
      try {
        metadataCommitter.purge(minBatchesToRetain)
      } catch {
        case NonFatal(e) =>
            logWarning("Got error while do maintenance. Ignore it.", e)
      }

    }
  }
  override def stop(): Unit = {
    logInfo(s"Stopping KinesisV2MicrobatchStream. deregisterStreamConsumer ${options.streamName} - ${consumerArn}")

    if (isEfoConsumer)  {
      tryAndIgnoreError("deregisterStreamConsumer") {
        KinesisUtils.deregisterStreamConsumer(kinesisClient, options.consumerName.get)
      }
    }

    tryAndIgnoreError("maintenanceTask stop") {
      maintenanceTask.foreach(_.stop())
    }

    tryAndIgnoreError("kinesisClient close") {
      kinesisClient.close()
    }
  }

  private class MaintenanceTask(periodMs: Long, task: => Unit, onError: => Unit) {
    logInfo(s"create MaintenanceTask with periodMs ${periodMs}")

    private val executor =
      newDaemonSingleThreadScheduledExecutor("KinesisV2MicrobatchStream-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task
        } catch {
          case NonFatal(e) =>
            logWarning("Error running maintenance thread", e)
            onError
        }
      }
    }

    private val future: ScheduledFuture[_] = executor.scheduleAtFixedRate(
      runnable, periodMs, periodMs, TimeUnit.MILLISECONDS)

    def stop(): Unit = {
      future.cancel(false)
      executor.shutdown()
    }

    def isRunning: Boolean = !future.isDone
  }

}
