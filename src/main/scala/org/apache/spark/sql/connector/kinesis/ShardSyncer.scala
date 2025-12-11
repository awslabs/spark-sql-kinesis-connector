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

import scala.collection.mutable

import software.amazon.awssdk.services.kinesis.model.Shard

import org.apache.spark.internal.Logging

/*
 * Helper class to sync batch with shards of the Kinesis stream.
 * It will create new activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It works in similar way as
 * com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer in KCL
 */

object ShardSyncer extends Logging {

  private def getShardIdToChildShardsMap(latestShards: Seq[Shard]):
  mutable.Map[String, List[String ]] = {
    val shardIdToChildShardsMap = mutable.Map.empty[String, List[String]]

    val shardIdToShardMap =
      latestShards.map {
        s => (s.shardId -> s)
      }.toMap

    for ((shardId, shard) <- shardIdToShardMap) {
      val parentShardId: String = shard.parentShardId
      if ( parentShardId != null && shardIdToShardMap.contains(parentShardId) ) {
        shardIdToChildShardsMap += (
          parentShardId ->
            (shardId :: shardIdToChildShardsMap.getOrElse(parentShardId, Nil))
          )
      }

      val adjacentParentShardId: String = shard.adjacentParentShardId
      if ( adjacentParentShardId != null && shardIdToShardMap.contains(adjacentParentShardId) ) {
        shardIdToChildShardsMap += (
          adjacentParentShardId ->
            (shardId :: shardIdToChildShardsMap.getOrElse(adjacentParentShardId, Nil))
          )
      }
    }
    // Assert that Parent Shards are closed
    shardIdToChildShardsMap.keySet.foreach {
      parentShardId =>
        shardIdToShardMap.get(parentShardId) match {
          case None =>
            throw new IllegalStateException(s"ShardId $parentShardId is not closed. " +
              s"This can happen due to a race condition between listShards and a" +
              s" reshard operation")
          case Some(parentShard: Shard) =>
            if (parentShard.sequenceNumberRange().endingSequenceNumber == null) {
              throw new IllegalStateException(s"ShardId $parentShardId is not closed. " +
                s"This can happen due to a race condition between listShards and a " +
                s"reshard operation")
            }
        }
    }
    shardIdToChildShardsMap
  }

  def AddShardInfoForAncestors(
                       shardId: String,
                       latestShards: Seq[Shard],
                       initialPosition: InitialKinesisPosition,
                       prevShardsList: mutable.Set[ String ],
                       newShardsInfoMap: mutable.HashMap[String, ShardInfo],
                       memoizationContext: mutable.Map[String, Boolean]): Unit = {

    val shardIdToShardMap =
      latestShards.map {
        s => (s.shardId -> s)
      }.toMap

    if (!memoizationContext.contains(shardId) &&
      shardId != null && shardIdToShardMap.contains(shardId) ) {
      if (prevShardsList.contains(shardId) ) {
        // we already have processed this shard in previous batch and added its ancestors
        memoizationContext.put(shardId, true)
        return
      }
      val shard = shardIdToShardMap(shardId)
      // get parent of shards if exist
      val parentShardIds: mutable.HashSet[String] = getParentShardIds(shard, latestShards)
      for (parentShardId <- parentShardIds) {
        // Add ShardInfo of Parent's ancestors.
        AddShardInfoForAncestors( parentShardId,
          latestShards, initialPosition, prevShardsList,
          newShardsInfoMap, memoizationContext)
      }
      // create shardInfo for its parent shards (if they don't exist)
      for (parentShardId <- parentShardIds) {
        if (!prevShardsList.contains(parentShardId) ) {
          logDebug("Need to create a shardInfo for shardId " + parentShardId)
          if (!newShardsInfoMap.contains(parentShardId)) {
            newShardsInfoMap.put(parentShardId,
              new ShardInfo(parentShardId, initialPosition.shardPosition(parentShardId)))
          }
        }
      }
      memoizationContext.put(shardId, true)
    }
  }

  def getParentShardIds(
                                          shard: Shard,
                                          shards: Seq[Shard]): mutable.HashSet[String] = {
    val parentShardIds = new mutable.HashSet[ String ]
    val parentShardId = shard.parentShardId
    val shardIdToShardMap =
      shards.map {
        s => (s.shardId -> s)
      }.toMap

    if ((parentShardId != null) && shardIdToShardMap.contains(parentShardId)) {
      parentShardIds.add(parentShardId)
    }
    val adjacentParentShardId = shard.adjacentParentShardId
    if ( (adjacentParentShardId != null) && shardIdToShardMap.contains(adjacentParentShardId)) {
      parentShardIds.add(adjacentParentShardId)
    }
    
    parentShardIds
  }

  /*
   *  Takes a Shard as input param.
   *  It determines if the given shard is open.
   */
  def isOpen(shard: Shard): Boolean = return shard.sequenceNumberRange.endingSequenceNumber == null

  def hasNewShards(prevShardsInfo: Seq[ShardInfo],
                   latestShardsInfo: Seq[ShardInfo]): Boolean = {

    val prevShardIds = prevShardsInfo.map(_.shardId)
    val result = latestShardsInfo.foldLeft(false) {
      (hasNewShard, shardInfo) =>
        if (!hasNewShard) {
          !prevShardIds.contains(shardInfo.shardId)
        } else {
          hasNewShard
        }
    }
    logInfo(s"hasNewShards: ${result}")
    result
  }

  def hasDeletedShards(prevShardsInfo: Seq[ShardInfo],
                       latestShardsInfo: Seq[ShardInfo]): Boolean = {
    val latestShardIds = latestShardsInfo.map(_.shardId)
    val result = prevShardsInfo.foldLeft(false) {
      (hasDeletedShard, shardInfo) =>
        if (!hasDeletedShard) {
          !latestShardIds.contains(shardInfo.shardId)
        } else {
          hasDeletedShard
        }
    }
    logInfo(s"hasDeletedShards: ${result}")
    result
  }

  def getLatestShardInfo(
                          latestShards: Seq[Shard],
                          prevShardsInfo: Seq[ShardInfo],
                          initialPosition: InitialKinesisPosition,
                          failOnDataLoss: Boolean = false): Seq[ShardInfo] = {

    logDebug(s"getLatestShardInfo initialPosition ${initialPosition}")

    if (latestShards.isEmpty) {
      return prevShardsInfo
    }
    
    val prevShardsList = new mutable.HashSet[String]
    val latestShardsList = new mutable.HashSet[String]
    prevShardsInfo.foreach {
      s: ShardInfo => prevShardsList.add(s.shardId)
    }
    latestShards.foreach {
      s: Shard => latestShardsList.add(s.shardId)
    }
    // check for deleted shards
    val deletedShardsList = prevShardsList.diff(latestShardsList)
    val newShardsInfoMap = new mutable.HashMap[String, ShardInfo]
    val memoizationContext = new mutable.HashMap[ String, Boolean]

    // check for deleted Shards and update newShardInfo if failOnDataLoss is false
    if (deletedShardsList.nonEmpty) {
      if (failOnDataLoss) {
        throw new IllegalStateException(
          s"""
             | Some data may have been lost because ${deletedShardsList.toString()}
             | are not available in Kinesis any more. The shard has been deleted before
             | we have processed all records in it. If you do not want your streaming query
             | to fail on such cases, set the source option "failOnDataLoss" to "false"
           """.stripMargin
        )
      } else {
        logWarning(
          s"""
             | Some data may have been lost because $deletedShardsList are not available in Kinesis
             | any more. The shard has been deleted before we have processed all records in it.
             | If you want your streaming query to fail on such cases, set the source option
             | "failOnDataLoss" to "true"
           """.stripMargin
        )
      }
    }

    // filter the deleted shards
    var filteredPrevShardsInfo = prevShardsInfo.filter {
      s: ShardInfo => !deletedShardsList.contains(s.shardId)
    }.toBuffer

    latestShards.foreach {
      shard: Shard => {
        updateShard(
          shard, prevShardsList, latestShards, initialPosition, newShardsInfoMap, memoizationContext, filteredPrevShardsInfo)
      }
    }

    logDebug(s"getLatestShardInfo filteredPrevShardsInfo ${filteredPrevShardsInfo}")
    logDebug(s"getLatestShardInfo newShardsInfoMap ${newShardsInfoMap}")
    filteredPrevShardsInfo.toSeq ++ newShardsInfoMap.values.toSeq
  }

  private def updateShard(
      shard: Shard,
      prevShardsList: mutable.HashSet[String],
      latestShards: Seq[Shard],
      initialPosition: InitialKinesisPosition,
      newShardsInfoMap: mutable.HashMap[String, ShardInfo],
      memoizationContext: mutable.Map[String, Boolean],
      filteredPrevShardsInfo: mutable.Buffer[ShardInfo]): Unit = {

    val shardId = shard.shardId()

    if (isOpen(shard)) {
      if (prevShardsList.contains(shardId)) {
        logDebug("Info for shardId " + shardId + " already exists")
      } else {
        AddShardInfoForAncestors(shardId,
          latestShards, initialPosition, prevShardsList, newShardsInfoMap, memoizationContext)
        newShardsInfoMap.put(shardId,
          new ShardInfo(shardId, initialPosition.shardPosition(shardId)))
      }
    } else {
      if (prevShardsList.contains(shardId)) {
        val updatedShardInfo = new ShardInfo(shardId, new ShardEnd())

        val index = filteredPrevShardsInfo.indexWhere(_.shardId == shardId)
        if (index >= 0) {
          filteredPrevShardsInfo.update(index, updatedShardInfo)
        }
      } else {
        AddShardInfoForAncestors(shardId,
          latestShards, initialPosition, prevShardsList, newShardsInfoMap, memoizationContext)
        newShardsInfoMap.put(shardId, new ShardInfo(shardId, new ShardEnd()))
      }
    }
  }

}
