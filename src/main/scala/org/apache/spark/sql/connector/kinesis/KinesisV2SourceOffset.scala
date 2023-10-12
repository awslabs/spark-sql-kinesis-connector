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
import scala.util.control.NonFatal

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.streaming.SerializedOffset

case class KinesisV2SourceOffset(shardsToOffsets: ShardOffsets) extends Offset {
  override def json: String = {
    val metadata = mutable.HashMap[String, String](
      "batchId" -> shardsToOffsets.batchId.toString,
      "streamName" -> shardsToOffsets.streamName)
    val result = mutable.HashMap[String, mutable.HashMap[String, String]]("metadata" -> metadata)

    val shardInfos = shardsToOffsets.shardInfoMap.keySet.toSeq.sorted  // sort for more determinism

    shardInfos.foreach {
      shardId: String =>
        val shardInfo: ShardInfo = shardsToOffsets.shardInfoMap(shardId)
        val part = result.getOrElse(shardInfo.shardId, new mutable.HashMap[String, String])
        part += KinesisPosition.ITERATOR_TYPE_FIELD -> shardInfo.iteratorType
        part += KinesisPosition.ITERATOR_POSITION_FILED -> shardInfo.iteratorPosition
        part += KinesisPosition.SUB_SEQUENCE_NUMBER_FILED -> shardInfo.subSequenceNumber.toString
        part += KinesisPosition.IS_LAST_FILED -> shardInfo.isLast.toString
        result += shardId -> part
    }
    Serialization.write(result)(KinesisV2SourceOffset.format)
  }
}

object KinesisV2SourceOffset {
  implicit val format = Serialization.formats(NoTypeHints)

  def getShardOffsets(offset: Offset): ShardOffsets = {
    offset match {
      case kso: KinesisV2SourceOffset => kso.shardsToOffsets
      case so: SerializedOffset => KinesisV2SourceOffset(so).shardsToOffsets
      case _ => throw
        new IllegalArgumentException(s"Invalid conversion " +
          s"from offset of ${offset.getClass} to KinesisV2SourceOffset")
    }
  }

  /*
   * Returns [[KinesisV2SourceOffset]] from a JSON [[SerializedOffset]]
   */
  def apply(so: SerializedOffset): KinesisV2SourceOffset = {
    apply(so.json)
  }

  /*
   * Returns [[KinesisV2SourceOffset]] from a JSON
   */
  def apply(json: String): KinesisV2SourceOffset = {
    try {
      val readObj = Serialization.read[ Map[ String, Map[ String, String ] ] ](json)
      val metadata = readObj.get("metadata")
      val shardInfoMap: Map[String, ShardInfo ] = readObj.filter(_._1 != "metadata").map {
        case (shardId, value) => shardId -> ShardInfo(shardId,
          value(KinesisPosition.ITERATOR_TYPE_FIELD),
          value(KinesisPosition.ITERATOR_POSITION_FILED),
          value(KinesisPosition.SUB_SEQUENCE_NUMBER_FILED).toLong,
          value(KinesisPosition.IS_LAST_FILED).toBoolean,
        )
      }
      KinesisV2SourceOffset(
        ShardOffsets(
          metadata.get("batchId").toLong,
          metadata.get("streamName"),
          shardInfoMap))
    } catch {
      case NonFatal(x) => throw new IllegalArgumentException(x)
    }
  }

  def getMap(shardInfos: Array[ShardInfo]): Map[String, ShardInfo] = {
    shardInfos.map {
      s: ShardInfo => (s.shardId -> s)
    }.toMap
  }

}


