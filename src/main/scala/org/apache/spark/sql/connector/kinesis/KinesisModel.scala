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

import java.time.DateTimeException
import java.time.format.DateTimeParseException
import java.util.Locale

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.KinesisPosition.NO_SUB_SEQUENCE_NUMBER
import org.apache.spark.sql.connector.kinesis.retrieval.SequenceNumber


sealed trait KinesisPosition extends Serializable {
  def iteratorType: String
  def iteratorPosition: String
  def subSequenceNumber: Long
  def isLast: Boolean

  override def toString: String = s"KinesisPosition($iteratorType, $iteratorPosition, $subSequenceNumber, $isLast)"
}

case class TrimHorizon() extends KinesisPosition {
  override val iteratorType: String = TrimHorizon.iteratorType
  override val iteratorPosition: String = ""
  override val subSequenceNumber: Long = NO_SUB_SEQUENCE_NUMBER
  override val isLast: Boolean = true
}

object TrimHorizon {
  val iteratorType: String = ShardIteratorType.TRIM_HORIZON.toString
}

case class Latest() extends KinesisPosition {
  override val iteratorType: String = Latest.iteratorType
  override val iteratorPosition: String = ""
  override val subSequenceNumber: Long = NO_SUB_SEQUENCE_NUMBER
  override val isLast: Boolean = true
}

object Latest {
  val iteratorType: String = ShardIteratorType.LATEST.toString
}

case class AtTimeStamp(
 timestamp: Long
) extends KinesisPosition {
  override val iteratorType: String = AtTimeStamp.iteratorType
  override val iteratorPosition : String = timestamp.toString
  override val subSequenceNumber: Long = NO_SUB_SEQUENCE_NUMBER
  override val isLast: Boolean = true

}

object AtTimeStamp {
  val iteratorType: String = ShardIteratorType.AT_TIMESTAMP.toString
}


case class AfterSequenceNumber(
  seqNumber: String,
  subSeqNumber: Long,
  last: Boolean
) extends KinesisPosition {
  override val iteratorType: String = AfterSequenceNumber.iteratorType
  override val iteratorPosition : String = seqNumber
  override val subSequenceNumber: Long = subSeqNumber
  override val isLast: Boolean = last
}

object AfterSequenceNumber {
  val iteratorType: String = ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString
}

case class AtSequenceNumber(
  seqNumber: String,
  subSeqNumber: Long,
  last: Boolean
) extends KinesisPosition {
  override val iteratorType: String = AtSequenceNumber.iteratorType
  override val iteratorPosition : String = seqNumber
  override val subSequenceNumber: Long = subSeqNumber
  override val isLast: Boolean = last
}

object AtSequenceNumber {
  val iteratorType: String = ShardIteratorType.AT_SEQUENCE_NUMBER.toString
}

case class ShardEnd() extends KinesisPosition {
  override val iteratorType: String = ShardEnd.iteratorType
  override val iteratorPosition: String = ""
  override val subSequenceNumber: Long = NO_SUB_SEQUENCE_NUMBER
  override val isLast: Boolean = true
}

object ShardEnd {
  val iteratorType = "SHARD_END"
}

object KinesisPosition {
  val ITERATOR_TYPE_FIELD = "iteratorType"
  val ITERATOR_POSITION_FILED = "iteratorPosition"
  val SUB_SEQUENCE_NUMBER_FILED = "subSequenceNumber"
  val IS_LAST_FILED = "isLast"

  val NO_SUB_SEQUENCE_NUMBER: Long = -1
  def make(iteratorType: String,
           iteratorPosition: String,
           subSequenceNumber: Long,
           isLast: Boolean
          ): KinesisPosition = iteratorType match {
    case iterType if TrimHorizon.iteratorType.equalsIgnoreCase(iterType) => new TrimHorizon()
    case iterType if Latest.iteratorType.equalsIgnoreCase(iterType) => new Latest()
    case iterType if AtTimeStamp.iteratorType.equalsIgnoreCase(iterType) => new AtTimeStamp(iteratorPosition.toLong)
    case iterType if AtSequenceNumber.iteratorType.equalsIgnoreCase(iterType) =>
      new AtSequenceNumber(iteratorPosition, subSequenceNumber, isLast)
    case iterType if AfterSequenceNumber.iteratorType.equalsIgnoreCase(iterType) =>
      new AfterSequenceNumber(iteratorPosition, subSequenceNumber, isLast)
    case iterType if ShardEnd.iteratorType.equalsIgnoreCase(iterType) => new ShardEnd()
  }


}

/**
 * Specifies initial position in Kenesis to start read from on the application startup.
 * @param shardPositions map of shardId->KinesisPosition
 * @param defaultPosition position that is used for shard that is requested but not present in map
 */
class InitialKinesisPosition(shardPositions: Map[String, KinesisPosition],
                                              defaultPosition: KinesisPosition)
  extends Serializable {

  def shardPosition(shardId: String): KinesisPosition =
    shardPositions.getOrElse(shardId, defaultPosition)

  override def toString: String = s"InitialKinesisPosition($shardPositions)::defaultPosition($defaultPosition) "
}

object InitialKinesisPosition extends Logging {
  implicit val format = Serialization.formats(NoTypeHints)

  def fromPredefPosition(pos: KinesisPosition): InitialKinesisPosition =
    new InitialKinesisPosition(Map(), pos)

  /**
   * Parses json representation on Kinesis position.
   * It is useful if Kinesis position is persisted explicitly (e.g. at the end of the batch)
   * and used to continue reading records from the same position on Spark application redeploy.
   * Kinesis position JSON representation example:
   * {{{
   * {
   *   "shardId-000000000001":{
   *     "iteratorType":"AFTER_SEQUENCE_NUMBER",
   *     "iteratorPosition":"49605240428222307037115827613554798409561082419642105874",
   *     "subSequenceNumber": "-1"
   *   },
   *   "metadata":{
   *     "streamName":"my.cool.stream2",
   *     "batchId":"7"
   *   },
   *   "shardId-000000000000":{
   *     "iteratorType":"AFTER_SEQUENCE_NUMBER",
   *     "iteratorPosition":"49605240428200006291917297020490128157480794051565322242",
   *     "subSequenceNumber": "1"
   *   }
   * }
   * }}}
   * @param text JSON representation of Kinesis position.
   * @return
   */
  def fromCheckpointJson(text: String, defaultPosition: KinesisPosition): InitialKinesisPosition = {
    val kso = KinesisV2SourceOffset(text)
    val shardOffsets = kso.shardsToOffsets

    new InitialKinesisPosition(
      shardOffsets.shardInfoMap
        .map(si => si._1 -> KinesisPosition.make(si._2.iteratorType,
          si._2.iteratorPosition,
          si._2.subSequenceNumber,
          si._2.isLast
        )),
      defaultPosition
    )
  }

  def getKinesisPosition(options: KinesisOptions): InitialKinesisPosition = {
    val CURRENT_TIMESTAMP = System.currentTimeMillis
    val initialPosition = options.startingPosition match {
      case l if l.equalsIgnoreCase(Latest.iteratorType) =>
        InitialKinesisPosition.fromPredefPosition(new AtTimeStamp(CURRENT_TIMESTAMP))
      case t if t.equalsIgnoreCase(TrimHorizon.iteratorType) =>
        InitialKinesisPosition.fromPredefPosition(new TrimHorizon)
      case e if e.equalsIgnoreCase("EARLIEST") =>
        InitialKinesisPosition.fromPredefPosition(new TrimHorizon)
      case ts if ts.toUpperCase(Locale.ROOT).startsWith(AtTimeStamp.iteratorType) =>
        try {
          val timestamp = getTimestampValue(ts.substring(AtTimeStamp.iteratorType.length + 1).trim)
          InitialKinesisPosition.fromPredefPosition(new AtTimeStamp(timestamp))
        } catch {
          case dte@(_: DateTimeException | _: DateTimeParseException | _: StringIndexOutOfBoundsException) =>
            val except = new IllegalArgumentException(s"${ts} for startingPosition entered in Invalid Format, " +
              "please follow the UTC Format YYYY-MM-DDTHH:MM:SSZ where Z is timezone in UTC +/- offset hours",
              dte)
            throw except
        }
      case json =>
        InitialKinesisPosition.fromCheckpointJson(json, new AtTimeStamp(CURRENT_TIMESTAMP))
    }

    logDebug(s"getKinesisPosition at ${options.startingPosition}: ${initialPosition}")
    initialPosition
  }
}

case class ShardInfo(
                      shardId: String,
                      iteratorType: String,
                      iteratorPosition: String,
                      subSequenceNumber: Long,
                      isLast: Boolean
                    ) extends Serializable {

  def this(shardId: String, kinesisPosition: KinesisPosition) {
    this(shardId,
      kinesisPosition.iteratorType,
      kinesisPosition.iteratorPosition,
      kinesisPosition.subSequenceNumber,
      kinesisPosition.isLast)
  }

  def isAggregated: Boolean = SequenceNumber.isAggregated(subSequenceNumber)
}

case class ShardOffsets(
                         batchId: Long,
                         streamName: String,
                         shardInfoMap: Map[String, ShardInfo]
                       ) extends Serializable {

  def this(batchId: Long, streamName: String) = {
    this(batchId, streamName, Map.empty[String, ShardInfo])
  }

  def this(shardInfoMap: Map[String, ShardInfo]) {
    this(-1, "", shardInfoMap)
  }

  def this(batchId: Long, streamName: String, shardInfos: Array[ShardInfo]) = {
    this(batchId, streamName, KinesisV2SourceOffset.getMap(shardInfos))
  }

  def this(shardInfos: Array[ShardInfo]) = {
    this(-1, "", KinesisV2SourceOffset.getMap(shardInfos))
  }

}
