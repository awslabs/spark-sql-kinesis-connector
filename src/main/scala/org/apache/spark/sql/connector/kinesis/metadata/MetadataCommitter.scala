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

package org.apache.spark.sql.connector.kinesis.metadata

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.ShardInfo
import org.apache.spark.sql.connector.kinesis.metadata.dynamodb.DynamodbMetadataCommitter
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

trait MetadataCommitter[T <: AnyRef] {
  // Functions that various committer need to implement
  // This committed will be used by executors to push metadata related to kinesis shards
  // Possibile Implemetations are HDFS, DynamoDB, Mysql etc
  def add(batchId: Long, shardId: String, metadata: T): Boolean
  def exists(batchId: Long): Boolean
  def get(batchId: Long): Seq[T]
  def get(batchId: Long, shardId: String): Option[T]

  // Removes all the log entry earlier than thresholdBatchId (exclusive).
  def purgeBefore(thresholdBatchId: Long): Unit

  // only keep the latest numVersionsToRetain.  Earlier version are removed
  def purge(numVersionsToRetain: Int): Unit
  def delete(batchId: Long): Boolean
  def delete(batchId: Long, shardId: String): Boolean
}

object MetadataCommitterFactory {
  def createMetadataCommitter(options: KinesisOptions,
                              defaultMetadataPath: String,
                              conf: Option[SerializableConfiguration] = None
                             ): MetadataCommitter[ShardInfo] = {
    options.metadataCommitterType match {
      case h if h.equalsIgnoreCase(KinesisOptions.HDFS_COMMITTER_TYPE) =>
        new HDFSMetadataCommitter[ShardInfo](options.metadataPath.getOrElse(defaultMetadataPath),
          conf.getOrElse{
            new SerializableConfiguration(
              new Configuration()
            )
          },
          options)
      case d if d.equalsIgnoreCase(KinesisOptions.DYNAMODB_COMMITTER_TYPE) =>
        new DynamodbMetadataCommitter[ShardInfo](options.dynamodbTableName, options)
      case _ => throw new IllegalArgumentException("Unsupported committer type")
    }
  }
}
