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
package org.apache.spark.sql.connector.kinesis.client

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler
trait KinesisClientConsumer {
  def subscribeToShard(
                        request: SubscribeToShardRequest,
                        responseHandler: SubscribeToShardResponseHandler): CompletableFuture[Void]

  def describeStreamSummary (stream: String): DescribeStreamSummaryResponse

  def describeStreamConsumer(streamArn: String,
                             consumerName: String): DescribeStreamConsumerResponse

  def registerStreamConsumer(streamArn: String,
                             consumerName: String): RegisterStreamConsumerResponse

  def deregisterStreamConsumer(streamArn: String,
                               consumerName: String): DeregisterStreamConsumerResponse

  def getShards: Seq[Shard]

  def getShardIterator(shardId: String,
                        iteratorType: String,
                        iteratorPosition: String,
                        failOnDataLoss: Boolean = false): String

  def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse

  def kinesisStreamName: String
  def close(): Unit

}
