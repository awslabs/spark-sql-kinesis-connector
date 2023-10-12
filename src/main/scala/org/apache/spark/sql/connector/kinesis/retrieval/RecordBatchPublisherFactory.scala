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

package org.apache.spark.sql.connector.kinesis.retrieval

import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.KinesisOptions.EFO_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisOptions.POLLING_CONSUMER_TYPE
import org.apache.spark.sql.connector.kinesis.KinesisPosition
import org.apache.spark.sql.connector.kinesis.client.KinesisClientConsumer
import org.apache.spark.sql.connector.kinesis.retrieval.efo.EfoRecordBatchPublisher
import org.apache.spark.sql.connector.kinesis.retrieval.polling.PollingRecordBatchPublisher

private [kinesis] object RecordBatchPublisherFactory {

  def create(
              startingPosition: KinesisPosition,
              consumerArn: String,
              streamShard: StreamShard,
              kinesisConsumer: KinesisClientConsumer,
              kinesisOptions: KinesisOptions,
              runningSupplier: => Boolean): RecordBatchPublisher = {

    kinesisOptions.consumerType match {
      case EFO_CONSUMER_TYPE =>
         new EfoRecordBatchPublisher(
          startingPosition,
          consumerArn: String,
          streamShard: StreamShard,
          kinesisConsumer: KinesisClientConsumer,
          kinesisOptions: KinesisOptions,
          runningSupplier
        )
      case POLLING_CONSUMER_TYPE =>
        new PollingRecordBatchPublisher(
          startingPosition,
          streamShard: StreamShard,
          kinesisConsumer: KinesisClientConsumer,
          kinesisOptions: KinesisOptions,
          runningSupplier
        )
      case _ => throw new IllegalArgumentException(s"Unknown consumerType: $kinesisOptions.consumerType")
    }

  }
}