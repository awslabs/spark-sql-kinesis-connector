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

import java.time.Duration

import scala.util.Try

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

case class KinesisOptions (
                            region: String,
                            endpointUrl: String,
                            streamName: String,
                            consumerType: String,
                            consumerName: Option[String],
                            failOnDataLoss: Boolean,
                            avoidEmptyBatches: Boolean,
                            maxFetchRecordsPerShard: Int,
                            maxFetchTimePerShardSec: Option[Int],
                            startingPosition: String,
                            describeShardIntervalMs: Long,
                            minBatchesToRetain: Option[Int],
                            checkNewRecordThreads: Int,
                            metadataCommitterType: String,
                            metadataPath: Option[String],
                            metadataNumRetries: Int,
                            metadataRetryIntervalsMs: Long,
                            metadataMaxRetryIntervalMs: Long,
                            dynamodbTableName: String,
                            clientNumRetries: Int,
                            clientRetryIntervalsMs: Long,
                            clientMaxRetryIntervalMs: Long,
                            maxDataQueueEmptyCount: Int,
                            dataQueueWaitTimeout: Duration,
                            dataQueueCapacity: Int,
                            efoSubscribeToShardTimeout: Duration,
                            efoSubscribeToShardMaxRetries: Int,
                            pollingNumberOfRecordsPerFetch: Int,
                            pollingFetchIntervalMs: Long,
                            maintenanceTaskIntervalSec: Int,
                            stsRoleArn: Option[String],
                            stsSessionName: Option[String],
                            stsEndpointUrl: Option[String],
                            awsAccessKeyId: Option[String],
                            awsSecretKey: Option[String],
                            sessionToken: Option[String],
                            customCredentialsProviderClass: Option[String],
                            customCredentialsProviderParam: Option[String],
                            kinesisRegion: String,
                            proxyAddress: Option[String],
                            proxyPort: Option[String],
                            proxyUsername: Option[String],
                            proxyPassword: Option[String],
                            maxResultListShardsPerCall: Int = 1000,
                            waitTableActiveTimeoutSeconds: Int = 600,
                            waitTableActiveSecondsBetweenPolls: Int = 10,
                            httpClientReadTimeout: Duration = Duration.ofMinutes(3),
)
object KinesisOptions {
  private val PREFIX = "kinesis."
  private val EFO_PREFIX = PREFIX + "subscribeToShard."
  private val POLLING_PREFIX = PREFIX + "getRecords."
  private val SINK_PREFIX = PREFIX + "sink."
  private val DYNAMODB_PREFIX = PREFIX + "dynamodb."
  private val INTERNAL_PREFIX = PREFIX + "internal."

  val POLLING_CONSUMER_TYPE = "GetRecords"
  val EFO_CONSUMER_TYPE = "SubscribeToShard"

  val HDFS_COMMITTER_TYPE: String = "HDFS"
  val DYNAMODB_COMMITTER_TYPE: String = "DYNAMODB"

  val REGION: String = PREFIX + "region"
  val ENDPOINT_URL: String = PREFIX + "endpointUrl"
  val STREAM_NAME: String = PREFIX + "streamName"
  val CONSUMER_TYPE: String = PREFIX + "consumerType"
  val CONSUMER_NAME: String = PREFIX + "consumerName"
  val FAIL_ON_DATA_LOSS: String = PREFIX + "failOnDataLoss"

  val MAX_FETCH_RECORDS_PER_SHARD: String = PREFIX + "maxFetchRecordsPerShard"
  val MAX_FETCH_TIME_PER_SHARD_SEC: String = PREFIX + "maxFetchTimePerShardSec"
  val STARTING_POSITION: String = PREFIX + "startingPosition"
  val DESCRIBE_SHARD_INTERVAL: String = PREFIX + "describeShardInterval"
  val MIN_BATCHES_TO_RETAIN: String = PREFIX + "minBatchesToRetain"
  val CHECK_NEW_RECORD_THREADS: String = PREFIX + "checkNewRecordThreads"

  val METADATA_COMMITTER_TYPE: String = PREFIX + "metadataCommitterType"
  val METADATA_PATH: String = PREFIX + "metadataPath"
  val METADATA_NUMBER_RETRIES: String = PREFIX + "metadataNumRetries"
  val METADATA_RETRY_INTERVALS_MS: String = PREFIX + "metadataRetryIntervalsMs"
  val METADATA_MAX_RETRY_INTERVALS_MS: String = PREFIX + "metadataMaxRetryIntervalMs"

  val CLIENT_NUMBER_RETRIES: String = PREFIX + "clientNumRetries"
  val CLIENT_RETRY_INTERVALS_MS: String = PREFIX + "clientRetryIntervalsMs"
  val CLIENT_MAX_RETRY_INTERVALS_MS: String = PREFIX + "clientMaxRetryIntervalMs"

  val DATA_QUEUE_CAPACITY: String = INTERNAL_PREFIX + "dataQueueCapacity"
  val MAX_DATA_QUEUE_EMPTY_COUNT: String = INTERNAL_PREFIX + "maxDataQueueEmptyCount"
  val DATA_QUEUE_WAIT_TIME_SEC: String = INTERNAL_PREFIX + "dataQueueWaitTimeoutSec"

  val STS_ROLE_ARN: String = PREFIX + "stsRoleArn"
  val STS_SESSION_NAME: String = PREFIX + "stsSessionName"
  val STS_ENDPOINT_URL: String = PREFIX + "stsEndpointUrl"
  
  val AWS_ACCESS_KEY_ID: String = PREFIX + "awsAccessKeyId"
  val AWS_SECRET_KEY: String = PREFIX + "awsSecretKey"
  val AWS_SESSION_TOKEN: String = PREFIX + "sessionToken"

  val CUSTOM_CREDENTIAL_PROVIDER_CLASS: String = PREFIX + "credentialProviderClass"
  val CUSTOM_CREDENTIAL_PROVIDER_PARAM: String = PREFIX + "credentialProviderParam"
  
  val KINESIS_REGION: String = PREFIX + "kinesisRegion"

  val DYNAMODB_TABLE_NAME: String = DYNAMODB_PREFIX + "tableName"
  
  val EFO_SUBSCRIBE_TO_SHARD_TIMEOUT: String = EFO_PREFIX + "timeoutSec"
  val EFO_SUBSCRIBE_TO_SHARD_RETRIES: String = EFO_PREFIX + "maxRetries"

  val POLLING_NUMBER_OF_RECORDS_PER_FETCH: String = POLLING_PREFIX + "numberOfRecordsPerFetch"
  val POLLING_FETCH_INTERVAL_MILLIS: String = POLLING_PREFIX + "fetchIntervalMs"

  val MAINTENANCE_TASK_INTERVAL_SEC: String = INTERNAL_PREFIX + "maintenanceTaskIntervalSec"

  //  This parameter can't fully avoid empty batches as Kinesis automatically generates empty events
  val AVOID_EMPTY_BATCHES: String = INTERNAL_PREFIX + "avoidEmptyBatches"

  val DEFAULT_CONSUMER_TYPE: String = POLLING_CONSUMER_TYPE
  val DEFAULT_MAX_FETCH_RECORDS_PER_SHARD: Int = 100000
  val DEFAULT_DESCRIBE_SHARD_INTERVAL: String = "1s"
  val DEFAULT_STARTING_POSITION: String = Latest.iteratorType
  val DEFAULT_FAIL_ON_DATA_LOSS: Boolean = false
  val DEFAULT_AVOID_EMPTY_BATCHES: Boolean = true
  val DEFAULT_CHECK_NEW_RECORD_THREADS: Int = 8

  val DEFAULT_METADATA_NUMBER_RETRIES: Int = 5
  val DEFAULT_METADATA_RETRY_INTERVALS_MS: Long = 1000L
  val DEFAULT_METADATA_MAX_RETRY_INTERVALS_MS: Long = 10000L

  val DEFAULT_CLIENT_NUMBER_RETRIES: Int = 5
  val DEFAULT_CLIENT_RETRY_INTERVALS_MS: Long = 1000L
  val DEFAULT_CLIENT_MAX_RETRY_INTERVALS_MS: Long = 10000L

  val DEFAULT_DATA_QUEUE_CAPACITY = 1000
  val DEFAULT_MAX_DATA_QUEUE_EMPTY_COUNT = 2
  val DEFAULT_DATA_QUEUE_WAIT_TIME_SEC = 5

  val DEFAULT_EFO_SUBSCRIBE_TO_SHARD_TIMEOUT: Int = 60 // seconds
  val DEFAULT_EFO_SUBSCRIBE_TO_SHARD_MAX_RETRIES: Int = 10

  val DEFAULT_POLLING_MAX_NUMBER_OF_RECORDS_PER_FETCH: Int = 10000
  val DEFAULT_POLLING_FETCH_INTERVAL_MILLIS: Long = 200L

  val DEFAULT_MAINTENANCE_TASK_INTERVAL_SEC = 100
  
  // Sink options
  val SINK_RECORD_TTL: String = SINK_PREFIX + "recordTtl"
  val SINK_FLUSH_WAIT_TIME_MILLIS: String = SINK_PREFIX + "flushWaitTimeMs"
  val SINK_RECORD_MAX_BUFFERED_TIME: String = SINK_PREFIX + "recordMaxBufferedTimeMs"
  val SINK_MAX_CONNECTIONS: String = SINK_PREFIX + "maxConnections"
  val SINK_RATE_LIMIT: String = SINK_PREFIX + "rateLimit"
  val SINK_THREAD_POOL_SIZE: String = SINK_PREFIX + "threadPoolSize"
  val SINK_THREADING_MODEL: String = SINK_PREFIX + "threadPool"
  val SINK_AGGREGATION_ENABLED: String = SINK_PREFIX + "aggregationEnabled"

  val DEFAULT_SINK_FLUSH_WAIT_TIME_MILLIS: String = "100"
  val DEFAULT_SINK_RECORD_TTL: String = "30000"
  val DEFAULT_SINK_RECORD_MAX_BUFFERED_TIME: String = "1000"
  val DEFAULT_SINK_MAX_CONNECTIONS: String = "1"
  val DEFAULT_SINK_RATE_LIMIT: String = "150"
  val DEFAULT_SINK_THREADING_MODEL: String = "PER_REQUEST"
  val DEFAULT_SINK_THREAD_POOL_SIZE: String = "64"
  val DEFAULT_SINK_AGGREGATION: String = "true"

  // proxy options
  val PROXY_ADDRESS: String = SINK_PREFIX + "proxyAddress"
  val PROXY_PORT: String = SINK_PREFIX + "proxyPort"
  val PROXY_USERNAME: String = SINK_PREFIX + "proxyUsername"
  val PROXY_PASSWORD: String = SINK_PREFIX + "proxyPassword"

  def apply(parameters: CaseInsensitiveStringMap): KinesisOptions = {

    val endpointUrl: String = parameterGetStringOrElse(parameters, ENDPOINT_URL,
      throw new IllegalArgumentException(s"${ENDPOINT_URL} is not specified"))

    val region: String = parameterGetStringOrElse(parameters, REGION,
      getRegionNameByEndpoint(endpointUrl))

    val streamName: String = parameterGetStringOrElse(parameters, STREAM_NAME,
      throw new IllegalArgumentException(s"${STREAM_NAME} is not specified"))

    val consumerType: String = {
      val param = parameterGetStringOrElse(parameters, CONSUMER_TYPE,
        DEFAULT_CONSUMER_TYPE)

      param match {
        case t if (t.equalsIgnoreCase(POLLING_CONSUMER_TYPE)
          || t.equalsIgnoreCase(EFO_CONSUMER_TYPE)) => t
        case _ => throw new IllegalArgumentException(s"${CONSUMER_TYPE} unknown - ${param}")
      }
    }

    val failOnDataLoss: Boolean = parameterGetBooleanOrElse(parameters,
      FAIL_ON_DATA_LOSS, DEFAULT_FAIL_ON_DATA_LOSS)

    val avoidEmptyBatches: Boolean = parameterGetBooleanOrElse(parameters,
      AVOID_EMPTY_BATCHES, DEFAULT_AVOID_EMPTY_BATCHES)

    val maxFetchRecordsPerShard: Int = parameterGetPositiveIntOrElse(parameters,
      MAX_FETCH_RECORDS_PER_SHARD,
      DEFAULT_MAX_FETCH_RECORDS_PER_SHARD)

    val maxFetchTimePerShardSec: Option[Int] = parameterGetPositiveIntOrElseOption(parameters,
      MAX_FETCH_TIME_PER_SHARD_SEC,
      None
    )

    maxFetchTimePerShardSec.foreach { maxFetchTime =>
      // maxFetchTime can't be too small as KDS sometimes takes more than 5s to response
      if (maxFetchTime < 10) {
        throw new IllegalArgumentException(s"${MAX_FETCH_TIME_PER_SHARD_SEC} must be no less than 10")
      }
    }

    val startingPosition: String = parameterGetStringOrElse(parameters,
      STARTING_POSITION,
      DEFAULT_STARTING_POSITION)

    val describeShardIntervalMs: Long = Utils.timeStringAsMs(parameterGetStringOrElse(parameters,
      DESCRIBE_SHARD_INTERVAL,
      DEFAULT_DESCRIBE_SHARD_INTERVAL))
    
    if(describeShardIntervalMs<0) {
      throw new IllegalArgumentException(s"${DESCRIBE_SHARD_INTERVAL} cannot be less than 0")
    }

    val minBatchesToRetain: Option[Int] = parameterGetPositiveIntOrElseOption(parameters,
      MIN_BATCHES_TO_RETAIN,
      None
      )

    val checkNewRecordThreads: Int = parameterGetPositiveIntOrElse(parameters,
      CHECK_NEW_RECORD_THREADS,
      DEFAULT_CHECK_NEW_RECORD_THREADS)

    val metadataCommitterType: String = parameterGetStringOrElse(parameters,
      METADATA_COMMITTER_TYPE,
      HDFS_COMMITTER_TYPE)

    val metadataPath: Option[String] = Option(parameters.get(METADATA_PATH))

    val metadataNumRetries: Int = parameterGetNonNegativeIntOrElse(parameters,
      METADATA_NUMBER_RETRIES,
      DEFAULT_METADATA_NUMBER_RETRIES)

    val metadataRetryIntervalsMs: Long = parameterGetNonNegativeLongOrElse(parameters,
      METADATA_RETRY_INTERVALS_MS,
      DEFAULT_METADATA_RETRY_INTERVALS_MS)

    val metadataMaxRetryIntervalMs: Long = parameterGetNonNegativeLongOrElse(parameters,
      METADATA_MAX_RETRY_INTERVALS_MS,
      DEFAULT_METADATA_MAX_RETRY_INTERVALS_MS)

    val clientNumRetries: Int = parameterGetNonNegativeIntOrElse(parameters,
      CLIENT_NUMBER_RETRIES,
      DEFAULT_CLIENT_NUMBER_RETRIES)

    val clientRetryIntervalsMs: Long = parameterGetNonNegativeLongOrElse(parameters,
      CLIENT_RETRY_INTERVALS_MS,
      DEFAULT_CLIENT_RETRY_INTERVALS_MS)

    val clientMaxRetryIntervalMs: Long = parameterGetNonNegativeLongOrElse(parameters,
      CLIENT_MAX_RETRY_INTERVALS_MS,
      DEFAULT_CLIENT_MAX_RETRY_INTERVALS_MS)

    val dataQueueCapacity: Int = parameterGetNonNegativeIntOrElse(parameters,
      DATA_QUEUE_CAPACITY,
      DEFAULT_DATA_QUEUE_CAPACITY)

    val maxDataQueueEmptyCount: Int = parameterGetNonNegativeIntOrElse(parameters,
      MAX_DATA_QUEUE_EMPTY_COUNT,
      DEFAULT_MAX_DATA_QUEUE_EMPTY_COUNT)

    val dataQueueWaitTimeout: Duration = {
      val timeoutSec = parameterGetNonNegativeIntOrElse(parameters,
          DATA_QUEUE_WAIT_TIME_SEC,
          DEFAULT_DATA_QUEUE_WAIT_TIME_SEC)

      // wait for more than 5s to ensure there are no real data events after empty events
      if ((timeoutSec * maxDataQueueEmptyCount) <= 5 ) {
        throw new IllegalArgumentException(s"${MAX_DATA_QUEUE_EMPTY_COUNT} * ${DATA_QUEUE_WAIT_TIME_SEC} must be greater than 5")
      }

      Duration.ofSeconds(timeoutSec)
    }

    val stsRoleArn: Option[String] = Option(parameters.get(STS_ROLE_ARN))
    val stsSessionName: Option[String] = {
      val sessionName = Option(parameters.get(STS_SESSION_NAME))
      if (stsRoleArn.isDefined != sessionName.isDefined) {
        throw new IllegalArgumentException(s"${STS_ROLE_ARN} and ${STS_SESSION_NAME} must be both defined or empty")
      }
      sessionName
    }
    val stsEndpointUrl: Option[String] = Option(parameters.get(STS_ENDPOINT_URL))
    
    val awsAccessKeyId: Option[String] = Option(parameters.get(AWS_ACCESS_KEY_ID))
    val awsSecretKey: Option[String] = {
      val secret = Option(parameters.get(AWS_SECRET_KEY))
      if (awsAccessKeyId.isDefined != secret.isDefined) {
        throw new IllegalArgumentException(s"${AWS_ACCESS_KEY_ID} and ${AWS_SECRET_KEY} must be both defined or empty")
      }
      secret
    }
    val sessionToken: Option[String] = Option(parameters.get(AWS_SESSION_TOKEN))

    val customCredentialsProviderClass: Option[String] = Option(parameters.get(CUSTOM_CREDENTIAL_PROVIDER_CLASS))
    val customCredentialsProviderParam: Option[String] = Option(parameters.get(CUSTOM_CREDENTIAL_PROVIDER_PARAM))
    
    val kinesisRegion = parameterGetStringOrElse(parameters, KINESIS_REGION,
      getRegionNameByEndpoint(endpointUrl))

    val consumerName: Option[String] = {
      val name = Option(parameters.get(CONSUMER_NAME))
      if (consumerType == EFO_CONSUMER_TYPE && name.isEmpty) {
        throw new IllegalArgumentException(s"${CONSUMER_NAME} must be specified for ${EFO_CONSUMER_TYPE}")
      }
      name
    }

    val efoSubscribeToShardTimeout: Duration = Duration.ofSeconds(
      parameterGetNonNegativeIntOrElse(parameters,
        EFO_SUBSCRIBE_TO_SHARD_TIMEOUT,
        DEFAULT_EFO_SUBSCRIBE_TO_SHARD_TIMEOUT))

    val efoSubscribeToShardMaxRetries: Int = parameterGetNonNegativeIntOrElse(parameters,
      EFO_SUBSCRIBE_TO_SHARD_RETRIES,
      DEFAULT_EFO_SUBSCRIBE_TO_SHARD_MAX_RETRIES)

    val pollingNumberOfRecordsPerFetch: Int = parameterGetPositiveIntOrElse(parameters,
      POLLING_NUMBER_OF_RECORDS_PER_FETCH,
      DEFAULT_POLLING_MAX_NUMBER_OF_RECORDS_PER_FETCH)

    val pollingFetchIntervalMs: Long = parameterGetNonNegativeLongOrElse(parameters,
      POLLING_FETCH_INTERVAL_MILLIS,
      DEFAULT_POLLING_FETCH_INTERVAL_MILLIS)

    val dynamodbTableName: String = parameterGetStringOrElse(parameters,
      DYNAMODB_TABLE_NAME,
      consumerType match {
        case POLLING_CONSUMER_TYPE => s"kdsc_${region}_${streamName}"
        case EFO_CONSUMER_TYPE => s"kdsc_${region}_${streamName}_${consumerName.getOrElse("")}"
      })

    val maintenanceTaskIntervalSec = parameterGetPositiveIntOrElse(parameters,
      MAINTENANCE_TASK_INTERVAL_SEC,
      DEFAULT_MAINTENANCE_TASK_INTERVAL_SEC)

    // adding optionality for proxy configuration
    val proxyAddress = Option(parameters.get(PROXY_ADDRESS))
    val proxyPort = Option(parameters.get(PROXY_PORT))
    val proxyUsername = Option(parameters.get(PROXY_USERNAME))
    val proxyPassword = Option(parameters.get(PROXY_PASSWORD))

    new KinesisOptions(
      region = region,
      endpointUrl = endpointUrl,
      streamName = streamName,
      consumerType = consumerType,
      failOnDataLoss = failOnDataLoss,
      maxFetchRecordsPerShard = maxFetchRecordsPerShard,
      maxFetchTimePerShardSec = maxFetchTimePerShardSec,
      startingPosition = startingPosition,
      describeShardIntervalMs = describeShardIntervalMs,
      avoidEmptyBatches = avoidEmptyBatches,
      minBatchesToRetain = minBatchesToRetain,
      checkNewRecordThreads = checkNewRecordThreads,
      metadataCommitterType = metadataCommitterType,
      metadataPath = metadataPath,
      metadataNumRetries = metadataNumRetries,
      metadataRetryIntervalsMs = metadataRetryIntervalsMs,
      metadataMaxRetryIntervalMs = metadataMaxRetryIntervalMs,
      dynamodbTableName = dynamodbTableName,
      clientNumRetries = clientNumRetries,
      clientRetryIntervalsMs = clientRetryIntervalsMs,
      clientMaxRetryIntervalMs = clientMaxRetryIntervalMs,
      dataQueueCapacity = dataQueueCapacity,
      maxDataQueueEmptyCount = maxDataQueueEmptyCount,
      dataQueueWaitTimeout = dataQueueWaitTimeout,
      consumerName = consumerName,
      efoSubscribeToShardTimeout = efoSubscribeToShardTimeout,
      efoSubscribeToShardMaxRetries = efoSubscribeToShardMaxRetries,
      pollingNumberOfRecordsPerFetch = pollingNumberOfRecordsPerFetch,
      pollingFetchIntervalMs = pollingFetchIntervalMs,
      maintenanceTaskIntervalSec = maintenanceTaskIntervalSec,
      stsRoleArn = stsRoleArn,
      stsSessionName = stsSessionName,
      stsEndpointUrl = stsEndpointUrl,
      awsAccessKeyId = awsAccessKeyId,
      awsSecretKey = awsSecretKey,
      sessionToken = sessionToken,
      customCredentialsProviderClass = customCredentialsProviderClass,
      customCredentialsProviderParam = customCredentialsProviderParam,
      kinesisRegion = kinesisRegion,
      proxyAddress = proxyAddress,
      proxyPort = proxyPort,
      proxyUsername = proxyUsername,
      proxyPassword = proxyPassword
    )
  }

  private def parameterGetStringOrElse(parameters: CaseInsensitiveStringMap,
                        key: String,
                        default: => String ): String = {
    val value = parameters.get(key)
    if(value != null) value.trim
    else default
  }

  private def parameterGetBooleanOrElse(parameters: CaseInsensitiveStringMap,
                                       key: String,
                                       default: => Boolean): Boolean = {
    val value = parameters.get(key)
    if (value != null) {
      Try(value.toBoolean).toOption.getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$value' for option '${key}', must be true or false")
      }
    }
    else default
  }

  private def parameterGetNonNegativeIntOrElse(parameters: CaseInsensitiveStringMap,
                                 key: String,
                                 default: => Int ): Int = {
    val value = parameters.get(key)
    if(value != null) {
      Try(value.toInt).toOption.filter(_ >= 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$value' for option '${key}', must be zero or a positive integer")
      }
    }
    else default
  }

  private def parameterGetPositiveIntOrElse(parameters: CaseInsensitiveStringMap,
                                               key: String,
                                               default: => Int): Int = {
    val value = parameters.get(key)
    if (value != null) {
      Try(value.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$value' for option '${key}', must be a positive integer")
      }
    }
    else default
  }

  private def parameterGetPositiveIntOrElseOption(parameters: CaseInsensitiveStringMap,
                                            key: String,
                                            default: => Option[Int]): Option[Int] = {
    val value = parameters.get(key)
    if (value != null) {
      Some(Try(value.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$value' for option '${key}', must be a positive integer")
      })
    }
    else default
  }

  private def parameterGetNonNegativeLongOrElse(parameters: CaseInsensitiveStringMap,
                                               key: String,
                                               default: => Long): Long = {
    val value = parameters.get(key)
    if (value != null) {
      Try(value.toLong).toOption.filter(_ >= 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$value' for option '${key}', must be zero or a positive long")
      }
    }
    else default
  }
}
