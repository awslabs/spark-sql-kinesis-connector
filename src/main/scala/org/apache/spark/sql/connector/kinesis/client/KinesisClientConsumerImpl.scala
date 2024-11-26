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

import java.net.URI
import java.time.Duration
import java.time.Instant
import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.util.control.NonFatal

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption
import software.amazon.awssdk.core.exception.AbortedException
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.Http2Configuration
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse
import software.amazon.awssdk.services.kinesis.model.KinesisException
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler
import software.amazon.awssdk.utils.AttributeMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.kinesis.AfterSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtSequenceNumber
import org.apache.spark.sql.connector.kinesis.AtTimeStamp
import org.apache.spark.sql.connector.kinesis.ConnectorAwsCredentialsProvider
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.tryAndIgnoreError
import org.apache.spark.sql.connector.kinesis.uninterruptibleSingleThreadExecutor
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.UninterruptibleThread

// This class uses Kinesis API to read data offsets from Kinesis

case class KinesisClientConsumerImpl(
   options: KinesisOptions,
   override val kinesisStreamName: String,
   endpointUrl: String
) extends Serializable with KinesisClientConsumer with Logging {

  logInfo(s"KinesisClientConsumerImpl.init for ${kinesisStreamName}")
  /*
   * Used to ensure execute fetch operations execute in an UninterruptibleThread
   */
  lazy val kinesisReaderExecutor: ExecutorService = uninterruptibleSingleThreadExecutor("Kinesis-Client-ConsumerImpl")

  lazy val execContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(kinesisReaderExecutor)

  private val maxOffsetFetchAttempts = options.clientNumRetries

  private val offsetFetchAttemptIntervalMs = options.clientRetryIntervalsMs

  private val maxRetryIntervalMs: Long = options.clientMaxRetryIntervalMs

  private val maxResultShardsPerCall = options.maxResultListShardsPerCall

  private val kinesisRegion = options.kinesisRegion
  
  private val connectorAwsCredentialsProvider = {
    val builder = ConnectorAwsCredentialsProvider.builder
    builder.kinesisOptions(options)
    if (options.customCredentialsProviderClass.isDefined) {
      builder.customCredentials(
        options.customCredentialsProviderClass.get,
        options.customCredentialsProviderParam
      )
    }
    if (options.stsRoleArn.isDefined) {
      builder.stsCredentials(
        options.stsRoleArn,
        options.stsSessionName,
        kinesisRegion,
        options.stsEndpointUrl
      )
    } else if (options.awsAccessKeyId.isDefined) {
      builder.staticCredentials(
        options.awsAccessKeyId,
        options.awsSecretKey,
        options.sessionToken
      )
    }
    builder.build()
  }
  
  private val credentialsProvider: AwsCredentialsProvider = connectorAwsCredentialsProvider.provider

  private var _amazonClient: Option[KinesisClient] = None

  private val overrideConfiguration = ClientOverrideConfiguration
    .builder()
    .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, "spark-sql-aws-kinesis-connector")
    .build

  private def getAmazonClient: KinesisClient = synchronized {
    if (_amazonClient.isEmpty) {
      _amazonClient = Some(KinesisClient.builder
        .httpClientBuilder(ApacheHttpClient.builder)
        .overrideConfiguration(overrideConfiguration)
        .credentialsProvider(credentialsProvider)
        .region(Region.of(kinesisRegion))
        .endpointOverride(URI.create(endpointUrl))
        .build())
    }
    _amazonClient.get
  }

  private var _amazonAsyncClient: Option[KinesisAsyncClient] = None

  private var _httpAsyncClient: Option[SdkAsyncHttpClient] = None

  private def getAmazonAsyncClient: KinesisAsyncClient = synchronized {
    val CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60)
    val INITIAL_WINDOW_SIZE_BYTES = 512 * 1024 // 512 KB
    val HEALTH_CHECK_PING_PERIOD = Duration.ofSeconds(90)

    val HTTP_CLIENT_MAX_CONCURRENCY = 10000
    val HTTP_CLIENT_READ_TIMEOUT = options.httpClientReadTimeout
    val HTTP_PROTOCOL = Protocol.HTTP2
    val TRUST_ALL_CERTIFICATES = false
    val HTTP_CLIENT_DEFAULTS = AttributeMap.builder()
        .put[Integer](SdkHttpConfigurationOption.MAX_CONNECTIONS, HTTP_CLIENT_MAX_CONCURRENCY)
        .put[Duration](SdkHttpConfigurationOption.READ_TIMEOUT, HTTP_CLIENT_READ_TIMEOUT)
        .put[java.lang.Boolean](SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, TRUST_ALL_CERTIFICATES)
        .put[Protocol](SdkHttpConfigurationOption.PROTOCOL, HTTP_PROTOCOL)
        .build()

    if (_amazonAsyncClient.isEmpty) {
      _httpAsyncClient = Some(NettyNioAsyncHttpClient.builder()
        .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
        .http2Configuration(
          Http2Configuration.builder()
            .healthCheckPingPeriod(HEALTH_CHECK_PING_PERIOD)
            .initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
            .build())
        .buildWithDefaults(HTTP_CLIENT_DEFAULTS))
      _amazonAsyncClient = Some(KinesisAsyncClient.builder
        .httpClient(_httpAsyncClient.get)
        .credentialsProvider(credentialsProvider)
        .region(Region.of(kinesisRegion))
        .endpointOverride(URI.create(endpointUrl))
        .overrideConfiguration(overrideConfiguration)
        .build())
    }
    _amazonAsyncClient.get
  }

  override def subscribeToShard(
  request: SubscribeToShardRequest,
  responseHandler: SubscribeToShardResponseHandler): CompletableFuture[Void] = {
    runUninterruptibly {
      retryOrTimeout[CompletableFuture[Void]](s"subscribeToShard") {
        getAmazonAsyncClient.subscribeToShard(request, responseHandler)
      }
    }
  }

  override def describeStreamSummary (stream: String): DescribeStreamSummaryResponse = {
    val describeStreamRequest = DescribeStreamSummaryRequest.builder().streamName(stream).build()
    val describeStreamSummaryResult: DescribeStreamSummaryResponse = runUninterruptibly {
      retryOrTimeout[DescribeStreamSummaryResponse](s"describe stream summary") {
        getAmazonClient.describeStreamSummary(describeStreamRequest)
      }
    }
    describeStreamSummaryResult
  }

  override def describeStreamConsumer(streamArn: String,
                             consumerName: String): DescribeStreamConsumerResponse = {
    val describeStreamConsumerRequest = DescribeStreamConsumerRequest.builder()
      .streamARN(streamArn)
      .consumerName(consumerName)
      .build()

    val describeStreamConsumerResult: DescribeStreamConsumerResponse = runUninterruptibly {
      retryOrTimeout[DescribeStreamConsumerResponse](s"describe stream consumer") {
        getAmazonClient
        .describeStreamConsumer(describeStreamConsumerRequest)
      }
    }
    describeStreamConsumerResult
  }

  override def registerStreamConsumer(streamArn: String,
                             consumerName: String): RegisterStreamConsumerResponse = {
    val registerStreamConsumerRequest: RegisterStreamConsumerRequest =
      RegisterStreamConsumerRequest.builder()
        .streamARN(streamArn)
        .consumerName(consumerName)
        .build()

    val registerStreamConsumerResult = runUninterruptibly {
      retryOrTimeout[RegisterStreamConsumerResponse](s"register stream consumer") {
        getAmazonClient.registerStreamConsumer(registerStreamConsumerRequest)
      }
    }
    registerStreamConsumerResult
  }

  override def deregisterStreamConsumer(streamArn: String,
                             consumerName: String): DeregisterStreamConsumerResponse = {
    logInfo(s"KinesisClientConsumerImpl deregisterStreamConsumer for ${kinesisStreamName}")
    val deregisterStreamConsumerRequest: DeregisterStreamConsumerRequest =
      DeregisterStreamConsumerRequest.builder()
        .streamARN(streamArn)
        .consumerName(consumerName)
        .build()

    val deregisterStreamConsumerResult = runUninterruptibly {
      retryOrTimeout[DeregisterStreamConsumerResponse](s"deregister stream consumer") {
        logInfo(s"KinesisClientConsumerImpl sending deregisterStreamConsumerRequest for ${kinesisStreamName}")
        getAmazonClient.deregisterStreamConsumer(deregisterStreamConsumerRequest)
      }
    }
    deregisterStreamConsumerResult
  }

  override def getShards: Seq[Shard] = {
    val shards = listShards
    logDebug(s"List shards in Kinesis Stream:  ${shards}")
    shards.toSeq
  }

  override def close(): Unit = synchronized {
    logInfo(s"KinesisClientConsumerImpl.close for ${kinesisStreamName}")
    runUninterruptibly {
      tryAndIgnoreError("close _amazonClient") { _amazonClient.foreach(_.close()) }
      tryAndIgnoreError("close _amazonAsyncClient") { _amazonAsyncClient.foreach(_.close()) }
      tryAndIgnoreError("close _httpAsyncClient") { _httpAsyncClient.foreach(_.close())}
    }
    
    connectorAwsCredentialsProvider.close()

    kinesisReaderExecutor.shutdown()


  }

  override def getShardIterator(shardId: String,
                       iteratorType: String,
                       iteratorPosition: String,
                       failOnDataLoss: Boolean = true): String = {

    val getShardIteratorRequestBuilder = GetShardIteratorRequest.builder()
      .shardId(shardId)
      .streamName(kinesisStreamName)
      .shardIteratorType(iteratorType)

    if (iteratorType == AfterSequenceNumber.iteratorType || iteratorType == AtSequenceNumber.iteratorType) {
      getShardIteratorRequestBuilder.startingSequenceNumber(iteratorPosition)
    }

    if (iteratorType == AtTimeStamp.iteratorType) {
      logDebug(s"TimeStamp while getting shard iterator ${
        new java.util.Date(iteratorPosition.toLong).toString}")
      getShardIteratorRequestBuilder.timestamp(Instant.ofEpochMilli(iteratorPosition.toLong))
    }

    val getShardIteratorRequest = getShardIteratorRequestBuilder.build()
    val getShardIteratorResult: GetShardIteratorResponse = runUninterruptibly {
      retryOrTimeout[GetShardIteratorResponse](s"Fetching Shard Iterator") {
        try {
          getAmazonClient.getShardIterator(getShardIteratorRequest)
        } catch {
          case r: ResourceNotFoundException =>
            if (!failOnDataLoss) {
              GetShardIteratorResponse.builder().build()
            }
            else {
              throw r
            }
        }
      }
    }


    getShardIteratorResult.shardIterator()
  }


  override def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
    val getRecordsRequest = GetRecordsRequest.builder()
      .shardIterator(shardIterator)
      .limit(limit)
      .build()

    val getRecordsResult: GetRecordsResponse = runUninterruptibly {
      retryOrTimeout[GetRecordsResponse ](s"get Records for a shard ") {
        getAmazonClient.getRecords(getRecordsRequest)
      }
    }
    getRecordsResult
  }

  private def listShards = {
    var nextToken = ""
    var returnedToken = ""
    val shards = new util.ArrayList[Shard]()
    var listShardsRequest = ListShardsRequest.builder()
      .streamName(kinesisStreamName)
      .maxResults(maxResultShardsPerCall)
      .build()

    do {
      val listShardsResult: ListShardsResponse = runUninterruptibly {
        retryOrTimeout[ListShardsResponse]( s"List shards") {
          getAmazonClient.listShards(listShardsRequest)
        }
      }
      shards.addAll(listShardsResult.shards)
      returnedToken = listShardsResult.nextToken()
      if (returnedToken != null) {
        nextToken = returnedToken
        listShardsRequest = ListShardsRequest.builder()
          .nextToken(nextToken) // nextToken and streamName cannot be provided together
          .maxResults(maxResultShardsPerCall)
          .build()
      } else {
        nextToken = ""
      }
    } while (nextToken.nonEmpty)

    shards.asScala
  }

  /*
   * This method ensures that the closure is called in an [[UninterruptibleThread]].
   * This is required when communicating with the AWS.
   */
  private def runUninterruptibly[T](body: => T) = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      logDebug("currentThread is not UninterruptibleThread. run in execContext")
      val future = Future {
        body
      }(execContext)

      var result: T = null.asInstanceOf[T]
      var shouldInterruptThread = false
      var isRunning = true
      while (isRunning) {
        try {
          result = ThreadUtils.awaitResult(future, scala.concurrent.duration.Duration.Inf)
          isRunning = false
        } catch {
          case ie if ie.isInstanceOf[InterruptedException] || ie.getCause.isInstanceOf[InterruptedException] =>
            shouldInterruptThread = true
          case NonFatal(e) =>
            throw e
        }
      }

      if(shouldInterruptThread) Thread.currentThread().interrupt()

      result
    } else {
        body
    }
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String)(body: => T): T = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    var retryCount = 0
    var result: Option[T] = None
    var lastError: Throwable = null
    var waitTimeInterval = offsetFetchAttemptIntervalMs

    def isMaxRetryDone = retryCount >= maxOffsetFetchAttempts

    while (result.isEmpty && !isMaxRetryDone) {
      if ( retryCount > 0 ) { // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval = scala.math.min(waitTimeInterval * 2, maxRetryIntervalMs)
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case pte: ProvisionedThroughputExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", pte)
            case lee: LimitExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", lee)
            case ae: AbortedException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ae)
            case ake: KinesisException =>
              if (ake.statusCode() >= 500) {
                logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ake)
              } else {
                throw ake
              }
            case e: Throwable =>
              throw new IllegalStateException(s"Error while $message", e)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      throw new IllegalStateException(
        s"Gave up after $retryCount retries while $message, last exception: ", lastError)
    }
  }

}
