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

import java.util.Locale
import java.util.concurrent.{ExecutionException, TimeUnit}

import scala.util.Try
import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration}
import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object CachedKinesisProducer extends Logging {

  private type Producer = KinesisProducer

  private lazy val cacheExpireTimeout: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.kinesis.producer.cache.timeout", "10m")

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], Producer] {
    override def load(config: Seq[(String, Object)]): Producer = {
      val configMap = config.map(x => x._1 -> x._2.toString).toMap
      createKinesisProducer(configMap)
    }
  }

  private val removalListener = new RemovalListener[Seq[(String, Object)], Producer]() {
    override def onRemoval(notification:
                           RemovalNotification[Seq[(String, Object)], Producer]): Unit = {
      val paramsSeq: Seq[(String, Object)] = notification.getKey
      val producer: Producer = notification.getValue
      logDebug(
        s"Evicting kinesis producer $producer params: $paramsSeq," +
          s" due to ${notification.getCause}")
      close(paramsSeq, producer)
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], Producer] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], Producer](cacheLoader)

  private def createKinesisProducer(producerConfiguration: Map[String, String]): Producer = {

    val region = producerConfiguration(
      KinesisOptions.REGION.toLowerCase(Locale.ROOT)
    )

    val recordMaxBufferedTime = producerConfiguration.getOrElse(
      KinesisOptions.SINK_RECORD_MAX_BUFFERED_TIME.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_RECORD_MAX_BUFFERED_TIME)
      .toLong

    val recordTTL = producerConfiguration.getOrElse(
      KinesisOptions.SINK_RECORD_TTL.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_RECORD_TTL)
      .toLong

    val maxConnections = producerConfiguration.getOrElse(
      KinesisOptions.SINK_MAX_CONNECTIONS.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_MAX_CONNECTIONS)
      .toInt

    val rateLimit = producerConfiguration.getOrElse(
      KinesisOptions.SINK_RATE_LIMIT.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_MAX_CONNECTIONS)
      .toLong

    val threadingModel = producerConfiguration.getOrElse(
      KinesisOptions.SINK_THREADING_MODEL.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_THREADING_MODEL)
      .toString

    val threadPoolSize = producerConfiguration.getOrElse(
      KinesisOptions.SINK_THREAD_POOL_SIZE.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_THREAD_POOL_SIZE)
      .toInt

    val aggregation = Try { producerConfiguration.getOrElse(
      KinesisOptions.SINK_AGGREGATION_ENABLED.toLowerCase(Locale.ROOT),
      KinesisOptions.DEFAULT_SINK_AGGREGATION)
      .toBoolean } getOrElse { KinesisOptions.DEFAULT_SINK_AGGREGATION.toBoolean}

    val kinesisProducerConfiguration = new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(recordMaxBufferedTime)
      .setMaxConnections(maxConnections)
      .setAggregationEnabled(aggregation)
      .setCredentialsProvider(
        com.amazonaws.auth.DefaultAWSCredentialsProviderChain.getInstance
      )
      .setRegion(region)
      .setRecordTtl(recordTTL)
      .setThread(threadingModel)
      .setThreadPoolSize(threadPoolSize)
      .setRateLimit(rateLimit)

    // check for proxy settings
    if (producerConfiguration.contains(KinesisOptions.PROXY_ADDRESS.toLowerCase(Locale.ROOT))) {
      val proxyAddress = producerConfiguration.get(KinesisOptions.PROXY_ADDRESS.toLowerCase(Locale.ROOT))
      if (proxyAddress.isDefined) {
        kinesisProducerConfiguration.setProxyHost(proxyAddress.get)
      }
    }

    if (producerConfiguration.contains(KinesisOptions.PROXY_PORT.toLowerCase(Locale.ROOT))) {
      val proxyPort = producerConfiguration.get(KinesisOptions.PROXY_PORT.toLowerCase(Locale.ROOT))
      if (proxyPort.isDefined) {
        kinesisProducerConfiguration.setProxyPort(proxyPort.get.toLong)
      }
    }

    if (producerConfiguration.contains(KinesisOptions.PROXY_USERNAME.toLowerCase(Locale.ROOT))) {
      val proxyUsername = producerConfiguration.get(KinesisOptions.PROXY_USERNAME.toLowerCase(Locale.ROOT))
      if (proxyUsername.isDefined) {
        kinesisProducerConfiguration.setProxyUserName(proxyUsername.get)
      }
    }

    if (producerConfiguration.contains(KinesisOptions.PROXY_PASSWORD.toLowerCase(Locale.ROOT))) {
      val proxyPassword = producerConfiguration.get(KinesisOptions.PROXY_PASSWORD.toLowerCase(Locale.ROOT))
      if (proxyPassword.isDefined) {
        kinesisProducerConfiguration.setProxyPassword(proxyPassword.get)
      }
    }

    val kinesisProducer = new Producer(kinesisProducerConfiguration)

    logDebug(s"Created a new instance of KinesisProducer for $producerConfiguration.")
    kinesisProducer
  }

  def getOrCreate(caseInsensitiveParams: Map[String, String]): Producer = {
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(
      caseInsensitiveParams
    )

    try {
      guavaCache.get(paramsSeq)
    } catch {
      case e@(_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(kinesisParams: Map[String, String]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kinesisParams.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /** For explicitly closing kinesis producer */
  def close(kinesisParams: Map[String, String]): Unit = {
    val paramsSeq = paramsToSeq(kinesisParams)
    guavaCache.invalidate(paramsSeq)
  }

  /** Auto close on cache evict */
  private def close(paramsSeq: Seq[(String, Object)], producer: Producer): Unit = {
    try {
      logInfo(s"Closing the KinesisProducer with params: ${paramsSeq.mkString("\n")}.")
      producer.flushSync()
      producer.destroy()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kinesis producer.", e)
    }
  }

  private def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

}
