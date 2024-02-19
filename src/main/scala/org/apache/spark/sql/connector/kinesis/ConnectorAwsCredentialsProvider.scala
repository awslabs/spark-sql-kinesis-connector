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

import org.apache.spark.internal.Logging

import java.io.Closeable
import java.net.URI

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
sealed trait ConnectorAwsCredentialsProvider extends Serializable with Closeable {
  def provider: AwsCredentialsProvider
  override def close(): Unit = {}
}

// Using permanent credentials are not recommended due to security concerns.
case class BasicAwsCredentials (
     awsAccessKeyId: String,
     awsSecretKey: String) extends ConnectorAwsCredentialsProvider {
  def provider: AwsCredentialsProvider = {
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey)
          )
    }
}

// For test only. Session credentials can expire. Don't use this in production environment.
case class BasicAwsSessionCredentials(
    awsAccessKeyId: String,
    awsSecretKey: String,
    sessionToken: String) extends ConnectorAwsCredentialsProvider {
  def provider: AwsCredentialsProvider = {
    StaticCredentialsProvider.create(
        AwsSessionCredentials.create(awsAccessKeyId, awsSecretKey, sessionToken)
       )
   }
}


case class RetryableDefaultCredentialsProvider() extends AwsCredentialsProvider with Closeable with Logging {
  private val provider = DefaultCredentialsProvider.builder()
    .asyncCredentialUpdateEnabled(true)
    .build()
  
  private val MAX_ATTEMPT = 15
  private val MAX_BACKOFF_MILL = 10000L
  override def resolveCredentials(): AwsCredentials = {
    val backoffManager = new FullJitterBackoffManager()
    backoffManager.setMaxMillis(MAX_BACKOFF_MILL)
    
    @tailrec
    def getCredentialsWithRetry(retries: Int): AwsCredentials = {
      Try {
        provider.resolveCredentials()
      } match {
        case Success(credentials) =>
          credentials
        case Failure(_) if retries > 0 =>
          val waitTime = backoffManager.calculateFullJitterBackoff(MAX_ATTEMPT - retries + 1)
          logWarning(s"getCredentialsWithRetry: sleep ${waitTime} millis, retry ${MAX_ATTEMPT - retries + 1}")
          backoffManager.sleep(waitTime)
          
          getCredentialsWithRetry(retries - 1) // Recursive call to retry
        case Failure(exception) =>
          logWarning(s"getCredentialsWithRetry failed after retrying ${MAX_ATTEMPT} times")
          throw exception
      }
    }

    getCredentialsWithRetry(MAX_ATTEMPT)
  }

  override def close(): Unit = {
    provider.close()
  }
}

case class ConnectorDefaultCredentialsProvider() extends ConnectorAwsCredentialsProvider {

  private var providerOpt: Option[RetryableDefaultCredentialsProvider] = None
  override def provider: AwsCredentialsProvider = {
    if (providerOpt.isEmpty) {
      providerOpt = Some(RetryableDefaultCredentialsProvider())
    }
    providerOpt.get
  }

  override def close(): Unit = {
    tryAndIgnoreError("close default credential provider")  {
      providerOpt.foreach(_.close())
    }
  }
}

case class ConnectorSTSCredentialsProvider(
                           stsRoleArn: String,
                           stsSessionName: String,
                           region: String,
                           credentialsProvider: ConnectorDefaultCredentialsProvider,
                           stsEndpoint: Option[String] = None,
                                          )
  extends ConnectorAwsCredentialsProvider  {

  private var providerOpt: Option[StsAssumeRoleCredentialsProvider] = None
  private var stsClientOpt: Option[StsClient] = None
  def provider: AwsCredentialsProvider = {
    if (providerOpt.isEmpty) {
      val stsClientBuilder = StsClient.builder
        .credentialsProvider(credentialsProvider.provider)
        .httpClientBuilder(ApacheHttpClient.builder())

      stsClientOpt = Some(stsEndpoint
        .map(endpoint =>
          stsClientBuilder.endpointOverride(new URI(endpoint))
        )
        .getOrElse(
          stsClientBuilder.region(Region.of(region))
        )
        .build())

      val assumeRoleRequest = AssumeRoleRequest.builder
        .roleArn(stsRoleArn)
        .roleSessionName(stsSessionName)
        .build

      val stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder
        .stsClient(stsClientOpt.get)
        .refreshRequest(assumeRoleRequest)
        .asyncCredentialUpdateEnabled(true)
        .build

      providerOpt = Some(stsAssumeRoleCredentialsProvider)
    }

    providerOpt.get
  }

  override def close(): Unit = {
    tryAndIgnoreError("close sts client") { stsClientOpt.foreach(_.close())}
    tryAndIgnoreError("close sts provider") { providerOpt.foreach(_.close()) }
  }
}

class Builder {
  private var stsRoleArn: Option[String] = None
  private var stsSessionName: Option[String] = None
  private var stsRegion: Option[String] = None
  private var stsEndpoint: Option[String] = None
  
  private var awsAccessKeyIdOpt: Option[String] = None
  private var awsSecretKeyOpt: Option[String] = None
  private var sessionTokenOpt: Option[String] = None
  def stsCredentials(roleArn: Option[String],
                     sessionName: Option[String],
                     region: String,
                     endpoint: Option[String] = None): Builder = {
    stsRoleArn = roleArn
    stsSessionName = sessionName
    stsRegion = Some(region)
    stsEndpoint = endpoint
    this
  }

  def staticCredentials(
    awsAccessKeyId: Option[String],
    awsSecretKey: Option[String],
    sessionToken: Option[String]
  ): Builder = {
    awsAccessKeyIdOpt = awsAccessKeyId
    awsSecretKeyOpt = awsSecretKey
    sessionTokenOpt = sessionToken
    this
  }
  
  def build(): ConnectorAwsCredentialsProvider = {
    val defaultProvider = ConnectorDefaultCredentialsProvider()

    stsRoleArn.map { _ =>
        ConnectorSTSCredentialsProvider(
          stsRoleArn.get,
          stsSessionName.get,
          stsRegion.get,
          defaultProvider,
          stsEndpoint
        )
      }.getOrElse {
        sessionTokenOpt.map { _ =>
          BasicAwsSessionCredentials(
            awsAccessKeyIdOpt.get,
            awsSecretKeyOpt.get,
            sessionTokenOpt.get
          )
        }.getOrElse {
          awsAccessKeyIdOpt.map { _ =>
            BasicAwsCredentials(
              awsAccessKeyIdOpt.get,
              awsSecretKeyOpt.get
                )
          }.getOrElse(defaultProvider)
        }
      }
  }
}

object ConnectorAwsCredentialsProvider {
  def builder: Builder = new Builder
}

