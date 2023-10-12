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

import java.net.URI

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
sealed trait ConnectorAwsCredentialsProvider extends Serializable {
  def provider: AwsCredentialsProvider
  def close(): Unit
}

case class ConnectorDefaultCredentialsProvider() extends ConnectorAwsCredentialsProvider {

  private var providerOpt: Option[DefaultCredentialsProvider] = None
  override def provider: AwsCredentialsProvider = {
    if (providerOpt.isEmpty) {
      providerOpt = Some(
        // create a new DefaultCredentialsProvider
        DefaultCredentialsProvider.builder().build()
      )
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

  def build(): ConnectorAwsCredentialsProvider = {
    val defaultProvider = ConnectorDefaultCredentialsProvider()

    stsRoleArn
      .map { _ =>
        ConnectorSTSCredentialsProvider(
          stsRoleArn.get,
          stsSessionName.get,
          stsRegion.get,
          defaultProvider,
          stsEndpoint
        )
      }
      .getOrElse(defaultProvider)
  }
}

object ConnectorAwsCredentialsProvider {
  def builder: Builder = new Builder
}

