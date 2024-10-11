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
package org.apache.spark.sql.connector.kinesis.it

import java.net.URI

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import org.apache.spark.sql.connector.kinesis.ConnectorAwsCredentialsProvider
import org.apache.spark.sql.connector.kinesis.ConnectorDefaultCredentialsProvider
import org.apache.spark.sql.connector.kinesis.KinesisOptions
import org.apache.spark.sql.connector.kinesis.tryAndIgnoreError

class CustomTestCredentialsProvider extends ConnectorAwsCredentialsProvider  {
  var stsRoleArn: String = ""
  var stsSessionName: String = ""
  var region: String = ""
  var credentialsProvider: ConnectorDefaultCredentialsProvider = ConnectorDefaultCredentialsProvider()
  var stsEndpoint: Option[String] = None

  private var providerOpt: Option[StsAssumeRoleCredentialsProvider] = None
  private var stsClientOpt: Option[StsClient] = None

  override def init(options: KinesisOptions, parameters: Option[String]): Unit = {
    parameters.map(_.split(",", 2).toList) match {
      case Some(parts) if parts.length == 2 =>
        stsRoleArn = parts.head.trim
        stsSessionName = parts(1).trim
      case _ =>
        throw new IllegalArgumentException(s"unknown parameters ${parameters}")
    }

    region = options.kinesisRegion
    stsEndpoint = options.stsEndpointUrl
  }
  override def provider: AwsCredentialsProvider = {
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

