/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.spark.deploy.yarn.security

import java.security.PrivilegedExceptionAction
import java.util
import java.util.UUID

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.elasticsearch.hadoop.cfg.CompositeSettings
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager
import org.elasticsearch.hadoop.mr.security.EsTokenIdentifier
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.mr.security.TokenUtil
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.rest.RestClient
import org.elasticsearch.hadoop.security.AuthenticationMethod
import org.elasticsearch.hadoop.security.EsToken
import org.elasticsearch.hadoop.security.UserProvider
import org.elasticsearch.spark.cfg.SparkSettingsManager

/**
 * A provider interface in Spark's Yarn library that obtains tokens for an application.
 *
 * When a job is submitted to a YARN cluster, the credential providers are constructed
 * using a service loader. Each provider is queried for if its service requires a token,
 * and if so, Spark requests that it obtains one.
 *
 * In client deployment mode, these tokens are retrieved on the driver when the YARN
 * application is first initialized and started by the main program.
 *
 * In cluster deployment mode, these tokens are retrieved initially on the driver before
 * submitting the application master that runs the main program.
 *
 * If a principal and keytab are provided to a job, a credentials file is created, and
 * a background thread is started on the application master that will obtain new tokens
 * when they get close to expiring. Those tokens are written to an HDFS directory which
 * the worker nodes will regularly poll to get updated tokens. If the job is launched
 * in client mode, the client will also receive updated tokens.
 */
class EsServiceCredentialProvider extends ServiceCredentialProvider {

  private[this] val LOG = LogFactory.getLog(classOf[EsServiceCredentialProvider])

  LOG.info("Loaded EsServiceCredentialProvider")

  /**
   * Name of the service for logging purposes and for the purpose of determining if the
   * service is disabled in the settings using the property
   * spark.security.credentials.[serviceName].enabled
   * @return the service name this provider corresponds to
   */
  override def serviceName: String = "elasticsearch"

  /**
    *  Given a configuration, check to see if tokens would be required.
    *
    * @param hadoopConf the current Hadoop configuration
    * @return true if tokens should be gathered, false if they should not be
    */
  override def credentialsRequired(hadoopConf: Configuration): Boolean = {
    credentialsRequired(null, hadoopConf)
  }

  /**
    *  Given a configuration, check to see if tokens would be required.
    *
    * @param sparkConf the current Spark configuration - used by Cloudera's CDS Spark fork (#1301)
    * @param hadoopConf the current Hadoop configuration
    * @return true if tokens should be gathered, false if they should not be
    */
  def credentialsRequired(sparkConf: SparkConf, hadoopConf: Configuration): Boolean = {
    val settings = if (sparkConf != null) {
      new CompositeSettings(util.Arrays.asList(
        new SparkSettingsManager().load(sparkConf),
        new HadoopSettingsManager().load(hadoopConf)
      ))
    } else {
      HadoopSettingsManager.loadFrom(hadoopConf)
    }
    val isSecurityEnabled = UserGroupInformation.isSecurityEnabled
    val esAuthMethod = settings.getSecurityAuthenticationMethod
    val required = isSecurityEnabled && AuthenticationMethod.KERBEROS.equals(esAuthMethod)
    LOG.info(s"Hadoop Security Enabled = [$isSecurityEnabled]")
    LOG.info(s"ES Auth Method = [$esAuthMethod]")
    LOG.info(s"Are creds required = [$required]")
    required
  }

  /**
   * Obtains api key tokens from Elasticsearch and stashes them in the given credentials object
   * @param hadoopConf Hadoop configuration, picking up all Hadoop specific settings
   * @param sparkConf All settings that exist in Spark
   * @param creds The credentials object that will be shared between all workers
   * @return The expiration time for the token
   */
  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    val settings = new CompositeSettings(util.Arrays.asList(
      new SparkSettingsManager().load(sparkConf),
      new HadoopSettingsManager().load(hadoopConf)
    ))
    InitializationUtils.setUserProviderIfNotSet(settings, classOf[HadoopUserProvider],
      LogFactory.getLog(classOf[EsServiceCredentialProvider]))
    val userProvider = UserProvider.create(settings)
    val client = new RestClient(settings)
    try {
      val user = userProvider.getUser
      val esToken = user.doAs(new PrivilegedExceptionAction[EsToken]() {
        override def run: EsToken = client.createNewApiToken(TokenUtil.KEY_NAME_PREFIX + UUID.randomUUID().toString)
      })
      if (LOG.isInfoEnabled) {
        LOG.info(s"getting token for: Elasticsearch[tokenName=${esToken.getName}, " +
          s"clusterName=${esToken.getClusterName}, user=${user}]")
      }
      val expiration = esToken.getExpirationTime
      val token = EsTokenIdentifier.createTokenFrom(esToken)
      creds.addToken(token.getService, token)
      Some(expiration)
    } finally {
      client.close()
    }
  }
}
