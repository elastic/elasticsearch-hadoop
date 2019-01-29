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

package org.elasticsearch.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.mr.security.TokenUtil;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.security.UserProvider;
import org.elasticsearch.hadoop.util.ClusterInfo;

/**
 * Utility functions for setting up Map Reduce Jobs to read and write from Elasticsearch.
 *
 * @see EsInputFormat For reading from Elasticsearch
 * @see EsOutputFormat For writing to Elasticsearch
 */
public final class EsMapReduceUtil {

    private static final Log LOG = LogFactory.getLog(EsMapReduceUtil.class);

    private EsMapReduceUtil() { /* No instances */ }

    /**
     * Given the settings contained within a job object, retrieve an authentication token from either the currently logged in
     * user or from the Elasticsearch cluster and add it to the job's credential set.
     * @param job for collecting the settings to connect to Elasticsearch, as well as for storing the authentication token
     */
    public static void initCredentials(Job job) {
        Configuration configuration = job.getConfiguration();
        Settings settings = HadoopSettingsManager.loadFrom(configuration);
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, LOG);
        UserProvider userProvider = UserProvider.create(settings);

        if (userProvider.isEsKerberosEnabled()) {
            User user = userProvider.getUser();
            ClusterInfo clusterInfo = settings.getClusterInfoOrNull();
            RestClient bootstrap = new RestClient(settings);
            try {
                // first get ES main action info if it's missing
                if (clusterInfo == null) {
                    clusterInfo = bootstrap.mainInfo();
                }
                // Add the token to the job
                TokenUtil.addTokenForJob(bootstrap, clusterInfo.getClusterName(), user, job);
            } catch (EsHadoopException ex) {
                throw new EsHadoopIllegalArgumentException(String.format("Cannot detect ES version - "
                        + "typically this happens if the network/Elasticsearch cluster is not accessible or when targeting "
                        + "a WAN/Cloud instance without the proper setting '%s'", ConfigurationOptions.ES_NODES_WAN_ONLY), ex);
            } finally {
                bootstrap.close();
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring Elasticsearch credentials since Kerberos Auth is not enabled.");
            }
        }
    }

    /**
     * Given the settings contained within the job conf, retrieve an authentication token from either the currently logged in
     * user or from the Elasticsearch cluster and add it to the job's credential set.
     * @param jobConf containing the settings to connect to Elasticsearch, as well as for storing the authentication token
     */
    public static void initCredentials(JobConf jobConf) {
        Settings settings = HadoopSettingsManager.loadFrom(jobConf);
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, LOG);
        UserProvider userProvider = UserProvider.create(settings);

        if (userProvider.isEsKerberosEnabled()) {
            User user = userProvider.getUser();
            ClusterInfo clusterInfo = settings.getClusterInfoOrNull();
            RestClient bootstrap = new RestClient(settings);
            try {
                // first get ES main action info if it's missing
                if (clusterInfo == null) {
                    clusterInfo = bootstrap.mainInfo();
                }
                // Add the token to the job
                TokenUtil.addTokenForJobConf(bootstrap, clusterInfo.getClusterName(), user, jobConf);
            } catch (EsHadoopException ex) {
                throw new EsHadoopIllegalArgumentException(String.format("Cannot detect ES version - "
                        + "typically this happens if the network/Elasticsearch cluster is not accessible or when targeting "
                        + "a WAN/Cloud instance without the proper setting '%s'", ConfigurationOptions.ES_NODES_WAN_ONLY), ex);
            } finally {
                bootstrap.close();
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring Elasticsearch credentials since Kerberos Auth is not enabled.");
            }
        }
    }
}
