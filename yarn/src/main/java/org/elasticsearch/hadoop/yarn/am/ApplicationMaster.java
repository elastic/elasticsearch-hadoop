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
package org.elasticsearch.hadoop.yarn.am;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.elasticsearch.hadoop.yarn.cfg.Config;
import org.elasticsearch.hadoop.yarn.util.Assert;
import org.elasticsearch.hadoop.yarn.util.PropertiesUtils;
import org.elasticsearch.hadoop.yarn.util.YarnUtils;

import static org.elasticsearch.hadoop.yarn.EsYarnConstants.CFG_PROPS;
import static org.elasticsearch.hadoop.yarn.EsYarnConstants.FS_URI;

public class ApplicationMaster implements AutoCloseable {

    private static final Log log = LogFactory.getLog(ApplicationMaster.class);

    private ApplicationAttemptId appId;
    private final Map<String, String> env;
    private AppMasterRpc rpc;
    private final Configuration cfg;
    private EsCluster cluster;
    private NMTokenCache nmTokenCache;
    private final Config appConfig;

    private RegisterApplicationMasterResponse amResponse;


    ApplicationMaster(Map<String, String> env) {
        this.env = env;
        cfg = new YarnConfiguration();
        if (env.containsKey(FS_URI)) {
            cfg.set(FileSystem.FS_DEFAULT_NAME_KEY, env.get(FS_URI));
        }
        appConfig = new Config(PropertiesUtils.propsFromBase64String(env.get(CFG_PROPS)));
    }

    void run() {
        log.info("Starting ApplicationMaster...");

        if (nmTokenCache == null) {
            nmTokenCache = new NMTokenCache();
        }

        if (rpc == null) {
            rpc = new AppMasterRpc(cfg, nmTokenCache);
            rpc.start();
        }

        // register AM
        appId = YarnUtils.getApplicationAttemptId(env);
        Assert.notNull(appId, "ApplicationAttemptId cannot be found in env %s" + env);
        amResponse = rpc.registerAM();
        updateTokens();
        cluster = new EsCluster(rpc, appConfig, env);
        try {
            cluster.start();
        } finally {
            close();
        }
    }


    private void updateTokens() {
        for (NMToken nmToken : amResponse.getNMTokensFromPreviousAttempts()) {
            nmTokenCache.setToken(nmToken.getNodeId().toString(), nmToken.getToken());
        }
    }

    @Override
    public void close() {
        boolean hasFailed = (cluster == null || cluster.hasFailed());

        try {
            if (cluster != null) {
                cluster.close();
                cluster = null;
            }

        } finally {
            if (amResponse != null) {
                updateTokens();
            }
            if (hasFailed) {
                rpc.failAM();
            }
            else {
                rpc.finishAM();
            }
        }
    }

    public static void main(String[] args) {
        ApplicationMaster am = new ApplicationMaster(System.getenv());
        try {
            am.run();
        } finally {
            am.close();
        }
    }
}