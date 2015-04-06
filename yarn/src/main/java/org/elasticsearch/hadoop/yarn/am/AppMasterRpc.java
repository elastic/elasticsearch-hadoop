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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.elasticsearch.hadoop.yarn.compat.YarnCompat;

class AppMasterRpc implements AutoCloseable {

    private final YarnConfiguration cfg;
    private AMRMClient<ContainerRequest> client;
    private final NMTokenCache nmTokenCache;

    public AppMasterRpc(Configuration cfg, NMTokenCache nmTokenCache) {
        this.cfg = new YarnConfiguration(cfg);
        this.nmTokenCache = nmTokenCache;
    }

    public void start() {
        if (client != null) {
            return;
        }

        client = AMRMClient.createAMRMClient();
        YarnCompat.setNMTokenCache(client, nmTokenCache);
        client.init(cfg);
        client.start();
    }

    public RegisterApplicationMasterResponse registerAM() {
        try {
            return client.registerApplicationMaster("", 0, "");
        } catch (Exception ex) {
            throw new EsYarnAmException(ex);
        }
    }

    public void failAM() {
        unregisterAM(FinalApplicationStatus.FAILED);
    }

    public void finishAM() {
        unregisterAM(FinalApplicationStatus.SUCCEEDED);
    }

    private void unregisterAM(FinalApplicationStatus status) {
        try {
            client.unregisterApplicationMaster(status, "", "");
        } catch (Exception ex) {
            throw new EsYarnAmException(ex);
        }
    }

    public void addContainerRequest(ContainerRequest req) {
        client.addContainerRequest(req);
    }

    public AllocateResponse allocate(int step) {
        try {
            return client.allocate(step);
        } catch (Exception ex) {
            throw new EsYarnAmException(ex);
        }
    }

    public Configuration getConfiguration() {
        return cfg;
    }

    public NMTokenCache getNMToCache() {
        return nmTokenCache;
    }

    @Override
    public void close() {
        if (client == null) {
            return;
        }

        client.stop();
        client = null;
    }
}
