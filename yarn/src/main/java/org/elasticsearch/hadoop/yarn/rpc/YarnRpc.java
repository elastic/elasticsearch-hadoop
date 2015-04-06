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
package org.elasticsearch.hadoop.yarn.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.elasticsearch.hadoop.yarn.EsYarnException;

/**
 * Base class handling the creation of various RPC protocols in YARN.
 *
 * While for Client protocol there's YarnClient, Application and Container protocols
 * do not have such an utility hence the reason for this class to handle the creation
 * and disposable in a sane way.
 *
 * @param <P> Protocol class
 */
public abstract class YarnRpc<P> implements AutoCloseable {

    private Class<P> protocolType;
    private Configuration cfg;
    private InetSocketAddress endpoint;

    private P proxy;

    public YarnRpc(Class<P> protocolType, Configuration cfg) {
        this.protocolType = protocolType;
        // make a copy to avoid the security credentials spilling to the main configuration
        this.cfg = new YarnConfiguration(cfg);
    }

    public void start() {
        // handle security
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.setConfiguration(cfg);
        }

        try {
            endpoint = resolveEndpoint(cfg);
        } catch (IOException ex) {
            throw new EsYarnException("Cannot resolve endpoint", ex);
        }

        UserGroupInformation ugi = null;
        try {
            ugi = UserGroupInformation.getCurrentUser();
        } catch (IOException ex) {
            throw new EsYarnException("Cannot get current user", ex);
        }

        // create proxy
        proxy = ugi.doAs(new PrivilegedAction<P>() {
            @SuppressWarnings("unchecked")
            @Override
            public P run() {
                return (P) YarnRPC.create(cfg).getProxy(protocolType, endpoint, cfg);
            }
        });

    }

    protected P proxy() {
        return proxy;
    }

    @Override
    public void close() {
        //potentially use RPC.stopProxy(proxy);
        YarnRPC.create(cfg).stopProxy(proxy, cfg);
    }

    protected abstract InetSocketAddress resolveEndpoint(Configuration cfg) throws IOException;
}
