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
package org.elasticsearch.hadoop.yarn.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.elasticsearch.hadoop.yarn.EsYarnConstants;
import org.elasticsearch.hadoop.yarn.am.ApplicationMaster;
import org.elasticsearch.hadoop.yarn.cfg.Config;
import org.elasticsearch.hadoop.yarn.compat.YarnCompat;
import org.elasticsearch.hadoop.yarn.util.PropertiesUtils;
import org.elasticsearch.hadoop.yarn.util.StringUtils;
import org.elasticsearch.hadoop.yarn.util.YarnUtils;

public class YarnLauncher {

    private static final Log log = LogFactory.getLog(YarnLauncher.class);

    private final ClientRpc client;
    private Config clientCfg;

    public YarnLauncher(ClientRpc client, Config cfg) {
        this.client = client;
        this.clientCfg = cfg;
    }

    public ApplicationId run() {
        client.start();

        YarnClientApplication app = client.newApp();
        ApplicationSubmissionContext am = setupAM(app);
        ApplicationId appId = client.submitApp(am);
        return am.getApplicationId();
    }

    private ApplicationSubmissionContext setupAM(YarnClientApplication clientApp) {
        ApplicationSubmissionContext appContext = clientApp.getApplicationSubmissionContext();
        // already happens inside Hadoop but to be consistent
        appContext.setApplicationId(clientApp.getNewApplicationResponse().getApplicationId());
        appContext.setApplicationName(clientCfg.appName());
        appContext.setAMContainerSpec(createContainerContext());
        appContext.setResource(YarnCompat.resource(client.getConfiguration(), clientCfg.amMem(), clientCfg.amVCores()));
        appContext.setPriority(Priority.newInstance(clientCfg.amPriority()));
        appContext.setQueue(clientCfg.amQueue());
        appContext.setApplicationType(clientCfg.appType());
        YarnCompat.setApplicationTags(appContext, clientCfg.appTags());

        return appContext;
    }

    private ContainerLaunchContext createContainerContext() {
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        amContainer.setLocalResources(setupEsYarnJar());
        amContainer.setEnvironment(setupEnv());
        amContainer.setCommands(setupCmd());

        return amContainer;
    }

    private Map<String, LocalResource> setupEsYarnJar() {
        Map<String, LocalResource> resources = new LinkedHashMap<String, LocalResource>();
        LocalResource esYarnJar = Records.newRecord(LocalResource.class);
        Path p = new Path(clientCfg.jarHdfsPath());
        FileStatus fsStat;
        try {
            fsStat = FileSystem.get(client.getConfiguration()).getFileStatus(p);
        } catch (IOException ex) {
            throw new IllegalArgumentException(
                    String.format("Cannot find jar [%s]; make sure the artifacts have been properly provisioned and the correct permissions are in place.", clientCfg.jarHdfsPath()), ex);
        }
        // use the normalized path as otherwise YARN chokes down the line
        esYarnJar.setResource(ConverterUtils.getYarnUrlFromPath(fsStat.getPath()));
        esYarnJar.setSize(fsStat.getLen());
        esYarnJar.setTimestamp(fsStat.getModificationTime());
        esYarnJar.setType(LocalResourceType.FILE);
        esYarnJar.setVisibility(LocalResourceVisibility.PUBLIC);

        resources.put(clientCfg.jarName(), esYarnJar);
        return resources;
    }

    private Map<String, String> setupEnv() {
        Configuration cfg = client.getConfiguration();

        Map<String, String> env = YarnUtils.setupEnv(cfg);
        YarnUtils.addToEnv(env, EsYarnConstants.CFG_PROPS, PropertiesUtils.propsToBase64(clientCfg.asProperties()));
        YarnUtils.addToEnv(env, clientCfg.envVars());

        return env;
    }

    private List<String> setupCmd() {
        List<String> cmds = new ArrayList<String>();
        // don't use -jar since it overrides the classpath
        cmds.add(YarnCompat.$$(ApplicationConstants.Environment.JAVA_HOME) + "/bin/java");
        cmds.add(ApplicationMaster.class.getName());
        cmds.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        cmds.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);
        return Collections.singletonList(StringUtils.concatenate(cmds, " "));
    }
}