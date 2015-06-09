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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.elasticsearch.hadoop.yarn.cfg.Config;
import org.elasticsearch.hadoop.yarn.compat.YarnCompat;
import org.elasticsearch.hadoop.yarn.util.StringUtils;
import org.elasticsearch.hadoop.yarn.util.YarnUtils;

/**
 * logical cluster managing the global lifecycle for its multiple containers.
 */
class EsCluster implements AutoCloseable {

    private static final Log log = LogFactory.getLog(EsCluster.class);

    private final AppMasterRpc amRpc;
    private final NodeMasterRpc nmRpc;
    private final Configuration cfg;
    private final Config appConfig;
    private final Map<String, String> masterEnv;

    private volatile boolean running = false;
    private volatile boolean clusterHasFailed = false;

    private final Set<ContainerId> allocatedContainers = new LinkedHashSet<ContainerId>();
    private final Set<ContainerId> completedContainers = new LinkedHashSet<ContainerId>();

    public EsCluster(final AppMasterRpc rpc, Config appConfig, Map<String, String> masterEnv) {
        this.amRpc = rpc;
        this.cfg = rpc.getConfiguration();
        this.nmRpc = new NodeMasterRpc(cfg, rpc.getNMToCache());
        this.appConfig = appConfig;
        this.masterEnv = masterEnv;
    }

    public void start() {
        running = true;
        nmRpc.start();

        UserGroupInformation.setConfiguration(cfg);

        log.info(String.format("Allocating Elasticsearch cluster with %d nodes", appConfig.containersToAllocate()));

        // register requests
        Resource capability = YarnCompat.resource(cfg, appConfig.containerMem(), appConfig.containerVCores());
        Priority prio = Priority.newInstance(appConfig.amPriority());

        for (int i = 0; i < appConfig.containersToAllocate(); i++) {
            // TODO: Add allocation (host/rack rules) - and disable location constraints
            ContainerRequest req = new ContainerRequest(capability, null, null, prio);
            amRpc.addContainerRequest(req);
        }


        // update status every 5 sec
        final long heartBeatRate = TimeUnit.SECONDS.toMillis(5);

        // start the allocation loop
        // when a new container is allocated, launch it right away

        int responseId = 0;

        try {
            do {
                AllocateResponse alloc = amRpc.allocate(responseId++);
                List<Container> currentlyAllocated = alloc.getAllocatedContainers();
                for (Container container : currentlyAllocated) {
                    launchContainer(container);
                    allocatedContainers.add(container.getId());
                }

                if (currentlyAllocated.size() > 0) {
                    int needed = appConfig.containersToAllocate() - allocatedContainers.size();
                    if (needed > 0) {
                        log.info(String.format("%s containers allocated, %s remaining", allocatedContainers.size(),
                                needed));
                    }
                    else {
                        log.info(String.format("Fully allocated %s containers", allocatedContainers.size()));
                    }
                }

                List<ContainerStatus> completed = alloc.getCompletedContainersStatuses();
                for (ContainerStatus status : completed) {
                    if (!completedContainers.contains(status.getContainerId())) {
                        ContainerId containerId = status.getContainerId();
                        completedContainers.add(containerId);

                        boolean containerSuccesful = false;

                        switch (status.getExitStatus()) {
                        case ContainerExitStatus.SUCCESS:
                            log.info(String.format("Container %s finished succesfully...", containerId));
                            containerSuccesful = true;
                            break;
                        case ContainerExitStatus.ABORTED:
                            log.warn(String.format("Container %s aborted...", containerId));
                            break;
                        case ContainerExitStatus.DISKS_FAILED:
                            log.warn(String.format("Container %s ran out of disk...", containerId));
                            break;
                        case ContainerExitStatus.PREEMPTED:
                            log.warn(String.format("Container %s preempted...", containerId));
                            break;
                        default:
                            log.warn(String.format("Container %s exited with an invalid/unknown exit code...", containerId));
                        }

                        if (!containerSuccesful) {
                            log.warn("Cluster has not completed succesfully...");
                            clusterHasFailed = true;
                            running = false;
                        }
                    }
                }

                if (completedContainers.size() == appConfig.containersToAllocate()) {
                    running = false;
                }

                if (running) {
                    try {
                        Thread.sleep(heartBeatRate);
                    } catch (Exception ex) {
                        throw new EsYarnNmException("Cluster interrupted");
                    }
                }
            } while (running);
        } finally {
            log.info("Cluster has completed running...");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(15));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            close();
        }
    }

    private void launchContainer(Container container) {
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

        ctx.setEnvironment(setupEnv(appConfig));
        ctx.setLocalResources(setupEsZipResource(appConfig));
        ctx.setCommands(setupEsScript(appConfig));

        log.info("About to launch container for command: " + ctx.getCommands());

        // setup container
        Map<String, ByteBuffer> startContainer = nmRpc.startContainer(container, ctx);
        log.info("Started container " + container);
    }

    private Map<String, String> setupEnv(Config appConfig) {
        // standard Hadoop env setup
        Map<String, String> env = YarnUtils.setupEnv(cfg);
        // copy esYarn Config
        //env.put(EsYarnConstants.CFG_PROPS, masterEnv.get(EsYarnConstants.CFG_PROPS));
        // plus expand its vars into the env
        YarnUtils.addToEnv(env, appConfig.envVars());

        return env;
    }


    private Map<String, LocalResource> setupEsZipResource(Config conf) {
        // elasticsearch.zip
        Map<String, LocalResource> resources = new LinkedHashMap<String, LocalResource>();

        LocalResource esZip = Records.newRecord(LocalResource.class);
        String esZipHdfsPath = conf.esZipHdfsPath();
        Path p = new Path(esZipHdfsPath);
        FileStatus fsStat;
        try {
            fsStat = FileSystem.get(cfg).getFileStatus(p);
        } catch (IOException ex) {
            throw new IllegalArgumentException(
                    String.format("Cannot find Elasticsearch zip at [%s]; make sure the artifacts have been properly provisioned and the correct permissions are in place.", esZipHdfsPath), ex);
        }
        // use the normalized path as otherwise YARN chokes down the line
        esZip.setResource(ConverterUtils.getYarnUrlFromPath(fsStat.getPath()));
        esZip.setSize(fsStat.getLen());
        esZip.setTimestamp(fsStat.getModificationTime());
        esZip.setType(LocalResourceType.ARCHIVE);
        esZip.setVisibility(LocalResourceVisibility.PUBLIC);

        resources.put(conf.esZipName(), esZip);
        return resources;
    }

    private List<String> setupEsScript(Config conf) {
        List<String> cmds = new ArrayList<String>();
        // don't use -jar since it overrides the classpath
        cmds.add(YarnCompat.$$(ApplicationConstants.Environment.SHELL));
        // make sure to include the ES.ZIP archive name used in the local resource setup above (since it's the folder where it got unpacked)
        cmds.add(conf.esZipName() + "/" + conf.esScript());
        cmds.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        cmds.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);
        return Collections.singletonList(StringUtils.concatenate(cmds, " "));
    }

    public boolean hasFailed() {
        return clusterHasFailed;
    }

    @Override
    public void close() {
        running = false;
        nmRpc.close();
    }
}