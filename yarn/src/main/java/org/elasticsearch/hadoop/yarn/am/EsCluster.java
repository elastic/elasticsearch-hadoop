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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.elasticsearch.hadoop.yarn.cfg.Config;
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

    private volatile boolean running = false;
    private volatile boolean clusterHasFailed = false;

    private final Set<ContainerId> completedContainers = Collections.synchronizedSet(new LinkedHashSet<ContainerId>());
	private final int numberOfContainers;

	public EsCluster(final AppMasterRpc rpc, int numberOfContainers) {
        this.amRpc = rpc;
        this.cfg = rpc.getConfiguration();
        this.nmRpc = new NodeMasterRpc(cfg, rpc.getNMToCache());
		this.numberOfContainers = numberOfContainers;
    }

	public void start() {
        nmRpc.start();

        log.info("Starting container cluster ..");

        AllocateResponse response = amRpc.allocate(0);
        for (Container container : response.getAllocatedContainers()) {
            // launch container
            launchContainer(container);
        }

        running = true;

        log.info("Started container cluster; monitoring completion...");

        // schedule a heart-beat 15 seconds before timing out (just to be on the safe side)
        //final long heartBeatRate = Math.max(YarnUtils.getAmHeartBeatRate(cfg) - TimeUnit.SECONDS.toMillis(15), TimeUnit.SECONDS.toMillis(5));

        // update status every 5 sec
        final long heartBeatRate = TimeUnit.SECONDS.toMillis(1);

        try {
            do {
                AllocateResponse allocate = amRpc.allocate(0);
                List<ContainerStatus> completed = allocate.getCompletedContainersStatuses();
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

				if (completedContainers.size() == numberOfContainers) {
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

        Configuration yarnConf = amRpc.getConfiguration();
        Config conf = new Config(null);

        ctx.setEnvironment(YarnUtils.setupEnv(yarnConf));
		ctx.setLocalResources(setupEsZipResource(conf));
		ctx.setCommands(setupEsScript(conf));

		log.info("About to launch container for command: " + ctx.getCommands());

		// setup container
		Map<String, ByteBuffer> startContainer = nmRpc.startContainer(container, ctx);
		log.info("Started container " + startContainer);
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
		cmds.add(ApplicationConstants.Environment.SHELL.$$());
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