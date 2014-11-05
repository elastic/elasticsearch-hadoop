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
package org.elasticsearch.hadoop.yarn.cli;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.elasticsearch.hadoop.yarn.cfg.Config;
import org.elasticsearch.hadoop.yarn.client.ClientRpc;
import org.elasticsearch.hadoop.yarn.client.YarnLauncher;
import org.elasticsearch.hadoop.yarn.util.HttpDownloader;
import org.elasticsearch.hadoop.yarn.util.IOUtils;
import org.elasticsearch.hadoop.yarn.util.PropertiesUtils;

/**
 * Starts the client app, allowing defaults to be overridden.
 */
public class YarnBootstrap extends Configured implements Tool {

    private static String HELP = null;
    private Config cfg;

    public static void main(String[] args) throws Exception {
        int status = -1;
        try {
            status = ToolRunner.run(new YarnBootstrap(), args);
        } catch (Exception ex) {
            System.err.println("Abnormal execution:" + ex.getMessage());
            ex.printStackTrace(System.err);
        }
        System.exit(status);
    }

    private void displayHelp(String message) {
        if (message != null) {
            System.out.println(message);
        }
        if (HELP == null) {
            HELP = IOUtils.readFrom(getClass().getResourceAsStream("help-msg.txt"));
        }
        System.out.println(HELP);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            displayHelp("No command specified");
            return -1;
        }

        String cmd = args[0];

        cfg = new Config(PropertiesUtils.fromCmdLine(args, 1));

        if ("-download-es".equals(cmd)) {
            downloadEs();
        }
        else if ("-install".equals(cmd)) {
            installEsYarn();
        }
        else if ("-install-es".equals(cmd)) {
            installEs();
        }
        else if ("-help".equals(cmd)) {
            displayHelp(null);
        }
        else if ("-start".equals(cmd)) {
            start();
        }
        else if ("-status".equals(cmd)) {
            status();
        }
        else if ("-stop".equals(cmd)) {
            stop();
        }
        else {
            displayHelp("Unknown command specified " + cmd);
            return -1;
        }

        return 0;
    }

    private void downloadEs() {
        if (cfg.downloadedEs().exists()) {
            System.out.println(String.format("Destination file %s already exists; aborting download...", cfg.downloadedEs()));
            return;
        }
        System.out.println(String.format("Downloading Elasticsearch %s", cfg.downloadEsVersion()));
        new HttpDownloader().downloadES(cfg);
    }

    private void installEsYarn() {
        install(cfg.downloadedEsYarn(), cfg.jarHdfsPath(), getConf());
    }

    private void installEs() {
        install(cfg.downloadedEs(), cfg.esZipHdfsPath(), getConf());
    }

    private void install(File src, String dst, Configuration cfg) {
        Path target = new Path(dst);
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs:///"), cfg);
            FileUtil.copy(src, fs, target, false, cfg);
            FileStatus stats = fs.getFileStatus(target);
            System.out.println(String.format("Uploaded %s to HDFS at %s", src.getAbsolutePath(), stats.getPath()));
        } catch (IOException ex) {
            throw new IllegalStateException(String.format("Cannot upload %s in HDFS at %s", src.getAbsolutePath(), dst), ex);
        }
    }

    private void start() {
        ClientRpc client = new ClientRpc(getConf());
        ApplicationId id = null;
        ApplicationReport report = null;

        try {
            YarnLauncher launcher = new YarnLauncher(client, cfg);
            id = launcher.run();
            report = client.getReport(id);
        } finally {
            client.close();
        }

        System.out.println(String.format("Launched Elasticsearch-YARN cluster [%s@%s] at %tc", id, report.getTrackingUrl(), report.getStartTime()));
    }

    private void stop() {
        ClientRpc client = new ClientRpc(getConf());
        client.start();
        try {
            List<ApplicationReport> apps = client.killEsApps();
            for (ApplicationReport report : apps) {
                System.out.println(String.format("Stopped Elasticsearch-YARN cluster with id %s", report.getApplicationId()));
            }
        } finally {
            client.close();
        }
    }

    private void status() {
        ClientRpc client = new ClientRpc(getConf());
        client.start();
        List<ApplicationReport> esApps = null;
        try {
            esApps = client.listEsClusters();
        } finally {
            client.close();
        }
        System.out.println(buildStatusReport(esApps));
    }

    private String buildStatusReport(List<ApplicationReport> esApps) {
        if (esApps.isEmpty()) {
            return String.format("No Elasticsearch YARN clusters found at %s, webapp at %s",
                    getConf().get(YarnConfiguration.RM_ADDRESS), WebAppUtils.getRMWebAppURLWithoutScheme(getConf()));
        }

        StringBuilder sb = new StringBuilder();
        // header
        sb.append("Id\t\t\t");
        sb.append("State\t");
        sb.append("Status\t");
        sb.append("Start Time\t");
        sb.append("Finish Time\t");
        sb.append("Tracking URL\t");
        sb.append("\n");

        DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

        for (ApplicationReport appReport : esApps) {
            sb.append(appReport.getApplicationId());
            sb.append(appReport.getYarnApplicationState());
            sb.append(appReport.getFinalApplicationStatus());
            sb.append(dateFormat.format(new Date(appReport.getStartTime())));
            sb.append(dateFormat.format(new Date(appReport.getFinishTime())));
            sb.append(appReport.getTrackingUrl());
            sb.append("\n");
        }

        return sb.toString();
    }
}