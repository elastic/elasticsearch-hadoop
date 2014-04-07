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
package org.elasticsearch.hadoop.integration.hive;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.service.HiveServer;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.mr.NTFSLocalFileSystem;
import org.elasticsearch.hadoop.util.TestUtils;

/**
 * Utility starting a local/embedded Hive server for testing purposes.
 * Uses sensible defaults to properly clean between reruns.
 *
 * Additionally it wrangles the Hive internals so it rather executes the jobs locally not within a child JVM (which Hive calls local) or external.
 */
class HiveEmbeddedServer implements HiveInstance {
    // Implementation note: For some reason when running inside local mode, Hive spawns a child VM which is not just problematic (win or *nix) but it does not copies the classpath nor creates any jars
    // As such, the current implementation tricks Hive into thinking it's not local but at the same time sets up Hadoop to run locally and stops Hive from setting any classpath.

    private static Log log = LogFactory.getLog(HiveEmbeddedServer.class);

    private HiveServer.HiveServerHandler server;
    private Properties testSettings;
    private HiveConf config;

    public HiveEmbeddedServer(Properties settings) {
        this.testSettings = settings;
    }

    public void start() throws Exception {
        log.info("Starting Hive Local/Embedded Server...");
        if (server == null) {
            config = configure();
            server = new HiveServer.HiveServerHandler(config);
        }
    }

    // Hive adds automatically the Hive builtin jars - this thread-local cleans that up
    private static class InterceptingThreadLocal extends InheritableThreadLocal<SessionState> {
        @Override
        public void set(SessionState value) {
            value.delete_resource(ResourceType.JAR);
            super.set(value);
        }
    }

    private HiveConf configure() throws Exception {
        TestUtils.delete(new File("/tmp/hive"));

        //HiveConf conf = new HiveConf(DriverContext.class);
        HiveConf conf = new HiveConf();
        refreshConfig(conf);

        HdpBootstrap.hackHadoopStagingOnWin();

        // work-around for NTFS FS
        // set permissive permissions since otherwise, on some OS it fails
        if (TestUtils.isWindows()) {
            conf.set("fs.file.impl", NTFSLocalFileSystem.class.getName());
            conf.set("hive.scratch.dir.permission", "650");
            //conf.set("hadoop.bin.path", getClass().getClassLoader().getResource("hadoop.cmd").getPath());
            System.setProperty("path.separator", ";");
        }
        else {
            conf.set("hive.scratch.dir.permission", "777");
        }

        int random = new Random().nextInt();

        conf.set("hive.metastore.warehouse.dir", "/tmp/hive/warehouse" + random);
        conf.set("hive.metastore.metadb.dir", "/tmp/hive/metastore_db" + random);
        conf.set("hive.exec.scratchdir", "/tmp/hive");
        //conf.set("fs.permissions.umask-mode", "022");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/tmp/hive/metastore_db" + random + ";create=true");
        conf.set("hive.metastore.local", "true");
        conf.set("hive.aux.jars.path", "");
        conf.set("hive.added.jars.path", "");
        conf.set("hive.added.files.path", "");
        conf.set("hive.added.archives.path", "");
        conf.set("fs.default.name", "file:///");

        // clear mapred.job.tracker - Hadoop defaults to 'local' if not defined. Hive however expects this to be set to 'local' - if it's not, it does a remote execution (i.e. no child JVM)
        Field field = Configuration.class.getDeclaredField("properties");
        field.setAccessible(true);
        Properties props = (Properties) field.get(conf);
        props.remove("mapred.job.tracker");
        props.remove("mapreduce.framework.name");
        props.setProperty("fs.default.name", "file:///");

        // intercept SessionState to clean the threadlocal
        Field tss = SessionState.class.getDeclaredField("tss");
        tss.setAccessible(true);
        tss.set(null, new InterceptingThreadLocal());

        return conf;
    }

    private void removeESSettings(HiveConf conf) {
        //delete all "es" properties
        Set<String> props = testSettings.stringPropertyNames();
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            // remove transient settings only to avoid reloading the configuration (which might override some manual settings)
            if (key.startsWith("es.") && !props.contains(key)) {
                // NB: don't use remove since the iterator works on a copy not on the real thing
                conf.unset(key);
            }
        }
    }
    private void refreshConfig(HiveConf conf) {
        removeESSettings(conf);
        // copy test settings
        Enumeration<?> names = testSettings.propertyNames();

        while (names.hasMoreElements()) {
            String key = names.nextElement().toString();
            String value = testSettings.getProperty(key);
            conf.set(key, value);
        }
    }

    public void removeESSettings() {
        removeESSettings(config);

        // clear session state
        //        SessionState sessionState = SessionState.get();
        //        if (sessionState != null) {
        //            cleanConfig(sessionState.getConf());
        //        }
    }

    public List<String> execute(String cmd) throws Exception {
        if (cmd.toUpperCase().startsWith("ADD JAR")) {
            // skip the jar since we're running in local mode
            System.out.println("Skipping ADD JAR in local/embedded mode");
            return Collections.emptyList();
        }
        server.execute(cmd);
        return server.fetchAll();
    }

    public void stop() {
        if (server != null) {
            log.info("Stopping Hive Local/Embedded Server...");
            server.clean();
            server.shutdown();
            server = null;
            config = null;
        }
    }
}