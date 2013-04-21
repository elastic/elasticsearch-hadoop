/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.integration.hive;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.elasticsearch.hadoop.unit.util.NTFSLocalFileSystem;
import org.elasticsearch.hadoop.unit.util.TestUtils;

/**
 * Utility starting a local/embedded Hive server for testing purposes.
 * Uses sensible defaults to properly clean between reruns.
 *
 * Additionally it wrangles the Hive internals so it rather executes the jobs locally not within a child JVM (which Hive calls local) or external.
 */
class HiveEmbeddedServer {
    // Implementation note: For some reason when running inside local mode, Hive spawns a child VM which is not just problematic (win or *nix) but it does not copies the classpath nor creates any jars
    // As such, the current implementation tricks Hive into thinking it's not local but at the same time sets up Hadoop to run locally and stops Hive from setting any classpath.

    private HiveServer.HiveServerHandler server;
    private Properties testSettings;
    private HiveConf config;

    public HiveEmbeddedServer(Properties settings) {
        this.testSettings = settings;
    }

    HiveInterface start() throws Exception {

        if (server == null) {
            config = configure();
            server = new HiveServer.HiveServerHandler(config);
        }
        return server;
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
        FileUtils.deleteQuietly(new File("/tmp/hive"));

        HiveConf conf = new HiveConf();

        refreshConfig(conf);

        // work-around for NTFS FS
        if (TestUtils.isWindows()) {
            conf.set("fs.file.impl", NTFSLocalFileSystem.class.getName());
            //conf.set("hadoop.bin.path", getClass().getClassLoader().getResource("hadoop.cmd").getPath());
            System.setProperty("path.separator", ";");
        }
        conf.set("hive.metastore.warehouse.dir", "/tmp/hive/warehouse");
        conf.set("hive.metastore.metadb.dir", "/tmp/hive/metastore_db");
        conf.set("hive.exec.scratchdir", "/tmp/hive");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/tmp/hive/metastore_db;create=true");
        conf.set("hive.metastore.local", "true");
        conf.set("hive.aux.jars.path", "");
        conf.set("hive.added.jars.path", "");
        conf.set("hive.added.files.path", "");
        conf.set("hive.added.archives.path", "");

        // clear mapred.job.tracker - Hadoop defaults to 'local' if not defined. Hive however expects this to be set to 'local' - if it's not, it does a remote execution (i.e. no child JVM)
        Field field = Configuration.class.getDeclaredField("properties");
        field.setAccessible(true);
        Properties props = (Properties) field.get(conf);
        props.remove("mapred.job.tracker");

        // intercept SessionState to clean the threadlocal
        Field tss = SessionState.class.getDeclaredField("tss");
        tss.setAccessible(true);
        tss.set(null, new InterceptingThreadLocal());

        return conf;
    }

    private void refreshConfig(HiveConf conf) {
        //delete all "es" properties
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith("es.")) {
                iter.remove();
            }
        }

        // copy test settings
        Enumeration<?> names = testSettings.propertyNames();

        while (names.hasMoreElements()) {
            String key = names.nextElement().toString();
            String value = testSettings.getProperty(key);
            conf.set(key, value);
        }
    }

    public void refreshConfig() {
        refreshConfig(config);
    }

    List<String> execute(String cmd) throws Exception {
        server.execute(cmd);
        return server.fetchAll();
    }

    void stop() {
        if (server != null) {
            server.clean();
            server.shutdown();
            server = null;
            config = null;
        }
    }
}
