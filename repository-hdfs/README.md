# Hadoop HDFS Snapshot/Restore plugin

`elasticsearch-repository-hdfs` plugin allows Elasticsearch 2.0 to use `hdfs` file-system as a repository for [snapshot/restore](http://www.elasticsearch.org/guide/en/elasticsearch/reference/master/modules-snapshots.html). See [this blog](http://www.elasticsearch.org/blog/introducing-snapshot-restore/) entry for a quick introduction to snapshot/restore.

## Requirements
- Elasticsearch (version *2.0* or higher).
- HDFS accessible file-system (from the Elasticsearch classpath)
- Elasticsearch Java Security Manager *disabled* (due to permission issues with HDFS)

## Flavors
The HDFS snapshot/restore plugin comes in three flavors:

* Default / Hadoop 1.x
The default version contains the plugin jar alongside Hadoop 1.x (stable) dependencies
* Yarn / Hadoop 2.x
The `hadoop2` version contains the plugin jar plus the Hadoop 2.x (Yarn) dependencies.
* Light
The `light` version contains just the plugin jar, without any Hadoop dependencies.

### What version to use?
It depends on whether you have Hadoop installed on your nodes or not. If you do, then we recommend exposing Hadoop to the Elasticsearch classpath (typically through an environment variable such as +ES_CLASSPATH+ - see the Elasticsearch [reference](https://www.elastic.co/guide/en/elasticsearch/reference/2.0/setup-configuration.html) for more info) and using the `light` version.
This guarantees the existing libraries and configuration are being picked up by the plugin.
If you do not have Hadoop installed, then select either the default version (for Hadoop stable/1.x) or, if you are using Hadoop 2, the `hadoop2` version.

## Installation

The HDFS Snapshot/Restore is an Elasticsearch plugin - be sure to familiarize with what these are and how they work by reading the [plugins chapter](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-plugins.html) in the Elasticsearch documentation.

### Disable the Java Security Manager (JSM)

By default, Elasticsearch enforces a Java Security Manager inside its running JVM for security purposes. Unfortunately Hadoop and its HDFS client are quite greedy in terms of the permissions needed, requiring significantly more than Elasticsearch itself.
Thus, one is required to *disable* the JSM, otherwise the plugin will *not* work correctly; this can be done by adding `security.manager.enabled: false` to the `elasticsearch.yml` configuration on _each_ node where the plugin runs.

One can easily check whether the JSM is disabled or not by looking at the logs for this warning:
```
[2015-10-25 23:13:45,478][INFO ][plugin.hadoop.hdfs       ] Loaded Hadoop [1.2.1] libraries from file:/xxx/plugins/repository-hdfs/
[2015-10-25 23:13:45,478][WARN ][plugin.hadoop.hdfs       ] The Java Security Manager is enabled; unfortunately Hadoop is not compatible with it so it needs to be disabled; see the docs for more information...
```

If the warning appears, the JSM is enabled. If it does not (after the message indicating the libraries have been loaded) then everything is fine.

Note that we are [working](https://github.com/elastic/elasticsearch/pull/14108) towards a solution to have Hadoop running without disabling the JSM. The current ETA is to have this addressed in Elasticsearch 2.2. In the meantime unfortunately, this is the only workaround. 
 
### Node restart
_After_ installing the plugin on _every_ Elasticsearch node, be sure to _restart_ it. This applies to _all_ nodes on which the plugins have been installed - without restarting the nodes, the plugin will not function properly.

### Stable version
As with any other plugin, simply run:
`bin/plugin -i elasticsearch/elasticsearch-repository-hdfs/2.2.0-beta2`

When looking for `light` or `hadoop2` artifacts use:
`bin/plugin -i elasticsearch/elasticsearch-repository-hdfs/2.2.0.beta2-<classifier>`

### Development Snapshot
To install the latest snapshot, please install the plugin manually using:
`bin/plugin -u <url-path-to-plugin.zip> -i elasticsearch-repository-hdfs-2.2.0-BUILD-SNAPSHOT`

Or grab the latest nightly build from the [repository](http://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/elasticsearch-repository-hdfs/) again through Maven:

```xml
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-repository-hdfs</artifactId>
  <version>2.2.0.BUILD-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
  <repository>
    <id>sonatype-oss</id>
    <url>http://oss.sonatype.org/content/repositories/snapshots</url>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
```

## Configuration Properties

Once installed, define the configuration for the `hdfs` repository through `elasticsearch.yml` or the [REST API](http://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html):

```
repositories
  hdfs:
    uri: "hdfs://<host>:<port>/"    # optional - Hadoop file-system URI
    path: "some/path"               # required - path with the file-system where data is stored/loaded
    load_defaults: "true"           # optional - whether to load the default Hadoop configuration (default) or not
    conf_location: "extra-cfg.xml"  # optional - Hadoop configuration XML to be loaded (use commas for multi values)
    conf.<key> : "<value>"          # optional - 'inlined' key=value added to the Hadoop configuration
    concurrent_streams: 5           # optional - the number of concurrent streams (defaults to 5)
    compress: "false"               # optional - whether to compress the metadata or not (default)
    chunk_size: "10mb"              # optional - chunk size (disabled by default)
```

NOTE: Be careful when including a paths within the `uri` setting; Some implementations ignore them completely while others consider them. In general, we recommend keeping the `uri` to a minimum and using the `path` element
instead.

## Plugging other file-systems

Any HDFS-compatible file-systems (like Amazon `s3://` or Google `gs://`) can be used as long as the proper Hadoop configuration is passed to the Elasticsearch plugin. In practice, this means making sure the correct Hadoop configuration files (`core-site.xml` and `hdfs-site.xml`) and its jars are available in plugin classpath, just as you would with any other Hadoop client or job.
Otherwise, the plugin will only read the _default_, vanilla configuration of Hadoop and will not be able to recognized the plugged in file-system.

## Feedback / Q&A
We're interested in your feedback! You can find us on the [forum](https://discuss.elastic.co) - please use the Hadoop channel. For more details, see the [community](http://www.elasticsearch.org/community/) page.

## License
This project is released under version 2.0 of the [Apache License](http://www.apache.org/licenses/LICENSE-2.0)

```
Licensed to Elasticsearch under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
```
