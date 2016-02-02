# Elasticsearch YARN - Beta Status

Elasticsearch YARN project allows Elasticsearch to run within a Hadoop YARN cluster, handling the provisioning and life-cycle of the Elasticsearch cluster the command-line interface (CLI).
Note the project is in Beta status.

## Requirements

Elasticsearch YARN requires Hadoop 2.4.x or higher (the latest stable release is recommended).

## Install

Simply download elasticsearch-yarn-<version>.jar in a location of choice and make sure to have Hadoop available in your classpath; double check through the `hadoop version` command:


    > ls
    elasticsearch-yarn-2.2.0.BUILD-SNAPSHOT.jar

    > hadoop version
    Hadoop 2.4.1
    Subversion http://svn.apache.org/repos/asf/hadoop/common -r 1604318
    Compiled by jenkins on 2014-06-21T05:43Z
    Compiled with protoc 2.5.0
    From source with checksum bb7ac0a3c73dc131f4844b873c74b630
    This command was run using /opt/share/hadoop/common/hadoop-common-2.4.1.jar


## Usage

Simply run `hadoop jar elasticsearch-yarn-<version>.jar` to get a list of available actions:

    > hadoop jar elasticsearch-yarn-<version>.jar
    No command specified
    Usage:
        -download-es  : Downloads Elasticsearch.zip
        -install      : Installs/Provisions Elasticsearch-YARN into HDFS
        -install-es   : Installs/Provisions Elasticsearch into HDFS
        -start        : Starts provisioned Elasticsearch in YARN
        -status       : Reports status of Elasticsearch in YARN
        -stop         : Stops Elasticsearch in YARN
        -help         : Prints this help

    Configuration options can be specified _after_ each command; see the documentation for more information.


Each command should be self-explanatory. The typical usage scenario is:

### Download Elasticsearch version needed

This is a _one-time_ action; if you already have Elasticsearch at hand, deploy it under `downloads` folder. To wit:

    > hadoop jar elasticsearch-yarn-<version> -download-es
    Downloading Elasticsearch 2.2.0
    Downloading ......................................................................................DONE

### Provision Elasticsearch into HDFS

Now that we have downloaded Elasticsearch, let us upload it into HDFS so it becomes available to the Hadoop nodes.
This is another _one-time_ action (as long as your HDFS cluster and the target location remain in place):

    > hadoop jar elasticsearch-yarn-<version>.jar -install-es
    Uploaded /opt/es-yarn/downloads/elasticsearch-<version>.zip to HDFS at hdfs://127.0.0.1:50463/apps/elasticsearch/elasticsearch-<version>.zip

This command uploads the `elasticsearch-<version>.zip`  (that we just downloaded) to HDFS (based on the Hadoop configuration detected in the classpath) under `/apps/elasticsearch` folder.

### Provision Elasticsearch-YARN into HDFS

Let us do the same _one-time_ command with the Elasticsearch-YARN jar:

    > hadoop jar elasticsearch-yarn-<version>.jar -install
    Uploaded opt/es-yarn/elasticsearch-yarn-<version>.jar to HDFS at hdfs://127.0.0.1:50463/apps/elasticsearch/elasticsearch-yarn-<version>.jar

### Start Elasticsearch on YARN

Now that we have the necessary artifacts in HDFS, let us start Elasticsearch:

    > hadoop jar elasticsearch-yarn-<version>.jar -start
    Launched Elasticsearch-YARN cluster [application_1415813090693_0001@http://hadoop:8088/proxy/application_1415813090693_0001/] at Wed Nov 12 19:24:53 EET 2014

That's it!

### List Elasticsearch clusters in YARN

There are plenty of tools in Hadoop to check running YARN applications; with Elasticsearch YARN try the `-status` command:

    > hadoop jar elasticsearch-yarn-<version>.jar -status
    Id                                State      Status     Start Time            Finish Time      Tracking URL
    application_1415813090693_0001  RUNNING   UNDEFINED  11/12/14 7:24 PM   N/A           http://hadoop:8088/proxy/application_1415813090693_0001/A

### Stop Elasticsearch clusters in YARN

Simply use the `-stop` command:

    > hadoop jar elasticsearch-yarn-<version>.jar -stop
    Stopped Elasticsearch Cluster with id application_1415813090693_0001

