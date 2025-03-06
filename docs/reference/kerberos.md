---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/kerberos.html
navigation_title: Kerberos
---
# {{esh-full}} and Kerberos

::::{important}
Kerberos support for Elasticsearch for Apache Hadoop requires {{es}} 6.7 or greater
::::

Securing Hadoop means using Kerberos. {{es}} supports Kerberos as an authentication method. While the use of Kerberos is not required for securing {{es}}, it is a convenient option for those who already deploy Kerberos to secure their Hadoop clusters. This chapter aims to explain the steps needed to set up elasticsearch-hadoop to use Kerberos authentication for {{es}}.

Elasticsearch for Apache Hadoop communicates with {{es}} entirely over HTTP. In order to support Kerberos authentication over HTTP, elasticsearch-hadoop uses the [Simple and Protected GSSAPI Negotiation Mechanism (SPNEGO)](https://tools.ietf.org/html/rfc4178) to negotiate which underlying authentication method to use (in this case, Kerberos) and to transmit the agreed upon credentials to the server. This authentication mechanism is performed using the [HTTP Negotiate](https://tools.ietf.org/html/rfc4559) authentication standard, where a request is sent to the server and a response is received back with a payload that further advances the negotiation. Once the negotiation between the client and server is complete, the request is accepted and a successful response is returned.

Elasticsearch for Apache Hadoop makes use of Hadoop’s user management processes; The Kerberos credentials of the current Hadoop user are used when authenticating to {{es}}. This means that Kerberos authentication in Hadoop must be enabled in order for elasticsearch-hadoop to obtain a user’s Kerberos credentials. In the case of using an integration that does not depend on Hadoop’s runtime, additional steps may be required to ensure that a running process has Kerberos credentials available for authentication. It is recommended that you consult the documentation of each framework that you are using on how to configure security.

## Setting up your environment [kerberos-settings]

::::{note}
This documentation assumes that you have already provisioned a Hadoop cluster with Kerberos authentication enabled (required). The general process of deploying Kerberos and securing Hadoop is beyond the scope of this documentation.
::::


Before starting, you will need to ensure that principals for your users are provisioned in your Kerberos deployment, as well as service principals for each {{es}} node. To enable Kerberos authentication on {{es}}, it must be [configured with a Kerberos realm](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kerberos.md). It is recommended that you familiarize yourself with how to configure {{es}} Kerberos realms so that you can make appropriate adjustments to fit your deployment. You can find more information on how they work in the [Elastic Stack documentation](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kerberos.md).

Additionally, you will need to [ configure the API Key Realm](elasticsearch://reference/elasticsearch/configuration-reference/security-settings.md) in {{es}}. Hadoop and other distributed data processing frameworks only authenticate with Kerberos in the process that launches a job. Once a job has been launched, the worker processes are often cut off from the original Kerberos credentials and need some other form of authentication. Hadoop services often provide mechanisms for obtaining *Delegation Tokens* during job submission. These tokens are then distributed to worker processes which use the tokens to authenticate on behalf of the user running the job. Elasticsearch for Apache Hadoop obtains API Keys in order to provide tokens for worker processes to authenticate with.

### Connector Settings [kerberos-settings-eshadoop]

The following settings are used to configure elasticsearch-hadoop to use Kerberos authentication:

`es.security.authentication` (default `simple`, or `basic` if `es.net.http.auth.user` is set)
:   Required. Similar to most Hadoop integrations, this property signals which method to use in order to authenticate with {{es}}. By default, the value is `simple`, unless `es.net.http.auth.user` is set, in which case it will default to `basic`. The available options for this setting are `simple` for no authentication, `basic` for basic http authentication, `pki` if relying on certificates, and `kerberos` if Kerberos authentication over SPNEGO should be used.

`es.net.spnego.auth.elasticsearch.principal` (default none)
:   Required if `es.security.authentication` is set to `kerberos`. Details the name of the service principal that the Elasticsearch server is running as. This will usually be of the form `HTTP/node.address@REALM`. Since {{es}} is distributed and should be using a service principal per node, you can use the `_HOST` pattern (like so `HTTP/_HOST@REALM`) to have elasticsearch-hadoop substitute the address of the node it is communicating with at runtime. Note that elasticsearch-hadoop will attempt to reverse resolve node IP addresses to hostnames in order to perform this substitution.

`es.net.spnego.auth.mutual` (default false)
:   Optional. The SPNEGO mechanism assumes that authentication may take multiple back and forth request-response cycles for a request to be fully accepted by the server. When a request is finally accepted by the server, the response contains a payload that can be verified to ensure that the server is the principal they say they are. Setting this to `true` instructs elasticsearch-hadoop to perform this mutual authentication, and to fail the response if it detects invalid credentials from the server.



## Kerberos on Hadoop [kerberos-hadoop]

### Requirements [kerberos-hadoop-requirements]

Before using Kerberos authentication to {{es}}, Kerberos authentication must be enabled in Hadoop.

#### Configure elasticsearch-hadoop [kerberos-hadoop-requirements-conf]

Elasticsearch for Apache Hadoop only needs [a few settings](#kerberos-settings-eshadoop) to configure Kerberos authentication. It is best to set these properties in your `core-site.xml` configuration so that they can be obtained across your entire Hadoop deployment, just like you would for turning on security options for services in Hadoop.

```xml
<configuration>
    ...
    <property>
        <name>es.security.authentication</name>
        <value>kerberos</value>
    </property>
    <property>
        <name>es.net.spnego.auth.elasticsearch.principal</name>
        <value>HTTP/_HOST@REALM.NAME.HERE</value>
    </property>
    ...
</configuration>
```




## Kerberos on YARN [kerberos-yarn]

When applications launch on a YARN cluster, they send along all of their application credentials to the Resource Manager process for them to be distributed to the containers. The Resource Manager has the ability to renew any tokens in those credentials that are about to expire and to cancel tokens once a job has completed. The tokens from {{es}} have a default lifespan of 7 days and they are not renewable. It is a best practice to configure YARN so that it is able to cancel those tokens at the end of a run in order to lower the risk of unauthorized use, and to lower the amount of bookkeeping {{es}} must perform to maintain them.

In order to configure YARN to allow it to cancel {{es}} tokens at the end of a run, you must add the elasticsearch-hadoop jar to the Resource Manager’s classpath. You can do that by placing the jar on the Resource Manager’s local filesystem, and setting the path to the jar in the `YARN_USER_CLASSPATH` environment variable. Once the jar is added, the Resource Manager will need to be restarted.

```ini
export YARN_USER_CLASSPATH=/path/to/elasticsearch-hadoop.jar
```

Additionally, the connection information for elasticsearch-hadoop should be present in the Hadoop configuration, preferably the `core-site.xml`. This is because when Resource Manager cancels a token, it does not take the job configuration into account. Without the connection settings in the Hadoop configuration, the Resource Manager will not be able to communicate to {{es}} in order to cancel the token.

Here is a few common security properties that you will need in order for the Resource Manager to contact {{es}} to cancel tokens:

```xml
<configuration>
    ...
    <property>
        <name>es.nodes</name>
        <value>es-master-1,es-master-2,es-master-3</value> <1>
    </property>
    <property>
        <name>es.security.authentication</name> <2>
        <value>kerberos</value>
    </property>
    <property>
        <name>es.net.spnego.auth.elasticsearch.principal</name> <3>
        <value>HTTP/_HOST@REALM</value>
    </property>
    <property>
        <name>es.net.ssl</name> <4>
        <value>true</value>
    </property>
    <property>
        <name>es.net.ssl.keystore.location</name> <5>
        <value>file:///path/to/ssl/keystore</value>
    </property>
    <property>
        <name>es.net.ssl.truststore.location</name> <6>
        <value>file:///path/to/ssl/truststore</value>
    </property>
    <property>
        <name>es.keystore.location</name> <7>
        <value>file:///path/to/es/secure/store</value>
    </property>
    ...
</configuration>
```

1. The addresses of some {{es}} nodes. These can be any nodes (or all of them) as long as they all belong to the same cluster.
2. Authentication must be configured as `kerberos` in the settings.
3. The name of the {{es}} service principal is not required for token cancellation but having the property in the `core-site.xml` is required for some integrations like Spark.
4. SSL should be enabled if you are using a secured {{es}} deployment.
5. Location on the local filesystem to reach the SSL Keystore.
6. Location on the local filesystem to reach the SSL Truststore.
7. Location on the local filesystem to reach the [elasticsearch-hadoop secure store for secure settings](/reference/security.md#keystore).



## Kerberos with Map/Reduce [kerberos-mr]

Before launching your Map/Reduce job, you must add a delegation token for {{es}} to the job’s credential set. The `EsMapReduceUtil` utility class can be used to do this for you. Simply pass your job to it before submitting it to the cluster. Using the local Kerberos credentials, the utility will establish a connection to {{es}}, request an API Key, and stow the key in the job’s credential set for the worker processes to use.

```java
Job job = Job.getInstance(getConf(), "My-Job-Name"); <1>

// Configure Job Here...

EsMapReduceUtil.initCredentials(job); <2>

if (!job.waitForCompletion(true)) { <3>
    return 1;
}
```

1. Creating a new job instance
2. EsMapReduceUtil obtains job delegation tokens for {es}
3. Submit the job to the cluster


You can obtain the job delegation tokens at any time during the configuration of the Job object, as long as your elasticsearch-hadoop specific configurations are set. It’s usually sufficient to do it right before submitting the job. You should only do this once per job since each call will wastefully obtain another API Key.

Additionally, the utility is also compatible with the `mapred` API classes:

```java
JobConf jobConf = new JobConf(getConf()); <1>
jobConf.setJobName("My-Job-Name");

// Configure JobConf Here...

EsMapReduceUtil.initCredentials(jobConf); <2>

JobClient.runJob(jobConf).waitForCompletion(); <3>
```

1. Creating a new job configuration
2. Obtain {{es}} delegation tokens
3. Submit the job to the cluster



## Kerberos with Hive [kerberos-hive]

### Requirements [kerberos-hive-requirements]

::::{important}
Using Kerberos auth on {{es}} is only supported using HiveServer2.
::::


Before using Kerberos authentication to {{es}} in Hive, Kerberos authentication must be enabled for Hadoop. Make sure you have done all the required steps for [configuring your Hadoop cluster](#kerberos-hadoop-requirements) as well as the steps for [configuring your YARN services](#kerberos-yarn) before using Kerberos authentication for {{es}}.

Finally, ensure that Hive Security is enabled.

Since Hive relies on user impersonation in {{es}} it is advised that you familiarise yourself with [{{es}} authentication](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/user-authentication.md) and [authorization](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/user-roles.md).


### Configure user impersonation settings for Hive [kerberos-hive-proxy]

Hive’s security model follows a proxy-based approach. When a client submits a query to a secured Hive server, Hive authenticates the client using Kerberos. Once Hive is sure of the client’s identity, it wraps its own identity with a *proxy user*. The proxy user contains the client’s simple user name, but contains no credentials. Instead, it is expected that all interactions are executed as the Hive principal impersonating the client user. This is why when configuring Hive security, one must specify in the Hadoop configuration which users Hive is allowed to impersonate:

```xml
<property>
    <name>hadoop.proxyuser.hive.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.hive.groups</name>
    <value>*</value>
</property>
```

{{es}} [supports user impersonation](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/submitting-requests-on-behalf-of-other-users.md), but only users from certain realm implementations can be impersonated. Most deployments of Kerberos include other identity management components like [LDAP](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/ldap.md) or [Active Directory](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/active-directory.md). In those cases, you can configure those realms in {{es}} to allow for user impersonation.

If you are only using Kerberos, or you are using a solution for which {{es}} does not support user impersonation, you must mirror your Kerberos principals to either a [native realm](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/native.md) or a [file realm](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/file-based.md) in {{es}}. When mirroring a Kerberos principal to one of these realms, set the new user’s username to just the main part of the principal name, without any realm or host information. For instance, `client@REALM` would just be `client` and `someservice/domain.name@REALM` would just be `someservice`.

You can follow this step by step process for mirroring users:

#### Create End User Roles [kerberos-hive-proxy-user-role]

Create a role for your end users that will be querying Hive. In this example, we will make a simple role for accessing indices that match `hive-index-*`. All our Hive users will end up using this role to read, write, and update indices in {{es}}.

```console
PUT _security/role/hive_user_role <1>
{
  "run_as": [],
  "cluster": ["monitor", "manage_token"], <2>
  "indices": [
      {
        "names": [ "hive-index-*" ],  <3>
        "privileges": [ "read", "write", "manage" ]
      }
  ]
}
```

1. Our example role name is `hive_user_role`.
2. User should be able to query basic cluster information and manage tokens.
3. Our user will be able to perform read write and management operations on an index.



#### Create role mapping for Kerberos user principal [kerberos-hive-proxy-user-mapping]

Now that the user role is created, we must map the Kerberos user principals to the role. {{es}} does not know the complete list of principals that are managed by Kerberos. As such, each principal that wishes to connect to {{es}} must be mapped to a list of roles that they will be granted after authentication.

```console
POST /_security/role_mapping/hive_user_1_mapping
{
  "roles": [ "hive_user_role" ], <1>
  "enabled": true,
  "rules": {
    "field" : { "username" : "hive.user.1@REALM" } <2>
  }
}
```

1. We set the roles for this mapping to be our example role `hive_user_role`.
2. When the user principal `hive.user.1@REALM` authenticates, it will be given the permissions from the `hive_user_role`.



#### Mirror the user to the native realm [kerberos-hive-proxy-user-mirror]

::::{note}
You may not have to perform this step if you are deploying [LDAP](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/ldap.md) or [Active Directory](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/active-directory.md) along with Kerberos. {{es}} will perform user impersonation by looking up the user names in those realms as long as the simple names (e.g. hive.user.1) on the Kerberos principals match the user names LDAP or Active Directory exactly.
::::


Mirroring the user to the native realm will allow {{es}} to accept authentication requests from the original principal as well as accept requests from Hive which is impersonating the user. You can create a user in the native realm like so:

```console
PUT /_security/user/hive.user.1 <1>
{
  "enabled" : true,
  "password" : "swordfish", <2>
  "roles" : [ "hive_user_role" ], <3>
  "metadata" : {
    "principal" : "hive.user.1@REALM" <4>
  }
}
```

1. The user name is `hive.user.1`, which is the simple name format of the `hive.user.1@REALM` principal we are mirroring.
2. Provide a password here for the user. This should ideally be a securely generated random password since this mirrored user is just for impersonation purposes.
3. Setting the user’s roles to be the example role `hive_user_role`.
4. This is not required, but setting the original principal on the user as metadata may be helpful for your own bookkeeping.



#### Create a role to impersonate Hive users [kerberos-hive-proxy-service-role]

Once you have configured {{es}} with a role mapping for your Kerberos principals and native users for impersonation, you must create a role that Hive will use to impersonate those users.

```console
PUT _security/role/hive_proxier
{
  "run_as": ["hive.user.1"] <1>
}
```

1. Hive’s proxy role should be limited to only run as the users who will be using Hive.



#### Create role mapping for Hive’s service principal [kerberos-hive-proxy-service-mapping]

Now that there are users to impersonate, and a role that can impersonate them, make sure to map the Hive principal to the proxier role, as well as any of the roles that the users it is impersonating would have. This allows the Hive principal to create and read indices, documents, or do anything else its impersonated users might be able to do. While Hive is impersonating the user, it must have these roles or else it will not be able to fully impersonate that user.

```console
POST /_security/role_mapping/hive_hiveserver2_mapping
{
  "roles": [
    "hive_user_role", <1>
    "hive_proxier" <2>
  ],
  "enabled": true,
  "rules": {
    "field" : { "username" : "hive/hiveserver2.address@REALM" } <3>
  }
}
```

1. Here we set the roles to be the superset of the roles from the users we want to impersonate. In our example, the `hive_user_role` role is set.
2. The role that allows Hive to impersonate Hive end users.
3. The name of the Hive server principal to match against.


::::{note}
If managing Kerberos role mappings via the API’s is not desired, they can instead be managed in a [role mapping file](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/mapping-users-groups-to-roles.md#mapping-roles-file).
::::




### Running your Hive queries [kerberos-hive-running]

Once all user accounts are configured and all previous steps for enabling Kerberos auth in Hadoop and Hive are complete, there should be no differences in creating Hive queries from before.



## Kerberos with Spark [kerberos-spark]

### Requirements [kerberos-spark-requirements]

Using Kerberos authentication in elasticsearch-hadoop for Spark has the following requirements:

1. Your Spark jobs must be deployed on YARN. Using Kerberos authentication in elasticsearch-hadoop does not support any other Spark cluster deployments (Mesos, Standalone).
2. Your version of Spark must be on or above version 2.1.0. It is this version that Spark added the ability to plug in third-party credential providers to obtain delegation tokens.

Before using Kerberos authentication to {{es}} in Spark, Kerberos authentication must be enabled for Hadoop. Make sure you have done all the required steps for [configuring your Hadoop cluster](#kerberos-hadoop-requirements) as well as the steps for [configuring your YARN services](#kerberos-yarn) before using Kerberos authentication for {{es}}.


### EsServiceCredentialProvider [kerberos-spark-credprovider]

Before Spark submits an application to a YARN cluster, [it loads a number of credential provider implementations](https://spark.apache.org/docs/2.1.0/running-on-yarn.md#running-in-a-secure-cluster) that are used to determine if any additional credentials must be obtained before the application is started. These implementations are loaded using Java’s `ServiceLoader` architecture. Thus, any jar that is on the classpath when the Spark application is submitted can offer implementations to be loaded and used. `EsServiceCredentialProvider` is one such implementation that is loaded whenever elasticsearch-hadoop is on the job’s classpath.

Once loaded, `EsServiceCredentialProvider` determines if Kerberos authentication is enabled for elasticsearch-hadoop. If it is determined that Kerberos authentication is enabled for elasticsearch-hadoop, then the credential provider will automatically obtain delegation tokens from {{es}} and add them to the credentials on the YARN application submission context. Additionally, in the case that the job is a long lived process like a Spark Streaming job, the credential provider is used to update or obtain new delegation tokens when the current tokens approach their expiration time.

The time that Spark’s credential providers are loaded and called depends on the cluster deploy mode when submitting your Spark app. When running in `client` deploy mode, Spark runs the user’s driver code in the local JVM, and launches the YARN application to oversee the processing as needed. The providers are loaded and run whenever the YARN application first comes online. When running in `cluster` deploy mode, Spark launches the YARN application immediately, and the user’s driver code is run from the resulting Application Master in YARN. The providers are loaded and run *immediately*, before any user code is executed.

#### Configuring the credential provider [kerberos-spark-credprovider-conf]

All implementations of the Spark credential providers use settings from only a few places:

1. The entries from the local Hadoop configuration files
2. The entries of the local Spark configuration file
3. The entries that are specified from the command line when the job is initially launched

Settings that are configured from the user code are not used because the provider must run once for all jobs that are submitted for a particular Spark application. User code is not guaranteed to be run before the provider is loaded. To make things more complicated, a credential provider is only given the local Hadoop configuration to determine if they should load delegation tokens.

These limitations mean that the settings to configure elasticsearch-hadoop for Kerberos authentication need to be in specific places:

First, `es.security.authentication` MUST be set in the local Hadoop configuration files as *kerberos*. If it is not set in the Hadoop configurations, then the credential provider will assume that *simple* authentication is to be used, and will not obtain delegation tokens.

Secondly, all general connection settings for elasticsearch-hadoop (like `es.nodes`, `es.ssl.enabled`, etc…​) must be specified either [in the local Hadoop configuration files](#kerberos-yarn), in the local Spark configuration file, or from the command line. If these settings are not available here, then the credential provider will not be able to contact {{es}} in order to obtain the delegation tokens that it requires.

```bash
$> bin/spark-submit \
    --class org.myproject.MyClass \
    --master yarn \
    --deploy-mode cluster \
    --jars path/to/elasticsearch-hadoop.jar \
    --conf 'spark.es.nodes=es-node-1,es-node-2,es-node-3' <1>
    --conf 'spark.es.ssl.enabled=true'
    --conf 'spark.es.net.spnego.auth.elasticsearch.principal=HTTP/_HOST@REALM' <2>
    path/to/jar.jar
```

1. An example of some connection settings specified at submission time
2. Be sure to include the {{es}} service principal.


::::{note}
Specifying this many configurations in the spark-submit command line is a pretty sure fire way to miss important settings. Thus, it is advised to set them in the [cluster wide Hadoop config](#kerberos-yarn).
::::



#### Renewing credentials for streaming jobs [kerberos-spark-credprovider-streaming]

::::{note}
In the event that you are running a streaming job, it is best to use the `cluster` deploy mode to allow YARN to manage running the driver code for the streaming application.
::::


Since streaming jobs are expected to run continuously without stopping, you should configure Spark so that the credential provider can obtain new tokens before the original tokens expire.

Configuring Spark to obtain new tokens is different from [configuring YARN to renew and cancel tokens](#kerberos-yarn). YARN can only renew existing tokens up to their maximum lifetime. Tokens from {{es}} are not renewable. Instead, they have a simple lifetime of 7 days. After those 7 days elapse, the tokens are expired. In order for an ongoing streaming job to continue running without interruption, completely new tokens must be obtained and sent to worker tasks. Spark has facilities for automatically obtaining and distributing completely new tokens once the original token lifetime has ended.

When submitting a Spark application on YARN, users can provide a principal and keytab file to the `spark-submit` command. Spark will log in with these credentials instead of depending on the local Kerberos TGT Cache for the current user. In the event that any delegation tokens are close to expiring, the loaded credential providers are given the chance to obtain new tokens using the given principal and keytab before the current tokens fully expire. Any new tokens are automatically distributed by Spark to the containers on the YARN cluster.

```bash
$> bin/spark-submit \
    --class org.myproject.MyClass \
    --master yarn \ <1>
    --deploy-mode cluster \ <2>
    --jars path/to/elasticsearch-hadoop.jar \
    --principal client@REALM <3>
    --keytab path/to/keytab.kt \ <4>
    path/to/jar.jar
```

1. YARN deployment is required for Kerberos
2. Use cluster deploy mode to allow for the driver to be run in the YARN Application Master
3. Specify the principal to run the job as
4. The path to the keytab that will be used to reauthenticate when credentials expire



#### Disabling the credential provider [kerberos-spark-credprovider-disable]

When elasticsearch-hadoop is on the classpath, `EsServiceCredentialProvider` is ALWAYS loaded by Spark. If Kerberos authentication is enabled for elasticsearch-hadoop in the local Hadoop configuration, then the provider will attempt to load delegation tokens for {{es}} regardless of if they are needed for that particular job.

It is advised that you do not add elasticsearch-hadoop libraries to jobs that are not configured to connect to or interact with {{es}}. This is the easiest way to avoid the confusion of unrelated jobs failing to launch because they cannot connect to {{es}}.

If you find yourself in a place where you cannot easily remove elasticsearch-hadoop from the classpath of jobs that do not need to interact with {{es}}, then you can explicitly disable the credential provider by setting a property at launch time. The property to set is dependent on your version of Spark:

* For Spark 2.3.0 and up: set the `spark.security.credentials.elasticsearch.enabled` property to `false`.
* For Spark 2.1.0-2.3.0: set the `spark.yarn.security.credentials.elasticsearch.enabled` property to `false`. This property is still accepted in Spark 2.3.0+, but is marked as deprecated.




