---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/security.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/hadoop.html
navigation_title: Security
---
# {{esh-full}} security

% What needs to be done: Refine

% - [ ] ./raw-migrated-files/elasticsearch-hadoop/elasticsearch-hadoop/security.md
% - [ ] ./raw-migrated-files/elasticsearch/elasticsearch-reference/hadoop.md

elasticsearch-hadoop can work in secure environments and has support for authentication and authorization. However it is important to understand that elasticsearch-hadoop per-se is a *connector*, that is, it bridges two different systems. So when talking about security, it is important to understand to what system it applies: the connector can run within a secure Hadoop environment talking to a vanilla/non-secured {{es}} cluster. Or vice-versa, it can run within a non-secured Spark environment while talking securely to a {{es}} cluster. Of course, the opposite can happen as well; the connector running within a secure Hadoop environment and communicating with a secured {{es}} cluster or the most common use-case, running from an open Spark environment to a default, non-secured {{es}} install. This enumeration of setups is actually on purpose, to illustrate that based on what piece of the environment is secured, its respective connector configuration needs to be adjusted.

## Secure Hadoop/Spark [_secure_hadoopspark] 

As the connector runs as a *library* within Hadoop or Spark, for the most part it does not require any special configuration as it will *inherit* and *run* using the enclosing job/task credentials. In other words, as long as your Hadoop/Spark job is properly configured to run against the secure environment, elasticsearch-hadoop as library simply runs within that secured context using the already configured credentials. Settings this up is beyond the purpose of this documentation however it typically boils down to setting up the proper credentials on the configuration object used by the job/task.

## Secure {{es}} [_secure_es] 

{{es}} itself can be secured which impacts clients (like elasticsearch-hadoop )on two fronts: transport layer which is now encrypted and access layer which requires authentication. Note that typically it is recommended to enable both options (secure transport and secure access).

### SSL/TLS configuration [_ssltls_configuration] 

In case of an encrypted transport, the SSL/TLS support needs to be enabled in elasticsearch-hadoop in order for the connector to properly communicate with {{es}}. This is done by setting `es.net.ssl` property to `true` and, depending on your SSL configuration (whether the certificates are signed by a CA or not, whether they are global at JVM level or just local to one application), might require setting up the `keystore` and/or `truststore`, that is where the *credentials* are stored (`keystore` - which typically stores private keys and certificates) and how to *verify* them (`truststore` - which typically stores certificates from third party also known as CA - certificate authorities). Typically (and again, do note that your environment might differ significantly), if the SSL setup for elasticsearch-hadoop is not already done at the JVM level, one needs to setup the keystore if the elasticsearch-hadoop security requires client authentication (PKI - Public Key Infrastructure), and setup `truststore` if SSL is enabled.

### Authentication [_authentication] 

The authentication support in elasticsearch-hadoop is of the following types:

Username/Password
:   Set these through `es.net.http.auth.user` and `es.net.http.auth.pass` properties.

API key authentication
:   You can configure [API key-based authentication](docs-content://deploy-manage/api-keys/elasticsearch-api-keys.md) using [custom HTTP request headers](/reference/configuration.md#_setting_http_request_headers).

    To authenticate using an API key, set the `Authorization` header with your Base64-encoded API key (the `encoded` value returned when you [create an API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key)) as follows:

    ```ini
    es.net.http.header.Authorization = ApiKey VnVhQ2ZHY0JDZGJr...
    ```

    Here's an example using PySpark:

    ```python
    es_options = {
        "es.nodes": es_host,
        "es.port": es_port,
        "es.net.ssl": "true",
        "es.nodes.wan.only": "true",
        "es.net.http.header.Authorization": f"ApiKey <es_api_key>", <1>
        "es.resource": es_index,
    }
    ```
    1. Substitute `<es_api_key>` with your API key.

PKI/X.509
:   Use X.509 certificates to authenticate elasticsearch-hadoop to elasticsearch-hadoop. For this, one would need to setup the `keystore` containing the private key and certificate to the appropriate user (configured in {{es}}) and the `truststore` with the CA certificate used to sign the SSL/TLS certificates in the {{es}} cluster. That is one setup the key to authenticate elasticsearch-hadoop and also to verify that is the right one. To do so, one should setup the `es.net.ssl.keystore.location` and `es.net.ssl.truststore.location` properties to indicate the `keystore` and `truststore` to use. It is recommended to have these secured through a password in which case `es.net.ssl.keystore.pass` and `es.net.ssl.truststore.pass` properties are required.

### Secure Settings [#keystore] 

elasticsearch-hadoop is configured using settings that sometimes contain sensitive information such as passwords. It may not be desirable for those property values to appear in the job configuration as plain text. For these situations, elasticsearch-hadoop supports reading some secure properties from a keystore file.

::::{note} 
The elasticsearch-hadoop keystore currently only provides obfuscation. In the future, password protection will be added.
::::

Only the following configurations can be read from the secure settings:

* `es.net.http.auth.pass`
* `es.net.ssl.keystore.pass`
* `es.net.ssl.truststore.pass`
* `es.net.proxy.http.pass`
* `es.net.proxy.https.pass`
* `es.net.proxy.socks.pass`

Provided with elasticsearch-hadoop is a keytool program that will allow you to create and add entries to a compatible keystore file.

```bash
$> java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool <command> <args>
```

To create a keystore file in your working directory, run the `create` command:

```bash
$> java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool create
$> ls
esh.keystore
```

A list of the settings in the keystore is available with the `list` command:

```bash
$> java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool list
```

Once a keystore file has been created, your sensitive settings can be added using the `add` command:

```bash
$> java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool add the.setting.name.to.set
```

A prompt will appear and request the value for the setting. To pass the value through stdin, use the `--stdin` flag:

```bash
$> cat /file/containing/setting/value | java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool add --stdin the.setting.name.to.set
```

To remove a setting from the keystore, use the `remove` command:

```bash
$> java -classpath path/to/eshadoop.jar org.elasticsearch.hadoop.cli.Keytool remove the.setting.name.to.set
```

Once your settings are all specified, you must make sure that the keystore is available on every node. This can be done by placing it on each node’s local file system, or by adding the keystore to the job’s classpath. Once the keystore has been added, its location must be specified with the `es.keystore.location`. To reference a local file, use a fully qualified file URL (for example, `file:///path/to/file`). If the secure store is propagated using the command line, just use the file’s name.
