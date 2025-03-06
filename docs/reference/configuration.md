---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
navigation_title: Configuration
---
# {{esh-full}} configuration

elasticsearch-hadoop behavior can be customized through the properties below, typically by setting them on the target job’s Hadoop `Configuration`. However some of them can be specified through other means depending on the library used (see the relevant section).

::::{admonition}
elasticsearch-hadoop uses the same conventions and reasonable defaults as {{es}} so you can try it out without bothering with the configuration. Most of the time, these defaults are just fine for running a production cluster; if you are fine-tunning your cluster or wondering about the effect of certain configuration options, please *do ask* for more information.

::::


::::{note}
All configuration properties start with the `es` prefix. The namespace `es.internal` is reserved by the library for its internal use and should *not* be used by the user at any point.
::::



## Required settings [_required_settings]

`es.resource`
:   {{es}} resource location, where data is read *and* written to. Requires the format `<index>/<type>` (relative to the {{es}} host/port (see [below](#cfg-network)))).

```ini
es.resource = twitter/tweet   # index 'twitter', type 'tweet'
```

`es.resource.read` (defaults to `es.resource`)
:   {{es}} resource used for reading (but not writing) data. Useful when reading and writing data to different {{es}} indices within the *same* job. Typically set automatically (except for the Map/Reduce module which requires manual configuration).

`es.resource.write`(defaults to `es.resource`)
:   {{es}} resource used for writing (but not reading) data. Used typically for *dynamic resource* writes or when writing and reading data to different {{es}} indices within the *same* job. Typically set automatically (except for the Map/Reduce module which requires manual configuration).

Note that specifying [multiple](https://www.elastic.co/guide/en/elasticsearch/guide/current/multi-index-multi-type.html) indices and/or types in the above resource settings are allowed **only** for reading. Specifying multiple indices for writes is only supported through the use of dynamic resource (described [below](#cfg-multi-writes)). Use `_all/types` to search `types` in all indices or `index/` to search all types within `index`. Do note that reading multiple indices/types typically works only when they have the same structure and only with some libraries. Integrations that require a strongly typed mapping (such as a table like Hive or SparkSQL) are likely to fail.


#### Dynamic/multi resource writes [cfg-multi-writes]

For writing, elasticsearch-hadoop allows the target resource to be resolved at runtime by using patterns (by using the `{<field-name>}` format), resolved at runtime based on the data being streamed to {{es}}. That is, one can save documents to a certain `index` or `type` based on one or multiple fields resolved from the document about to be saved.

For example, assuming the following document set (described here in JSON for readability - feel free to translate this into the actual Java objects):

```json
{
    "media_type":"game",
    "title":"Final Fantasy VI",
    "year":"1994"
},
{
    "media_type":"book",
    "title":"Harry Potter",
    "year":"2010"
},
{
    "media_type":"music",
    "title":"Surfing With The Alien",
    "year":"1987"
}
```

to index each of them based on their `media_type` one would use the following pattern:

```ini
# index the documents based on their type
es.resource.write = my-collection/{media_type}
```

which would result in `Final Fantasy VI` indexed under `my-collection/game`, `Harry Potter` under `my-collection/book` and `Surfing With The Alien` under `my-collection/music`. For more information, please refer to the dedicated integration section.

::::{important}
Dynamic resources are supported *only* for writing, for doing multi-index/type reads, use an appropriate [search query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).
::::



#### Formatting dynamic/multi resource writes [cfg-multi-writes-format]

When using dynamic/multi writes, one can also specify a formatting of the value returned by the field. Out of the box, elasticsearch-hadoop provides formatting for date/timestamp fields which is useful for automatically grouping time-based data (such as logs) within a certain time range under the same index. By using the Java [SimpleDataFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.md) syntax, one can format and parse the date in a locale-sensitive manner.

For example assuming the data contains a `@timestamp` field, one can group the documents in *daily* indices using the following configuration:

```ini
# index the documents based on their date
es.resource.write = my-collection/{@timestamp|yyyy.MM.dd} <1>
```

1. `@timestamp` field formatting - in this case `yyyy.MM.dd`


The same configuration property is used (`es.resource.write`) however, through the special `|` characters a formatting pattern is specified. Please refer to the [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.md) javadocs for more information on the syntax supported. In this case `yyyy.MM.dd` translates the date into the year (specified by four digits), month by 2 digits followed by the day by two digits (such as `2015.01.28`).

[Logstash](http://logstash.net/) users will find this *pattern* quite [familiar](http://logstash.net/docs/latest/filters/date).


## Essential settings [_essential_settings]


### Network [cfg-network]

`es.nodes` (default localhost)
:   List of {{es}} nodes to connect to. When using {{es}} remotely, *do* set this option. Note that the list does *not* have to contain *every* node inside the {{es}} cluster; these are discovered automatically by elasticsearch-hadoop by default (see below). Each node can also have its HTTP/REST port specified individually (e.g. `mynode:9600`).

`es.port` (default 9200)
:   Default HTTP/REST port used for connecting to {{es}} - this setting is applied to the nodes in `es.nodes` that do not have any port specified.

::::{note}
Added in 2.2.
::::


`es.nodes.path.prefix` (default empty)
:   Prefix to add to *all* requests made to {{es}}. Useful in environments where the cluster is proxied/routed under a certain path. For example, if the cluster is located at `someaddress:someport/custom/path/prefix`, one would set `es.nodes.path.prefix` to `/custom/path/prefix`.


### Querying [_querying]

`es.query` (default none)
:   Holds the query used for reading data from the specified `es.resource`. By default it is not set/empty, meaning the entire data under the specified index/type is returned. `es.query` can have three forms:

    uri query
    :   using the form `?uri_query`, one can specify a [query string](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search). Notice the leading `?`.

    query dsl
    :   using the form `query_dsl` - note the query dsl needs to start with `{` and end with `}` as mentioned [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html)

    external resource
    :   if none of the two above do match, elasticsearch-hadoop will try to interpret the parameter as a path within the HDFS file-system. If that is not the case, it will try to load the resource from the classpath or, if that fails, from the Hadoop `DistributedCache`. The resource should contain either a `uri query` or a `query dsl`.


To wit, here is an example:

```ini
# uri (or parameter) query
es.query = ?q=costinl

# query dsl
es.query = { "query" : { "term" : { "user" : "costinl" } } }

# external resource
es.query = org/mypackage/myquery.json
```

In other words, `es.query` is flexible enough so that you can use whatever search api you prefer, either inline or by loading it from an external resource.

::::{tip}
We recommend using query dsl externalized in a file, included within the job jar (and thus available on its classpath). This makes it easy to identify, debug and organize your queries. Through-out the documentation we use the uri query to save text and increase readability - real-life queries quickly become unwieldy when used as uris.
::::



### Operation [_operation]

`es.input.json` (default false)
:   Whether the input is already in JSON format or not (the default). Please see the appropriate section of each integration for more details about using JSON directly.

`es.write.operation` (default index)
:   The write operation elasticsearch-hadoop should perform - can be any of:

    `index` (default)
    :   new data is added while existing data (based on its id) is replaced (reindexed).

    `create`
    :   adds new data - if the data already exists (based on its id), an exception is thrown.

    `update`
    :   updates existing data (based on its id). If no data is found, an exception is thrown.

    `upsert`
    :   known as *merge* or insert if the data does not exist, updates if the data exists (based on its id).

    `delete`
    :   deletes existing data (based on its id). If no data is found, an exception is thrown.


::::{note}
Added in 2.1.
::::


`es.output.json` (default false)
:   Whether the output from the connector should be in JSON format or not (the default). When enabled, the documents are returned in raw JSON format (as returned from {{es}}). Please see the appropriate section of each integration for more details about using JSON directly.

::::{note}
Added in 5.0.0.
::::


`es.ingest.pipeline` (default none)
:   The name of an existing {{es}} Ingest pipeline that should be targeted when indexing or creating documents. Only usable when doing `index` and `create` operations; Incompatible with `update` or `upsert` operations.


### Mapping (when writing to {{es}}) [cfg-mapping]

`es.mapping.id` (default none)
:   The document field/property name containing the document id.

`es.mapping.parent` (default none)
:   The document field/property name containing the document parent. To specify a constant, use the `<CONSTANT>` format.

::::{note}
Added in 5.6.0.
::::


`es.mapping.join` (default none)
:   The document field/property name containing the document’s join field. Constants are not accepted. Join fields on a document must contain either the parent relation name as a string, or an object that contains the child relation name and the id of its parent. If a child document is identified when using this setting, the document’s routing is automatically set to the parent id if no other routing is configured in `es.mapping.routing`.

`es.mapping.routing` (default depends on `es.mapping.join`)
:   The document field/property name containing the document routing. To specify a constant, use the `<CONSTANT>` format. If a join field is specified with `es.mapping.join`, then this defaults to the value of the join field’s parent id. If a join field is not specified, then this defaults to none.

`es.mapping.version` (default none)
:   The document field/property name containing the document version. To specify a constant, use the `<CONSTANT>` format.

`es.mapping.version.type` (default depends on `es.mapping.version`)
:   Indicates the [type of versioning](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) used. If `es.mapping.version` is undefined (default), its value is unspecified. If `es.mapping.version` is specified, its value becomes `external`.

::::{warning}
Deprecated in 6.0.0.
::::


`es.mapping.ttl` (default none)
:   The document field/property name containing the document time-to-live. To specify a constant, use the `<CONSTANT>` format. Will not work on Elasticsearch 6.0+ index versions, but support will continue for 5.x index versions and below.

::::{warning}
Deprecated in 6.0.0.
::::


`es.mapping.timestamp` (default none)
:   The document field/property name containing the document timestamp. To specify a constant, use the `<CONSTANT>` format. Will not work on Elasticsearch 6.0+ index versions, but support will continue for 5.x index versions and below.

::::{note}
Added in 2.1.
::::


`es.mapping.include` (default none)
:   Field/property to be included in the document sent to {{es}}. Useful for *extracting* the needed data from entities. The syntax is similar to that of {{es}} [include/exclude](elasticsearch://reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering). Multiple values can be specified by using a comma. By default, no value is specified meaning all properties/fields are included.

::::{important}
The `es.mapping.include` feature is ignored when `es.input.json` is specified. In order to prevent the connector from indexing data that is implicitly excluded, any jobs with these property conflicts will refuse to execute!
::::


::::{note}
Added in 2.1.
::::


`es.mapping.exclude` (default none)
:   Field/property to be excluded in the document sent to {{es}}. Useful for *eliminating* unneeded data from entities. The syntax is similar to that of {{es}} [include/exclude](elasticsearch://reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering). Multiple values can be specified by using a comma. By default, no value is specified meaning no properties/fields are excluded.

::::{important}
The `es.mapping.exclude` feature is ignored when `es.input.json` is specified. In order to prevent the connector from indexing data that is explicitly excluded, any jobs with these property conflicts will refuse to execute!
::::


% TO DO: uncomment example and the following paragraph after
% https://github.com/elastic/docs-builder/issues/431 is addressed

% For example:

% ```ini
% # extracting the id from the field called 'uuid'
% es.mapping.id = uuid
%
% # specifying a parent with id '123'
% es.mapping.parent = <123>
%
% # combine include / exclude for complete control
% # include
% es.mapping.include = u*, foo.*
% # exclude
% es.mapping.exclude = *.description
% ```

% Using the configuration above, each entry will have only its top-level fields, starting with `u` and nested fields under `foo` included in the document with the exception of any nested field named `description`. Additionally the document parent will be `123` while the document id extracted from field `uuid`.


### Field information (when reading from {{es}}) [cfg-field-info]

::::{note}
Added in 2.1.
::::


`es.mapping.date.rich` (default true)
:   Whether to create a *rich* `Date` like object for `Date` fields in {{es}} or returned them as primitives (`String` or `long`). By default this is true. The actual object type is based on the library used; noteable exception being Map/Reduce which provides no built-in `Date` object and as such `LongWritable` and `Text` are returned regardless of this setting.

::::{note}
Added in 2.2.
::::


`es.read.field.include` (default empty)
:   Fields/properties that are parsed and considered when reading the documents from {{es}}. By default empty meaning all fields are considered. Use this property with *caution* as it can have nasty side-effects. Typically used in cases where some documents returned do not fit into an expected mapping.

::::{note}
Added in 2.2.
::::


`es.read.field.exclude` (default empty)
:   Fields/properties that are discarded when reading the documents from {{es}}. By default empty meaning no fields are excluded. Use this property with *caution* as it can have nasty side-effects. Typically used in cases where some documents returned do not fit into an expected mapping.

For example:

```ini
# To exclude field company.income
es.read.field.exclude = company.income
```

::::{note}
Added in 2.2.
::::


`es.read.field.as.array.include` (default empty)
:   Fields/properties that should be considered as arrays/lists. Since {{es}} can map one or multiple values to a field, elasticsearch-hadoop cannot determine from the mapping whether to treat a field on a document as a single value or an array. When encountering multiple values, elasticsearch-hadoop automatically reads the field into the appropriate array/list type for an integration, but in strict mapping scenarios (like Spark SQL) this may lead to problems (an array is encountered when Spark’s Catalyst engine expects a single value). The syntax for this setting is similar to that of {{es}} [include/exclude](elasticsearch://reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering). Multiple values can be specified by using a comma. By default, no value is specified, meaning no fields/properties are treated as arrays.

::::{note}
Not all fields need to specify `es.read.field.as.array` to be treated as an array. Fields of type `nested` are always treated as an array of objects and should not be marked under `es.read.field.as.array.include`.
::::


For example, given the document:

```json
{
  "foo": {
    "bar": [ "abc", "def" ]
  }
}
```

Use the following configuration:

```ini
# mapping foo.bar as an array
es.read.field.as.array.include = foo.bar
```

If your document contains multiple levels of arrays:

```json
{
  "foo": {
    "bar": [[["abc", "def"], ["ghi", "jkl"]], [["mno", "pqr"], ["stu", "vwx"]]]
  }
}
```

Then specify the dimensionality of the array in the configuration:

```ini
# mapping foo.bar as a 3-level/dimensional array
es.read.field.as.array.include = foo.bar:3
```

`es.read.field.as.array.exclude` (default empty)
:   Fields/properties that should not be considered as arrays/lists. Similar to `es.read.field.as.array.include` above. Multiple values can be specified by using a comma. By default, no value is specified meaning no properties/fields are excluded (and since none is included as indicated above), no field is treated as array before-hand. Note that this setting does not affect nested fields as they are always considered to be arrays.


### Metadata (when reading from {{es}}) [_metadata_when_reading_from_es]

`es.read.metadata` (default false)
:   Whether to include the document metadata (such as id and version) in the results or not (default).

`es.read.metadata.field` (default _metadata)
:   The field under which the metadata information is placed. When `es.read.metadata` is set to true, the information is returned as a `Map` under the specified field.

`es.read.metadata.version` (default false)
:   Whether to include the document version in the returned metadata. Applicable only if `es.read.metadata` is enabled.


### Update settings (when writing to {{es}}) [cfg-update]

One using the `update` or `upsert` operation, additional settings (that mirror the [update](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) API) are available:

`es.update.script.inline` (default none)
:   Inline Script used for updating the document.

::::{note}
Added in 6.0.
::::


`es.update.script.file` (default none)
:   Name of file script to use for updating document. File scripts are removed in 6.x, and as such, this property will throw an error if used against a 6.x and above cluster.

::::{note}
Added in 6.0.
::::


`es.update.script.stored` (default none)
:   Identifier of a stored script to use for updating the document.

`es.update.script.lang` (default none)
:   Script language. By default, no value is specified applying the node configuration.

`es.update.script.params` (default none)
:   Script parameters (if any). The document (currently read) field/property who’s value is used. To specify a constant, use the `<CONSTANT>` format. Multiple values can be specified through commas (`,`)

For example:

```ini
# specifying 2 parameters, one extracting the value from field 'number', the other containing the value '123':
es.update.script.params = param1:number,param2:<123>
```

`es.update.script.params.json`
:   Script parameters specified in `raw`, JSON format. The specified value is passed as is, without any further processing or filtering. Typically used for migrating existing update scripts.

For example:

```ini
es.update.script.params.json = {"param1":1, "param2":2}
```

`es.update.retry.on.conflict` (default 0)
:   How many times an update to a document is retried in case of conflict. Useful in concurrent environments.


## Advanced settings [_advanced_settings]


### Index [configuration-options-index]

`es.index.auto.create` (default yes)
:   Whether elasticsearch-hadoop should create an index (if its missing) when writing data to {{es}} or fail.

`es.index.read.missing.as.empty` (default no)
:   Whether elasticsearch-hadoop will allow reading of non existing indices (and return an empty data set) or not (and throw an exception)

`es.field.read.empty.as.null` (default yes)
:   Whether elasticsearch-hadoop will treat empty fields as `null`. This settings is typically not needed (as elasticsearch-hadoop already handles the null case) but is enabled for making it easier to work with text fields that haven’t been sanitized yet.

`es.field.read.validate.presence` (default warn)
:   To help out spot possible mistakes when querying data from Hadoop (which results in incorrect data being returned), elasticsearch-hadoop can perform validation spotting missing fields and potential typos. Possible values are :

    `ignore`
    :   no validation is performed

    `warn`
    :   a warning message is logged in case the validation fails

    `strict`
    :   an exception is thrown, halting the job, if a field is missing


The default (`warn`) will log any typos to the console when the job starts:

```bash
WARN main mr.EsInputFormat - Field(s) [naem, adress] not found
   in the Elasticsearch mapping specified; did you mean [name, location.address]?
```

::::{note}
Added in 6.6.
::::


`es.read.shard.preference` (default none)
:   The value to use for the shard preference of a search operation when executing a scroll query. If left empty, the connector will automatically sense when to use the `_local` shard preference. This is most useful in hot/cold architectures when you need direct control over which nodes search operations are executed on:

```bash
es.read.shard.preference = _only_nodes:abc*
```

::::{important}
Elasticsearch for Apache Hadoop makes no attempt to validate this preference setting before running, and incorrectly configured shard preferences may cause scroll queries to fail in the event that shards cannot be located with the provided preferences.
::::


`es.read.source.filter` (default none)
:   Normally when using an integration that allows specifying some form of schema (such as Hive), the connector will automatically extract the field names from the schema and request only those fields from {{es}} to save on bandwidth. When using an integration that *does not* leverage any data schemas (such as normal MR and Spark), this property allows you to specify a comma delimited string of field names that you would like to return from {{es}}.

::::{important}
If `es.read.source.filter` is set, an exception will be thrown in the case that the connector tries to push down a different source field filtering. In these cases you should clear this property and trust that the connector knows which fields should be returned. This occurs in SparkSQL, and Hive.
::::


```bash
User specified source filters were found [name,timestamp], but the connector is executing in a state where it has provided its own source filtering [name,timestamp,location.address]. Please clear the user specified source fields under the [es.read.source.filter] property to continue. Bailing out...
```

::::{note}
Added in 5.4.
::::


`es.index.read.allow.red.status` (default false)
:   When executing a job that is reading from Elasticsearch, if the resource provided for reads includes an index that has missing shards that are causing the cluster to have a status of `red`, then ES-Hadoop will inform you of this status and fail fast. In situations where the job must continue using the remaining available data that is still reachable, users can enable this property to instruct the connector to ignore shards it could not reach.

::::{warning}
Using `es.index.read.allow.red.status` can lead to jobs running over incomplete datasets. Jobs executed against a red cluster will yield inconsistent results when compared to jobs executed against a fully green or yellow cluster. Use this setting with caution.
::::



### Input [_input]

::::{note}
Added in 5.0.0.
::::


`es.input.max.docs.per.partition`
:   When reading from an {{es}} cluster that supports scroll slicing ({{es}} v5.0.0 and above), this parameter advises the connector on what the maximum number of documents per input partition should be. The connector will sample and estimate the number of documents on each shard to be read and divides each shard into input slices using the value supplied by this property. This property is a suggestion, not a guarantee. The final number of documents per partition is not guaranteed to be below this number, but rather, they will be close to this number. This property is ignored if you are reading from an {{es}} cluster that does not support scroll slicing ({{es}} any version below v5.0.0). By default, this value is unset, and the input partitions are calculated based on the number of shards in the indices being read.


### Network [_network]

`es.nodes.discovery` (default true)
:   Whether to discover the nodes within the {{es}} cluster or only to use the ones given in `es.nodes` for metadata queries. Note that this setting only applies during start-up; afterwards when reading and writing, elasticsearch-hadoop uses the target index shards (and their hosting nodes) unless `es.nodes.client.only` is enabled.

`es.nodes.client.only` (default false)
:   Whether to use {{es}} [client nodes](elasticsearch://reference/elasticsearch/configuration-reference/node-settings.md) (or *load-balancers*). When enabled, elasticsearch-hadoop will route *all* its requests (after nodes discovery, if enabled) through the *client* nodes within the cluster. Note this typically significantly reduces the node parallelism and thus it is disabled by default. Enabling it also disables `es.nodes.data.only` (since a client node is a non-data node).

::::{note}
Added in 2.1.2.
::::


`es.nodes.data.only` (default true)
:   Whether to use {{es}} [data nodes](elasticsearch://reference/elasticsearch/configuration-reference/node-settings.md) only. When enabled, elasticsearch-hadoop will route *all* its requests (after nodes discovery, if enabled) through the *data* nodes within the cluster. The purpose of this configuration setting is to avoid overwhelming non-data nodes as these tend to be "smaller" nodes. This is enabled by default.

::::{note}
Added in 5.0.0.
::::


`es.nodes.ingest.only` (default false)
:   Whether to use {{es}} [ingest nodes](elasticsearch://reference/elasticsearch/configuration-reference/node-settings.md) only. When enabled, elasticsearch-hadoop will route *all* of its requests (after nodes discovery, if enabled) through the *ingest* nodes within the cluster. The purpose of this configuration setting is to avoid incurring the cost of forwarding data meant for a pipeline from non-ingest nodes; Really only useful when writing data to an Ingest Pipeline (see `es.ingest.pipeline` above).

::::{note}
Added in 2.2.
::::


`es.nodes.wan.only` (default false)
:   Whether the connector is used against an {{es}} instance in a cloud/restricted environment over the WAN, such as Amazon Web Services. In this mode, the connector disables discovery and *only* connects through the declared `es.nodes` during all operations, including reads and writes. Note that in this mode, performance is *highly*  affected.

::::{note}
Added in 2.2.
::::


`es.nodes.resolve.hostname` (default depends)
:   Whether the connector should resolve the nodes hostnames to IP addresses or not. By default it is `true` unless `wan` mode is enabled (see above) in which case it will default to false.

::::{note}
Added in 2.2.
::::


`es.http.timeout` (default 1m)
:   Timeout for HTTP/REST connections to {{es}}.

`es.http.retries` (default 3)
:   Number of retries for establishing a (broken) http connection. The retries are applied for each *conversation* with an {{es}} node. Once the retries are depleted, the connection will automatically be re-reouted to the next available {{es}} node (based on the declaration of `es.nodes`, followed by the discovered nodes - if enabled).

`es.scroll.keepalive` (default 10m)
:   The maximum duration of result scrolls between query requests.

`es.scroll.size` (default 1000)
:   Number of results/items/documents returned per scroll request on each executor/worker/task.

::::{note}
Added in 2.2.
::::


`es.scroll.limit` (default -1)
:   Number of *total* results/items returned by each individual scroll. A negative value indicates that all documents that match should be returned. Do note that this applies per scroll which is typically bound to one of the job tasks. Thus the total number of documents returned is `LIMIT * NUMBER_OF_SCROLLS (OR TASKS)`

`es.action.heart.beat.lead` (default 15s)
:   The lead to task timeout before elasticsearch-hadoop informs Hadoop the task is still running to prevent task restart.

::::{note}
Added in 5.3.0.
::::



### Setting HTTP Request Headers [_setting_http_request_headers]

`es.net.http.header.[HEADER-NAME]`
:   By using the `es.net.http.header.` prefix, you can provide HTTP Headers to all requests made to {{es}} from elasticsearch-hadoop. Please note that some standard HTTP Headers are reserved by the connector to ensure correct operation and cannot be set or overridden by the user (`Accept` and `Content-Type` for instance).

For example, here the user is setting the `Max-Forwards` HTTP header:

```ini
es.net.http.header.Max-Forwards = 10
```


### Secure Settings [_secure_settings]

::::{note}
Added in 6.4.
::::


`es.keystore.location`
:   location of the secure settings keystore file (typically a URL, without a prefix it is interpreted as a classpath entry). See [Secure Settings](/reference/security.md#keystore) for more info.

::::{note}
Added in 2.1.
::::



### Basic Authentication [_basic_authentication]

`es.net.http.auth.user`
:   Basic Authentication user name

`es.net.http.auth.pass`
:   [Securable](/reference/security.md#keystore). Basic Authentication password

::::{note}
Added in 2.1.
::::



### SSL [_ssl]

`es.net.ssl` (default false)
:   Enable SSL

`es.net.ssl.keystore.location`
:   key store (if used) location (typically a URL, without a prefix it is interpreted as a classpath entry)

`es.net.ssl.keystore.pass`
:   [Securable](/reference/security.md#keystore). key store password

`es.net.ssl.keystore.type` (default JKS)
:   key store type. PK12 is a common, alternative format

`es.net.ssl.truststore.location`
:   trust store location (typically a URL, without a prefix it is interpreted as a classpath entry)

`es.net.ssl.truststore.pass`
:   [Securable](/reference/security.md#keystore). trust store password

`es.net.ssl.cert.allow.self.signed` (default false)
:   Whether or not to allow self signed certificates

`es.net.ssl.protocol`(default TLS)
:   SSL protocol to be used


### Proxy [_proxy]

`es.net.proxy.http.host`
:   Http proxy host name

`es.net.proxy.http.port`
:   Http proxy port

`es.net.proxy.http.user`
:   Http proxy user name

`es.net.proxy.http.pass`
:   [Securable](/reference/security.md#keystore). Http proxy password

`es.net.proxy.http.use.system.props`(default yes)
:   Whether the use the system Http proxy properties (namely `http.proxyHost` and `http.proxyPort`) or not

::::{note}
Added in 2.2.
::::


`es.net.proxy.https.host`
:   Https proxy host name

::::{note}
Added in 2.2.
::::


`es.net.proxy.https.port`
:   Https proxy port

::::{note}
Added in 2.2.
::::


`es.net.proxy.https.user`
:   Https proxy user name

::::{note}
Added in 2.2.
::::


`es.net.proxy.https.pass`
:   [Securable](/reference/security.md#keystore). Https proxy password

::::{note}
Added in 2.2.
::::


`es.net.proxy.https.use.system.props`(default yes)
:   Whether the use the system Https proxy properties (namely `https.proxyHost` and `https.proxyPort`) or not

`es.net.proxy.socks.host`
:   Http proxy host name

`es.net.proxy.socks.port`
:   Http proxy port

`es.net.proxy.socks.user`
:   Http proxy user name

`es.net.proxy.socks.pass`
:   [Securable](/reference/security.md#keystore). Http proxy password

`es.net.proxy.socks.use.system.props`(default yes)
:   Whether the use the system Socks proxy properties (namely `socksProxyHost` and `socksProxyHost`) or not

::::{note}
elasticsearch-hadoop allows proxy settings to be applied only to its connection using the setting above. Take extra care when there is already a JVM-wide proxy setting (typically through system properties) to avoid unexpected behavior. IMPORTANT: The semantics of these properties are described in the JVM [docs](http://docs.oracle.com/javase/8/docs/api/java/net/doc-files/net-properties.md#Proxies). In some cases, setting up the JVM property `java.net.useSystemProxies` to `true` works better then setting these properties manually.
::::



### Serialization [configuration-serialization]

`es.batch.size.bytes` (default 1mb)
:   Size (in bytes) for batch writes using {{es}} [bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) API. Note the bulk size is allocated *per task* instance. Always multiply by the number of tasks within a Hadoop job to get the total bulk size at runtime hitting {{es}}.

`es.batch.size.entries` (default 1000)
:   Size (in entries) for batch writes using {{es}} [bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) API - (0 disables it). Companion to `es.batch.size.bytes`, once one matches, the batch update is executed. Similar to the size, this setting is *per task* instance; it gets multiplied at runtime by the total number of Hadoop tasks running.

`es.batch.write.refresh` (default true)
:   Whether to invoke an [index refresh](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-refresh) or not after a bulk update has been completed. Note this is called only after the entire write (meaning multiple bulk updates) have been executed.

`es.batch.write.retry.count` (default 3)
:   Number of retries for a given batch in case {{es}} is overloaded and data is rejected. Note that only the rejected data is retried. If there is still data rejected after the retries have been performed, the Hadoop job is cancelled (and fails). A negative value indicates infinite retries; be careful in setting this value as it can have unwanted side effects.

`es.batch.write.retry.wait` (default 10s)
:   Time to wait between batch write retries that are caused by bulk rejections.

`es.ser.reader.value.class` (default *depends on the library used*)
:   Name of the `ValueReader` implementation for converting JSON to objects. This is set by the framework depending on the library (Map/Reduce, Hive, etc…​) used.

`es.ser.writer.value.class` (default *depends on the library used*)
:   Name of the `ValueWriter` implementation for converting objects to JSON. This is set by the framework depending on the library (Map/Reduce, Hive, etc…​) used.

