---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/hive.html
---

# Apache Hive integration [hive]

Hive abstracts Hadoop by abstracting it through SQL-like language, called HiveQL so that users can apply data defining and manipulating operations to it, just like with SQL. In Hive data sets are [defined](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-DDLOperations) through *tables* (that expose type information) in which data can be [loaded](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-DMLOperations), [selected and transformed](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-SQLOperations) through built-in operators or custom/user defined functions (or [UDF](https://cwiki.apache.org/confluence/display/Hive/OperatorsAndFunctions)s).


## Installation [_installation_2]

Make elasticsearch-hadoop jar available in the Hive classpath. Depending on your options, there are various [ways](https://cwiki.apache.org/confluence/display/Hive/HivePlugins#HivePlugins-DeployingjarsforUserDefinedFunctionsandUserDefinedSerDes) to achieve that. Use [ADD](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Cli#LanguageManualCli-HiveResources) command to add files, jars (what we want) or archives to the classpath:

```
ADD JAR /path/elasticsearch-hadoop.jar;
```

::::{note}
the command expects a proper URI that can be found either on the local file-system or remotely. Typically it’s best to use a distributed file-system (like HDFS or Amazon S3) and use that since the script might be executed on various machines.
::::


::::{important}
When using JDBC/ODBC drivers, `ADD JAR` command is not available and will be ignored. Thus it is recommend to make the jar available to the Hive global classpath and indicated below.
::::


As an alternative, one can use the command-line:

```bash
$ bin/hive --auxpath=/path/elasticsearch-hadoop.jar
```

or use the `hive.aux.jars.path` property specified either through the command-line or, if available, through the `hive-site.xml` file, to register additional jars (that accepts an URI as well):

```bash
$ bin/hive -hiveconf hive.aux.jars.path=/path/elasticsearch-hadoop.jar
```

or if the `hive-site.xml` configuration can be modified, one can register additional jars through the `hive.aux.jars.path` option (that accepts an URI as well):

```xml
<property>
  <name>hive.aux.jars.path</name>
  <value>/path/elasticsearch-hadoop.jar</value>
  <description>A comma separated list (with no spaces) of the jar files</description>
</property>
```


## Configuration [hive-configuration]

When using Hive, one can use `TBLPROPERTIES` to specify the [configuration](/reference/configuration.md) properties (as an alternative to Hadoop `Configuration` object) when declaring the external table backed by {{es}}:

```sql
CREATE EXTERNAL TABLE artists (...)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists',
              'es.index.auto.create' = 'false'); <1>
```

1. elasticsearch-hadoop setting



## Mapping [hive-alias]

By default, elasticsearch-hadoop uses the Hive table schema to map the data in {{es}}, using both the field names and types in the process. There are cases however when the names in Hive cannot be used with {{es}} (the field name can contain characters accepted by {{es}} but not by Hive). For such cases, one can use the `es.mapping.names` setting which accepts a comma-separated list of mapped names in the following format: `Hive field name`:`Elasticsearch field name`

To wit:

```sql
CREATE EXTERNAL TABLE artists (...)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'radio/artists',
            'es.mapping.names' = 'date:@timestamp, url:url_123'); <1>
```

1. Hive column `date` mapped in {{es}} to `@timestamp`; Hive column `url` mapped in {{es}} to `url_123`


::::{tip}
Hive is case **insensitive** while {{es}} is not. The loss of information can create invalid queries (as the column in Hive might not match the one in {{es}}). To avoid this, elasticsearch-hadoop will always convert Hive column names to lower-case. This being said, it is recommended to use the default Hive style and use upper-case names only for Hive commands and avoid mixed-case names.
::::


::::{tip}
Hive treats missing values through a special value `NULL` as indicated [here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-HandlingofNULLValues). This means that when running an incorrect query (with incorrect or non-existing field names) the Hive tables will be populated with `NULL` instead of throwing an exception. Make sure to validate your data and keep a close eye on your schema since updates will otherwise go unnotice due to this lenient behavior.
::::



## Writing data to {{es}} [_writing_data_to_es_2]

With elasticsearch-hadoop, {{es}} becomes just an external [table](https://cwiki.apache.org/confluence/display/Hive/LanguageManual`DDL#LanguageManualDDL-CreateTable) in which data can be loaded or read from:

```sql
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'<1>
TBLPROPERTIES('es.resource' = 'radio/artists'); <2>

-- insert data to Elasticsearch from another table called 'source'
INSERT OVERWRITE TABLE artists
    SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture)
                    FROM source s;
```

1. {{es}} Hive `StorageHandler`
2. {{es}} resource (index and type) associated with the given storage


For cases where the id (or other metadata fields like `ttl` or `timestamp`) of the document needs to be specified, one can do so by setting the appropriate [mapping](/reference/configuration.md#cfg-mapping), namely `es.mapping.id`. Following the previous example, to indicate to {{es}} to use the field `id` as the document id, update the `table` properties:

```sql
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    ...)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.mapping.id' = 'id'...);
```


### Writing existing JSON to {{es}} [writing-json-hive]

For cases where the job input data is already in JSON, elasticsearch-hadoop allows direct indexing *without* applying any transformation; the data is taken as is and sent directly to {{es}}. In such cases, one needs to indicate the json input by setting the `es.input.json` parameter. As such, in this case elasticsearch-hadoop expects the output table to contain only one field, who’s content is used as the JSON document. That is, the library will recognize specific *textual* types (such as `string` or `binary`) or simply call (`toString`).

| `Hive type` | Comment |
| --- | --- |
| `binary` | use this when the JSON data is represented as a `byte[]` or similar |
| `string` | use this if the JSON data is represented as a `String` |
| *anything else* | make sure the `toString()` returns the desired JSON document |
| `varchar` | use this as an alternative to Hive `string` |

::::{important}
Make sure the data is properly encoded, in `UTF-8`. The field content is considered the final form of the document sent to {{es}}.
::::


```java
CREATE EXTERNAL TABLE json (data STRING) <1>
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = '...',
              'es.input.json` = 'yes'); <2>
...
```

1. The table declaration only one field of type `STRING`
2. Indicate elasticsearch-hadoop the table content is in JSON format



### Writing to dynamic/multi-resources [_writing_to_dynamicmulti_resources]

One can index the data to a different resource, depending on the *row* being read, by using patterns. Coming back to the aforementioned [media example](/reference/configuration.md#cfg-multi-writes), one could configure it as follows:

```sql
CREATE EXTERNAL TABLE media (
    name    STRING,
    type    STRING,<1>
    year    STRING,
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'my-collection-{type}/doc'); <2>
```

1. Table field used by the resource pattern. Any of the declared fields can be used.
2. Resource pattern using field `type`


For each *row* about to be written, elasticsearch-hadoop will extract the `type` field and use its value to determine the target resource.

The functionality is also available when dealing with raw JSON - in this case, the value will be extracted from the JSON document itself. Assuming the JSON source contains documents with the following structure:

```js
{
    "media_type":"music",<1>
    "title":"Surfing With The Alien",
    "year":"1987"
}
```

1. field within the JSON document that will be used by the pattern


the table declaration can be as follows:

```sql
CREATE EXTERNAL TABLE json (data STRING) <1>
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'my-collection-{media_type}/doc', <2>
              'es.input.json` = 'yes');
```

1. Schema declaration for the table. Since JSON input is used, the schema is simply a holder to the raw data
2. Resource pattern relying on fields *within* the JSON document and *not* on the table schema



## Reading data from {{es}} [_reading_data_from_es]

Reading from {{es}} is strikingly similar:

```sql
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'<1>
TBLPROPERTIES('es.resource' = 'radio/artists', <2>
              'es.query' = '?q=me*');          <3>

-- stream data from Elasticsearch
SELECT * FROM artists;
```

1. same {{es}} Hive `StorageHandler`
2. {{es}} resource
3. {{es}} query



## Type conversion [hive-type-conversion]

::::{important}
If automatic index creation is used, please review [this](/reference/mapping-types.md#auto-mapping-type-loss) section for more information.
::::


Hive provides various [types](https://cwiki.apache.org/confluence/display/Hive/LanguageManual`Types) for defining data and internally uses different implementations depending on the target environment (from JDK native types to binary-optimized ones). {{es}} integrates with all of them, including and Serde2 [lazy](http://hive.apache.org/javadocs/r1.0.1/api/index.md?org/apache/hadoop/hive/serde2/lazy/package-summary.md) and [lazy binary](http://hive.apache.org/javadocs/r1.0.1/api/index.md?org/apache/hadoop/hive/serde2/lazybinary/package-summary.md):

| Hive type | {{es}} type |
| --- | --- |
| `void` | `null` |
| `boolean` | `boolean` |
| `tinyint` | `byte` |
| `smallint` | `short` |
| `int` | `int` |
| `bigint` | `long` |
| `double` | `double` |
| `float` | `float` |
| `string` | `string` |
| `binary` | `binary` |
| `timestamp` | `date` |
| `struct` | `map` |
| `map` | `map` |
| `array` | `array` |
| `union` | not supported (yet) |
| `decimal` | `string` |
| `date` | `date` |
| `varchar` | `string` |
| `char` | `string` |

::::{note}
While {{es}} understands Hive types up to version 2.0, it is backwards compatible with Hive 1.0
::::


It is worth mentioning that rich data types available only in {{es}}, such as [`GeoPoint`](elasticsearch://reference/elasticsearch/mapping-reference/geo-point.md) or [`GeoShape`](elasticsearch://reference/elasticsearch/mapping-reference/geo-shape.md) are supported by converting their structure into the primitives available in the table above. For example, based on its storage a `geo_point` might be returned as a `string` or an `array`.
