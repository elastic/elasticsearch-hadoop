---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/mapreduce.html
---

# Map/Reduce integration [mapreduce]

For low-level or performance-sensitive environments, elasticsearch-hadoop provides dedicated `InputFormat` and `OutputFormat` implementations that can read and write data to {{es}}. In Map/Reduce, the `Mapper`s and `Reducer`s are reading and writing `Writable` objects, a Hadoop specific interface optimized for serialization. As such, elasticsearch-hadoop `InputFormat` and `OutputFormat` will return and expect `MapWritable` objects; A map is used for each document being read or written. The map itself can have any type of internal structure as long as its objects are also `Writable` - it can hold nested maps, numbers or strings in their `Writable` representation. Internally elasticsearch-hadoop automatically converts the `Map` of `Writable` to JSON documents and vice-versa so you do not have to deal with the low-level parsing or conversion to and from JSON. Moreover, if the data sent to {{es}} is already in JSON format, it can be streamed in directly without any conversion to `Writable` objects. Read the rest of the chapter to find out more.


## Installation [_installation]

In order to use elasticsearch-hadoop, the [jar](/reference/installation.md) needs to be available to the job class path. At ~`250kB` and without any dependencies, the jar can be either bundled in the job archive, manually or through CLI [Generic Options](http://hadoop.apache.org/docs/r1.2.1/commands_manual.html#Generic`Options) (if your jar implements the [Tool](http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/util/Tool.html) interface), be distributed through Hadoop’s [DistributedCache](http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#DistributedCache) or made available by provisioning the cluster manually.

::::{important}
All the options above affect *only* the code running on the distributed nodes. If your code that launches the Hadoop job refers to elasticsearch-hadoop, make sure to include the JAR in the `HADOOP_CLASSPATH`: `HADOOP_CLASSPATH="<colon-separated-paths-to-your-jars-including-elasticsearch-hadoop>"`
::::


```bash
$ bin/hadoop jar myJar.jar -libjars elasticsearch-hadoop.jar
```


## Configuration [_configuration]

When using elasticsearch-hadoop in a Map/Reduce job, one can use Hadoop’s `Configuration` object to configure elasticsearch-hadoop by setting the various options as properties on the aforementioned object. Typically one would set the {{es}} host and port (assuming it is not running on the default `localhost:9200`), the target index/type and potentially the query, for example:

```java
Configuration conf = new Configuration();
conf.set("es.nodes", "es-server:9200");    <1>
conf.set("es.resource", "radio/artists");  <2>
...
```

1. A node within the {{es}} cluster elasticsearch-hadoop will be connecting to. By default, elasticsearch-hadoop will detect the rest of the nodes in the cluster.
2. The `resource` (index/type) elasticsearch-hadoop will use to read and write data.


Simply use the configuration object when constructing the Hadoop job and you are all set.


## Writing data to {{es}} [_writing_data_to_es]

With elasticsearch-hadoop, Map/Reduce jobs can write data to {{es}} making it searchable through [indexes](docs-content://reference/glossary/index.md#glossary-index). elasticsearch-hadoop supports both (so-called)  [*old*](http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/mapred/package-use.html) and [*new*](http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/mapreduce/package-use.html) Hadoop APIs.

`EsOutputFormat` expects a `Map<Writable, Writable>` representing a *document* value that is converted internally into a JSON document and indexed in {{es}}. Hadoop `OutputFormat` requires implementations to expect a key and a value however, since for {{es}} only the document (that is the value) is necessary, `EsOutputFormat` ignores the key.


### *Old* (`org.apache.hadoop.mapred`) API [_old_org_apache_hadoop_mapred_api]

To write data to ES, use `org.elasticsearch.hadoop.mr.EsOutputFormat` on your job along with the relevant configuration [properties](/reference/configuration.md):

```java
JobConf conf = new JobConf();
conf.setSpeculativeExecution(false);           <1>
conf.set("es.nodes", "es-server:9200");
conf.set("es.resource", "radio/artists");      <2>
conf.setOutputFormat(EsOutputFormat.class);    <3>
conf.setMapOutputValueClass(MapWritable.class);<4>
conf.setMapperClass(MyMapper.class);
...
JobClient.runJob(conf);
```

1. Disable speculative execution
2. Target index/type
3. Dedicated `OutputFormat`
4. Specify the mapper output class (`MapWritable`)


A `Mapper` implementation can use `EsOutputFormat` as follows:

```java
public class MyMapper extends MapReduceBase implements Mapper {
 @Override
 public void map(Object key, Object value, OutputCollector output,
                    Reporter reporter) throws IOException {
   // create the MapWritable object
   MapWritable doc = new MapWritable();
   ...
   // write the result to the output collector
   // one can pass whatever value to the key; EsOutputFormat ignores it
   output.collect(NullWritable.get(), map);
 }}
```

For cases where the id (or other metadata fields like `ttl` or `timestamp`) of the document needs to be specified, one can do so by setting the appropriate [mapping](/reference/configuration.md#cfg-mapping) namely `es.mapping.id`. Thus assuming the documents contain a field called `radioId` which is unique and is suitable for an identifier, one can update the job configuration as follows:

```java
JobConf conf = new JobConf();
conf.set("es.mapping.id", "radioId");
```

At runtime, elasticsearch-hadoop will extract the value from each document and use it accordingly during the bulk call.


### Writing existing JSON to {{es}} [writing-json-old-api]

For cases where the job input data is already in JSON, elasticsearch-hadoop allows direct indexing *without* applying any transformation; the data is taken as is and sent directly to {{es}}. In such cases, one needs to indicate the json input by setting the `es.input.json` parameter. As such, in this case elasticsearch-hadoop expects either a `Text` or `BytesWritable` (preferred as it requires no `String` conversion) object as output; if these types are not used, the library will simply fall back to the `toString` representation of the target object.

| `Writable` | Comment |
| --- | --- |
| `BytesWritable` | use this when the JSON data is represented as a `byte[]` or similar |
| `Text` | use this if the JSON data is represented as a `String` |
| *anything else* | make sure the `toString()` returns the desired JSON document |

::::{important}
Make sure the data is properly encoded, in `UTF-8`. The job output is considered the final form of the document sent to {{es}}.
::::


```java
JobConf conf = new JobConf();
conf.set("es.input.json", "yes");        <1>
conf.setMapOutputValueClass(Text.class); <2>
...
JobClient.runJob(conf);
```

1. Indicate the input for `EsOutputFormat` is of type JSON.
2. Set the proper output type (`Text` in this case)


The `Mapper` implementation becomes:

```java
public class MyMapper extends MapReduceBase implements Mapper {
 @Override
 public void map(Object key, Object value, OutputCollector output,
                    Reporter reporter) throws IOException {
   // assuming the document is a String called 'source'
   String source =  ...
   Text jsonDoc = new Text(source);
   // send the doc directly
   output.collect(NullWritable.get(), jsonDoc);
 }}
```


### Writing to dynamic/multi-resources [writing-dyn-index-old-api]

For cases when the data being written to {{es}} needs to be indexed under different buckets (based on the data content) one can use the `es.resource.write` field which accepts pattern that are resolved from the document content, at runtime. Following the aforementioned [media example](/reference/configuration.md#cfg-multi-writes), one could configure it as follows:

```java
JobConf conf = new JobConf();
conf.set("es.resource.write","my-collection-{media-type}/doc");
```

If `Writable` objects are used, for each `MapWritable` elasticsearch-hadoop will extract the value under `media-type` key and use that as the {{es}} index suffix. If raw JSON is used, then elasticsearch-hadoop will parse the document, extract the field `media-type` and use its value accordingly.


### *New* (`org.apache.hadoop.mapreduce`) API [_new_org_apache_hadoop_mapreduce_api]

Using the *new* is strikingly similar - in fact, the exact same class (`org.elasticsearch.hadoop.mr.EsOutputFormat`) is used:

```java
Configuration conf = new Configuration();
conf.setBoolean("mapred.map.tasks.speculative.execution", false);    <1>
conf.setBoolean("mapred.reduce.tasks.speculative.execution", false); <2>
conf.set("es.nodes", "es-server:9200");
conf.set("es.resource", "radio/artists");                            <3>
Job job = new Job(conf);
job.setOutputFormatClass(EsOutputFormat.class);
job.setMapOutputValueClass(MapWritable.class);                       <4>
...
job.waitForCompletion(true);
```

1. Disable mapper speculative execution
2. Disable reducer speculative execution
3. Target index/type
4. Specify `Mapper` value output type (in this case `MapWritable`)


Same goes for the `Mapper` instance :

```java
public class SomeMapper extends Mapper {
 @Override
 protected void map(Object key, Object value, Context context)
        throws IOException, InterruptedException {
   // create the MapWritable object
   MapWritable doc = new MapWritable();
   ...
   context.write(NullWritable.get(), doc);
 }}
```

Specifying the id or other document [metadata](/reference/configuration.md#cfg-mapping) is just as easy:

```java
Configuration conf = new Configuration();
conf.set("es.mapping.id", "radioId");
```


### Writing existing JSON to {{es}} [writing-json-new-api]

As before, when dealing with JSON directly, under the *new* API the configuration looks as follows:

```java
Configuration conf = new Configuration();
conf.set("es.input.json", "yes");                 <1>
Job job = new Job(conf);
job.setMapOutputValueClass(BytesWritable.class); <2>
...
job.waitForCompletion(true);
```

1. Indicate the input for `EsOutputFormat` is of type JSON.
2. Set the output type, in this example `BytesWritable`


```java
public class SomeMapper extends Mapper {
 @Override
 protected void map(Object key, Object value, Context context)
        throws IOException, InterruptedException {
   // assuming the document is stored as bytes
   byte[] source =  ...
   BytesWritable jsonDoc = new BytesWritable(source);
   // send the doc directly
   context.write(NullWritable.get(), jsonDoc);
 }}
```


### Writing to dynamic/multi-resources [writing-dyn-index-new-api]

As expected, the difference between the `old` and `new` API are minimal (to be read non-existing) in this case as well:

```java
Configuration conf = new Configuration();
conf.set("es.resource.write","my-collection-{media-type}/doc");
...
```


## Reading data from {{es}} [mr-reading]

In a similar fashion, to read data from {{es}}, one needs to use `org.elasticsearch.hadoop.mr.EsInputFormat` class. While it can read an entire index, it is much more convenient to use a query - elasticsearch-hadoop will automatically execute the query *in real time* and return back the feed the results back to Hadoop. Since the query is executed against the real data, this acts as a *live* view of the data set.

Just like its counter partner (`EsOutputFormat`), `EsInputFormat` returns a `Map<Writable, Writable>` for each JSON document returned by {{es}}. Since the `InputFormat` requires both a key and a value to be returned, `EsInputFormat` will return the document id (inside {{es}}) as the key (typically ignored) and the document/map as the value.

::::{tip}
If one needs the document structure returned from {{es}} to be preserved, consider using `org.elasticsearch.hadoop.mr.LinkedMapWritable`. The class extends Hadoop’s `MapWritable` (and thus can easily replace it) and preserve insertion order; that is when iterating the map, the entries will be returned in insertion order (as oppose to `MapWritable` which does *not* maintain it). However, due to the way Hadoop works, one needs to specify `LinkedMapWritable` as the job map output value (instead of `MapWritable`).
::::



### *Old* (`org.apache.hadoop.mapred`) API [_old_org_apache_hadoop_mapred_api_2]

Following our example above on radio artists, to get a hold of all the artists that start with *me*, one could use the following snippet:

```java
JobConf conf = new JobConf();
conf.set("es.resource", "radio/artists");       <1>
conf.set("es.query", "?q=me*");                 <2>
conf.setInputFormat(EsInputFormat.class);       <3>
conf.setMapOutputKeyClass(Text.class);          <4>
conf.setMapOutputValueClass(MapWritable.class); <5>

...
JobClient.runJob(conf);
```

1. Target index/type
2. Query
3. Dedicated `InputFormat`
4. `Text` as the key class (containing the document id)
5. `MapWritable` or elasticsearch-hadoop’s `LinkedMapWritable` (to preserve insertion order) as the value class (containing the document structure)


A `Mapper` using `EsInputFormat` might look as follows:

```java
public class MyMapper extends MapReduceBase implements Mapper {
 @Override
 public void map(Object key, Object value, OutputCollector output,
                    Reporter reporter) throws IOException {
   Text docId = (Text) key;
   MapWritable doc = (MapWritable) value;      <1>
   ...
 }}
```

1. `LinkedMapWritable` is type compatible with `MapWritable` so the cast will work for both


::::{note}
Feel free to use Java 5 generics to avoid the cast above. For clarity and readability, the examples in this chapter do not include generics.
::::



### *New* (`org.apache.hadoop.mapreduce`) API [_new_org_apache_hadoop_mapreduce_api_2]

As expected, the `mapreduce` API version is quite similar:

```java
Configuration conf = new Configuration();
conf.set("es.resource", "radio/artists/");            <1>
conf.set("es.query", "?q=me*");                       <2>
Job job = new Job(conf);
job.setInputFormatClass(EsInputFormat.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(MapWritable.class);        <3>
...

job.waitForCompletion(true);
```

1. Target index/type
2. Query
3. `MapWritable` or elasticsearch-hadoop’s `LinkedMapWritable` (to preserve insertion order) as the value class (containing the document structure)


and well as the `Mapper` implementation:

```java
public class SomeMapper extends Mapper {
 @Override
 protected void map(Object key, Object value, Context context)
        throws IOException, InterruptedException {
   Text docId = (Text) key;
   MapWritable doc = (MapWritable) value;             <1>
   ...
 }}
```

1. `LinkedMapWritable` is type compatible with `MapWritable` so the cast will work for both



### Reading from {{es}} in JSON format [mr-reading-as-json]

In the case where the results from {{es}} need to be in JSON format (typically to be sent down the wire to some other system), one can instruct elasticsearch-hadoop to return the data as is. By setting `es.output.json` to `true`, the connector will parse the response from {{es}}, identify the documents and, without converting them, return their content to the user as `Text` objects:

```java
Configuration conf = new Configuration();
conf.set("es.resource", "source/category");
conf.set("es.output.json", "true");
```


### Using different indices for reading and writing [mr-read-write-job]

Sometimes, one needs to read data from one {{es}} resource, process it and then write it back to a different resource inside the *same* job . `es.resource` setting is not enough since it implies the same resource both as a source and destination. In such cases, one should use `es.resource.read` and `es.resource.write` to differentiate between the two resources (the example below uses the *mapreduce* API):

```java
Configuration conf = new Configuration();
conf.set("es.resource.read", "source/category");
conf.set("es.resource.write", "sink/group");
```


## Type conversion [type-conversion-writable]

::::{important}
If automatic index creation is used, please review [this](/reference/mapping-types.md#auto-mapping-type-loss) section for more information.
::::


elasticsearch-hadoop automatically converts Hadoop built-in `Writable` types to {{es}} [field types](elasticsearch://reference/elasticsearch/mapping-reference/field-data-types.md) (and back) as shown in the table below:

| `Writable` | {{es}} type |
| --- | --- |
| `null` | `null` |
| `NullWritable` | `null` |
| `BooleanWritable` | `boolean` |
| `Text` | `string` |
| `ByteWritable` | `byte` |
| `IntWritable` | `int` |
| `VInt` | `int` |
| `LongWritable` | `long` |
| `VLongWritable` | `long` |
| `BytesWritable` | `binary` |
| `DoubleWritable` | `double` |
| `FloatWritable` | `float` |
| `MD5Writable` | `string` |
| `ArrayWritable` | `array` |
| `AbstractMapWritable` | `map` |
| `ShortWritable` | `short` |

It is worth mentioning that rich data types available only in {{es}}, such as [`GeoPoint`](elasticsearch://reference/elasticsearch/mapping-reference/geo-point.md) or [`GeoShape`](elasticsearch://reference/elasticsearch/mapping-reference/geo-shape.md) are supported by converting their structure into the primitives available in the table above. For example, based on its storage a `geo_point` might be returned as a `Text` (basically a `String`) or an `ArrayWritable`.

