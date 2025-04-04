---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
---
# Apache Spark support [spark]

Spark provides fast iterative/functional-like capabilities over large data sets, typically by *caching* data in memory. As opposed to the rest of the libraries mentioned in this documentation, Apache Spark is computing framework that is not tied to Map/Reduce itself however it does integrate with Hadoop, mainly to HDFS. elasticsearch-hadoop allows {{es}} to be used in Spark in two ways: through the dedicated support available since 2.1 or through the Map/Reduce bridge since 2.0. Spark 2.0 is supported in elasticsearch-hadoop since version 5.0

## Installation [spark-installation]

Just like other libraries, elasticsearch-hadoop needs to be available in Spark’s classpath. As Spark has multiple deployment modes, this can translate to the target classpath, whether it is on only one node (as is the case with the local mode - which will be used through-out the documentation) or per-node depending on the desired infrastructure.

### Native RDD support [spark-native]

elasticsearch-hadoop provides *native* integration between {{es}} and Apache Spark, in the form of an `RDD` (Resilient Distributed Dataset) (or *Pair* `RDD` to be precise) that can read data from Elasticsearch. The `RDD` is offered in two *flavors*: one for Scala (which returns the data as `Tuple2` with Scala collections) and one for Java (which returns the data as `Tuple2` containing `java.util` collections).

::::{important}
Whenever possible, consider using the *native* integration as it offers the best performance and maximum flexibility.
::::

#### Configuration [spark-native-cfg]

To configure elasticsearch-hadoop for Apache Spark, one can set the various properties described in the [*Configuration*](/reference/configuration.md) chapter in the [`SparkConf`](http://spark.apache.org/docs/1.6.2/programming-guide.md#initializing-spark) object:

```scala
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName(appName).setMaster(master)
conf.set("es.index.auto.create", "true")
```

```java
SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
conf.set("es.index.auto.create", "true");
```

For those that want to set the properties through the command-line (either directly or by loading them from a file), note that Spark *only* accepts those that start with the "spark." prefix and will *ignore* the rest (and depending on the version a warning might be thrown). To work around this limitation, define the elasticsearch-hadoop properties by appending the `spark.` prefix (thus they become `spark.es.`) and elasticsearch-hadoop will automatically resolve them:

```bash
$ ./bin/spark-submit --conf spark.es.resource=index/type ... <1>
```

1. Notice the `es.resource` property which became `spark.es.resource`

#### Writing data to {{es}} [spark-write]

With elasticsearch-hadoop, any `RDD` can be saved to {{es}} as long as its content can be translated into documents. In practice this means the `RDD` type needs to be a `Map` (whether a Scala or a Java one), a [`JavaBean`](http://docs.oracle.com/javase/tutorial/javabeans/) or a Scala [case class](http://docs.scala-lang.org/tutorials/tour/case-classes.html). When that is not the case, one can easily *transform* the data in Spark or plug-in their own custom [`ValueWriter`](/reference/configuration.md#configuration-serialization).

##### Scala [spark-write-scala]

When using Scala, simply import the `org.elasticsearch.spark` package which, through the [*pimp my library*](http://www.artima.com/weblogs/viewpost.jsp?thread=179766) pattern, enriches the  *any* `RDD` API with `saveToEs` methods:

```scala
import org.apache.spark.SparkContext    <1>
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._        <2>

...

val conf = ...
val sc = new SparkContext(conf)         <3>

val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

sc.makeRDD( <4>
  Seq(numbers, airports)
).saveToEs("spark/docs") <5>
```

1. Spark Scala imports
2. elasticsearch-hadoop Scala imports
3. start Spark through its Scala API
4. `makeRDD` creates an ad-hoc `RDD` based on the collection specified; any other `RDD` (in Java or Scala) can be passed in
5. index the content (namely the two *documents* (numbers and airports)) in {{es}} under `spark/docs`


::::{note}
Scala users might be tempted to use `Seq` and the `→` notation for declaring *root* objects (that is the JSON document) instead of using a `Map`. While similar, the first notation results in slightly different types that cannot be matched to a JSON document: `Seq` is an order sequence (in other words a list) while `→` creates a `Tuple` which is more or less an ordered, fixed number of elements. As such, a list of lists cannot be used as a document since it cannot be mapped to a JSON object; however it can be used freely within one. Hence why in the example above `Map(k→v)` was used instead of `Seq(k→v)`
::::


As an alternative to the *implicit* import above, one can use elasticsearch-hadoop Spark support in Scala through `EsSpark` in the `org.elasticsearch.spark.rdd` package which acts as a utility class allowing explicit method invocations. Additionally instead of `Map`s (which are convenient but require one mapping per instance due to their difference in structure), use a *case class* :

```scala
import org.apache.spark.SparkContext
import org.elasticsearch.spark.rdd.EsSpark                        <1>

// define a case class
case class Trip(departure: String, arrival: String)               <2>

val upcomingTrip = Trip("OTP", "SFO")
val lastWeekTrip = Trip("MUC", "OTP")

val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))             <3>
EsSpark.saveToEs(rdd, "spark/docs")                               <4>
```

1. `EsSpark` import
2. Define a case class named `Trip`
3. Create an `RDD` around the `Trip` instances
4. Index the `RDD` explicitly through `EsSpark`


For cases where the id (or other metadata fields like `ttl` or `timestamp`) of the document needs to be specified, one can do so by setting the appropriate [mapping](/reference/configuration.md#cfg-mapping) namely `es.mapping.id`. Following the previous example, to indicate to {{es}} to use the field `id` as the document id, update the `RDD` configuration (it is also possible to set the property on the `SparkConf` though due to its global effect it is discouraged):

```scala
EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))
```


##### Java [spark-write-java]

Java users have a dedicated class that provides a similar functionality to `EsSpark`, namely `JavaEsSpark` in the `org.elasticsearch.spark.rdd.api.java` (a package similar to Spark’s [Java API](https://spark.apache.org/docs/1.0.1/api/java/index.md?org/apache/spark/api/java/package-summary.md)):

```java
import org.apache.spark.api.java.JavaSparkContext;                              <1>
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;                        <2>
...

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);                              <3>

Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);                   <4>
Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));<5>
JavaEsSpark.saveToEs(javaRDD, "spark/docs");                                    <6>
```

1. Spark Java imports
2. elasticsearch-hadoop Java imports
3. start Spark through its Java API
4. to simplify the example, use [Guava](https://code.google.com/p/guava-libraries/)(a dependency of Spark) `Immutable`* methods for simple `Map`, `List` creation
5. create a simple `RDD` over the two collections; any other `RDD` (in Java or Scala) can be passed in
6. index the content (namely the two *documents* (numbers and airports)) in {{es}} under `spark/docs`


The code can be further simplified by using Java 5 *static* imports. Additionally, the `Map` (who’s mapping is dynamic due to its *loose* structure) can be replaced with a `JavaBean`:

```java
public class TripBean implements Serializable {
   private String departure, arrival;

   public TripBean(String departure, String arrival) {
       setDeparture(departure);
       setArrival(arrival);
   }

   public TripBean() {}

   public String getDeparture() { return departure; }
   public String getArrival() { return arrival; }
   public void setDeparture(String dep) { departure = dep; }
   public void setArrival(String arr) { arrival = arr; }
}
```

```java
import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark;                <1>
...

TripBean upcoming = new TripBean("OTP", "SFO");
TripBean lastWeek = new TripBean("MUC", "OTP");

JavaRDD<TripBean> javaRDD = jsc.parallelize(
                            ImmutableList.of(upcoming, lastWeek));        <2>
saveToEs(javaRDD, "spark/docs");                                          <3>
```

1. statically import `JavaEsSpark`
2. define an `RDD` containing `TripBean` instances (`TripBean` is a `JavaBean`)
3. call `saveToEs` method without having to type `JavaEsSpark` again


Setting the document id (or other metadata fields like `ttl` or `timestamp`) is similar to its Scala counterpart, though potentially a bit more verbose depending on whether you are using the JDK classes or some other utilities (like Guava):

```java
JavaEsSpark.saveToEs(javaRDD, "spark/docs", ImmutableMap.of("es.mapping.id", "id"));
```


#### Writing existing JSON to {{es}} [spark-write-json]

For cases where the data in the `RDD` is already in JSON, elasticsearch-hadoop allows direct indexing *without* applying any transformation; the data is taken as is and sent directly to {{es}}. As such, in this case, elasticsearch-hadoop expects either an `RDD` containing `String` or byte arrays (`byte[]`/`Array[Byte]`), assuming each entry represents a JSON document. If the `RDD` does not have the proper signature, the `saveJsonToEs` methods cannot be applied (in Scala they will not be available).


##### Scala [spark-write-json-scala]

```scala
val json1 = """{"reason" : "business", "airport" : "SFO"}"""      <1>
val json2 = """{"participants" : 5, "airport" : "OTP"}"""

new SparkContext(conf).makeRDD(Seq(json1, json2))
                      .saveJsonToEs("spark/json-trips") <2>
```

1. example of an entry within the `RDD` - the JSON is *written* as is, without any transformation, it should not contains breakline character like \n or \r\n
2. index the JSON data through the dedicated `saveJsonToEs` method



##### Java [spark-write-json-java]

```java
String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";  <1>
String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";

JavaSparkContext jsc = ...
JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2)); <2>
JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");             <3>
```

1. example of an entry within the `RDD` - the JSON is *written* as is, without any transformation, it should not contains breakline character like \n or \r\n
2. notice the `RDD<String>` signature
3. index the JSON data through the dedicated `saveJsonToEs` method



#### Writing to dynamic/multi-resources [spark-write-dyn]

For cases when the data being written to {{es}} needs to be indexed under different buckets (based on the data content) one can use the `es.resource.write` field which accepts a pattern that is resolved from the document content, at runtime. Following the aforementioned [media example](/reference/configuration.md#cfg-multi-writes), one could configure it as follows:


##### Scala [spark-write-dyn-scala]

```scala
val game = Map(
  "media_type"->"game", <1>
      "title" -> "FF VI",
       "year" -> "1994")
val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/doc")  <2>
```

1. Document *key* used for splitting the data. Any field can be declared (but make sure it is available in all documents)
2. Save each object based on its resource pattern, in this example based on `media_type`


For each document/object about to be written, elasticsearch-hadoop will extract the `media_type` field and use its value to determine the target resource.


##### Java [spark-write-dyn-java]

As expected, things in Java are strikingly similar:

```java
Map<String, ?> game =
  ImmutableMap.of("media_type", "game", "title", "FF VI", "year", "1994");
Map<String, ?> book = ...
Map<String, ?> cd = ...

JavaRDD<Map<String, ?>> javaRDD =
                jsc.parallelize(ImmutableList.of(game, book, cd));
saveToEs(javaRDD, "my-collection-{media_type}/doc");  <1>
```

1. Save each object based on its resource pattern, `media_type` in this example



#### Handling document metadata [spark-write-meta]

{{es}} allows each document to have its own [metadata](elasticsearch://reference/elasticsearch/mapping-reference/document-metadata-fields.md). As explained above, through the various [mapping](/reference/configuration.md#cfg-mapping) options one can customize these parameters so that their values are extracted from their belonging document. Further more, one can even include/exclude what parts of the data are sent back to {{es}}. In Spark, elasticsearch-hadoop extends this functionality allowing metadata to be supplied *outside* the document itself through the use of [*pair* `RDD`s](http://spark.apache.org/docs/latest/programming-guide.html#working-with-key-value-pairs). In other words, for `RDD`s containing a key-value tuple, the metadata can be extracted from the key and the value used as the document source.

The metadata is described through the `Metadata` Java [enum](http://docs.oracle.com/javase/tutorial/java/javaOO/enum.md) within `org.elasticsearch.spark.rdd` package which identifies its type - `id`, `ttl`, `version`, etc…​ Thus an `RDD` keys can be a `Map` containing the `Metadata` for each document and its associated values. If `RDD` key is not of type `Map`, elasticsearch-hadoop will consider the object as representing the document id and use it accordingly. This sounds more complicated than it is, so let us see some examples.


##### Scala [spark-write-meta-scala]

Pair `RDD`s, or simply put `RDD`s with the signature `RDD[(K,V)]` can take advantage of the `saveToEsWithMeta` methods that are available either through the *implicit* import of `org.elasticsearch.spark` package or `EsSpark` object. To manually specify the id for each document, simply pass in the `Object` (not of type `Map`) in your `RDD`:

```scala
val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
val muc = Map("iata" -> "MUC", "name" -> "Munich")
val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

// instance of SparkContext
val sc = ...

val airportsRDD = <1>
  sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))  <2>
airportsRDD.saveToEsWithMeta("airports/2015")    <3>
```

1. `airportsRDD` is a *key-value* pair `RDD`; it is created from a `Seq` of `tuple`s
2. The key of each tuple within the `Seq` represents the *id* of its associated value/document; in other words, document `otp` has id `1`, `muc` `2` and `sfo` `3`
3. Since `airportsRDD` is a pair `RDD`, it has the `saveToEsWithMeta` method available. This tells elasticsearch-hadoop to pay special attention to the `RDD` keys and use them as metadata, in this case as document ids. If `saveToEs` would have been used instead, then elasticsearch-hadoop would consider the `RDD` tuple, that is both the key and the value, as part of the document.


When more than just the id needs to be specified, one should use a `scala.collection.Map` with keys of type `org.elasticsearch.spark.rdd.Metadata`:

```scala
import org.elasticsearch.spark.rdd.Metadata._          <1>

val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
val muc = Map("iata" -> "MUC", "name" -> "Munich")
val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

// metadata for each document
// note it's not required for them to have the same structure
val otpMeta = Map(ID -> 1, TTL -> "3h")                <2>
val mucMeta = Map(ID -> 2, VERSION -> "23")            <3>
val sfoMeta = Map(ID -> 3)                             <4>

// instance of SparkContext
val sc = ...

val airportsRDD = sc.makeRDD( <5>
  Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
airportsRDD.saveToEsWithMeta("airports/2015") <6>
```

1. Import the `Metadata` enum
2. The metadata used for `otp` document. In this case, `ID` with a value of 1 and `TTL` with a value of `3h`
3. The metadata used for `muc` document. In this case, `ID` with a value of 2 and `VERSION` with a value of `23`
4. The metadata used for `sfo` document. In this case, `ID` with a value of 3
5. The metadata and the documents are assembled into a *pair* `RDD`
6. The `RDD` is saved accordingly using the `saveToEsWithMeta` method



##### Java [spark-write-meta-java]

In a similar fashion, on the Java side, `JavaEsSpark` provides `saveToEsWithMeta` methods that are applied to `JavaPairRDD` (the equivalent in Java of `RDD[(K,V)]`). Thus to save documents based on their ids one can use:

```java
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

// data to be saved
Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
Map<String, ?> jfk = ImmutableMap.of("iata", "JFK", "name", "JFK NYC");

JavaSparkContext jsc = ...

// create a pair RDD between the id and the docs
JavaPairRDD<?, ?> pairRdd = jsc.parallelizePairs(ImmutableList.of( <1>
        new Tuple2<Object, Object>(1, otp),          <2>
        new Tuple2<Object, Object>(2, jfk)));        <3>
JavaEsSpark.saveToEsWithMeta(pairRDD, target);       <4>
```

1. Create a `JavaPairRDD` by using Scala `Tuple2` class wrapped around the document id and the document itself
2. Tuple for the first document wrapped around the id (`1`) and the doc (`otp`) itself
3. Tuple for the second document wrapped around the id (`2`) and `jfk`
4. The `JavaPairRDD` is saved accordingly using the keys as a id and the values as documents


When more than just the id needs to be specified, one can choose to use a `java.util.Map` populated with keys of type `org.elasticsearch.spark.rdd.Metadata`:

```java
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.rdd.Metadata;          <1>

import static org.elasticsearch.spark.rdd.Metadata.*; <2>

// data to be saved
Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
Map<String, ?> sfo = ImmutableMap.of("iata", "SFO", "name", "San Fran");

// metadata for each document
// note it's not required for them to have the same structure
Map<Metadata, Object> otpMeta = ImmutableMap.<Metadata, Object>of(ID, 1, TTL, "1d"); <3>
Map<Metadata, Object> sfoMeta = ImmutableMap.<Metadata, Object> of(ID, "2", VERSION, "23"); <4>

JavaSparkContext jsc = ...

// create a pair RDD between the id and the docs
JavaPairRDD<?, ?> pairRdd = jsc.parallelizePairs<(ImmutableList.of(
        new Tuple2<Object, Object>(otpMeta, otp),    <5>
        new Tuple2<Object, Object>(sfoMeta, sfo)));  <6>
JavaEsSpark.saveToEsWithMeta(pairRDD, target);       <7>
```

1. `Metadata` `enum` describing the document metadata that can be declared
2. static import for the `enum` to refer to its values in short format (`ID`, `TTL`, etc…​)
3. Metadata for `otp` document
4. Metadata for `sfo` document
5. Tuple between `otp` (as the value) and its metadata (as the key)
6. Tuple associating `sfo` and its metadata
7. `saveToEsWithMeta` invoked over the `JavaPairRDD` containing documents and their respective metadata



#### Reading data from {{es}} [spark-read]

For reading, one should define the {{es}} `RDD` that *streams* data from {{es}} to Spark.


##### Scala [spark-read-scala]

Similar to writing, the `org.elasticsearch.spark` package, enriches the `SparkContext` API with `esRDD` methods:

```scala
import org.apache.spark.SparkContext    <1>
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._        <2>

...

val conf = ...
val sc = new SparkContext(conf)         <3>

val RDD = sc.esRDD("radio/artists")     <4>
```

1. Spark Scala imports
2. elasticsearch-hadoop Scala imports
3. start Spark through its Scala API
4. a dedicated `RDD` for {{es}} is created for index `radio/artists`


The method can be overloaded to specify an additional query or even a configuration `Map` (overriding `SparkConf`):

```scala
...
import org.elasticsearch.spark._

...
val conf = ...
val sc = new SparkContext(conf)

sc.esRDD("radio/artists", "?q=me*") <1>
```

1. create an `RDD` streaming all the documents matching `me*` from index `radio/artists`


The documents from {{es}} are returned, by default, as a `Tuple2` containing as the first element the document id and the second element the actual document represented through Scala [collections](http://docs.scala-lang.org/overviews/collections/overview.html), namely one `Map[String, Any]`where the keys represent the field names and the value their respective values.


##### Java [spark-read-java]

Java users have a dedicated `JavaPairRDD` that works the same as its Scala counterpart however the returned `Tuple2` values (or second element) returns the documents as native, `java.util` collections.

```java
import org.apache.spark.api.java.JavaSparkContext;               <1>
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;             <2>
...

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);               <3>

JavaPairRDD<String, Map<String, Object>> esRDD =
                        JavaEsSpark.esRDD(jsc, "radio/artists"); <4>
```

1. Spark Java imports
2. elasticsearch-hadoop Java imports
3. start Spark through its Java API
4. a dedicated `JavaPairRDD` for {{es}} is created for index `radio/artists`


In a similar fashion one can use the overloaded `esRDD` methods to specify a query or pass a `Map` object for advanced configuration. Let us see how this looks, but this time around using [Java static imports](http://docs.oracle.com/javase/1.5.0/docs/guide/language/static-import.md). Further more, let us discard the documents ids and retrieve only the `RDD` values:

```java
import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.*;   <1>

...
JavaRDD<Map<String, Object>> rdd =
        esRDD(jsc, "radio/artists", "?q=me*")  <2>
            .values(); <3>
```

1. statically import `JavaEsSpark` class
2. create an `RDD` streaming all the documents starting with `me` from index `radio/artists`. Note the method does not have to be fully qualified due to the static import
3. return only *values* of the `PairRDD` - hence why the result is of type `JavaRDD` and *not* `JavaPairRDD`


By using the `JavaEsSpark` API, one gets a hold of Spark’s dedicated `JavaPairRDD` which are better suited in Java environments than the base `RDD` (due to its Scala signatures). Moreover, the dedicated `RDD` returns {{es}} documents as proper Java collections so one does not have to deal with Scala collections (which is typically the case with `RDD`s). This is particularly powerful when using Java 8, which we strongly recommend as its [lambda expressions](http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.md) make collection processing *extremely* concise.

To wit, let us assume one wants to filter the documents from the `RDD` and return only those that contain a value that contains `mega` (please ignore the fact one can and should do the filtering directly through {{es}}).

In versions prior to Java 8, the code would look something like this:

```java
JavaRDD<Map<String, Object>> esRDD =
                        esRDD(jsc, "radio/artists", "?q=me*").values();
JavaRDD<Map<String, Object>> filtered = esRDD.filter(
    new Function<Map<String, Object>, Boolean>() {
      @Override
      public Boolean call(Map<String, Object> map) throws Exception {
          returns map.contains("mega");
      }
    });
```

with Java 8, the filtering becomes a one liner:

```java
JavaRDD<Map<String, Object>> esRDD =
                        esRDD(jsc, "radio/artists", "?q=me*").values();
JavaRDD<Map<String, Object>> filtered = esRDD.filter(doc ->
                                                doc.contains("mega"));
```


##### Reading data in JSON format [spark-read-json]

In case where the results from {{es}} need to be in JSON format (typically to be sent down the wire to some other system), one can use the dedicated `esJsonRDD` methods. In this case, the connector will return the documents content as it is received from {{es}} without any processing as an `RDD[(String, String)]` in Scala or `JavaPairRDD[String, String]` in Java with the keys representing the document id and the value its actual content in JSON format.


#### Type conversion [spark-type-conversion]

::::{important}
When dealing with multi-value/array fields, please see [this](/reference/mapping-types.md#mapping-multi-values) section and in particular [these](/reference/configuration.md#cfg-field-info) configuration options. IMPORTANT: If automatic index creation is used, please review [this](/reference/mapping-types.md#auto-mapping-type-loss) section for more information.
::::


elasticsearch-hadoop automatically converts Spark built-in types to {{es}} [types](elasticsearch://reference/elasticsearch/mapping-reference/field-data-types.md) (and back) as shown in the table below:

| Scala type | {{es}} type |
| --- | --- |
| `None` | `null` |
| `Unit` | `null` |
| `Nil` | empty `array` |
| `Some[T]` | `T` according to the table |
| `Map` | `object` |
| `Traversable` | `array` |
| *case class* | `object` (see `Map`) |
| `Product` | `array` |

in addition, the following *implied* conversion applies for Java types:

| Java type | {{es}} type |
| --- | --- |
| `null` | `null` |
| `String` | `string` |
| `Boolean` | `boolean` |
| `Byte` | `byte` |
| `Short` | `short` |
| `Integer` | `int` |
| `Long` | `long` |
| `Double` | `double` |
| `Float` | `float` |
| `Number` | `float` or `double` (depending on size) |
| `java.util.Calendar` | `date`  (`string` format) |
| `java.util.Date` | `date`  (`string` format) |
| `java.util.Timestamp` | `date`  (`string` format) |
| `byte[]` | `string` (BASE64) |
| `Object[]` | `array` |
| `Iterable` | `array` |
| `Map` | `object` |
| *Java Bean* | `object` (see `Map`) |

The conversion is done as a *best* effort; built-in Java and Scala types are guaranteed to be properly converted, however there are no guarantees for user types whether in Java or Scala. As mentioned in the tables above, when a `case` class is encountered in Scala or `JavaBean` in Java, the converters will try to `unwrap` its content and save it as an `object`. Note this works only for top-level user objects - if the user object has other user objects nested in, the conversion is likely to fail since the converter does not perform nested `unwrapping`. This is done on purpose since the converter has to *serialize* and *deserialize* the data and user types introduce ambiguity due to data loss; this can be addressed through some type of mapping however that takes the project way too close to the realm of ORMs and arguably introduces too much complexity for little to no gain; thanks to the processing functionality in Spark and the plugability in elasticsearch-hadoop one can easily transform objects into other types, if needed with minimal effort and maximum control.

It is worth mentioning that rich data types available only in {{es}}, such as [`GeoPoint`](elasticsearch://reference/elasticsearch/mapping-reference/geo-point.md) or [`GeoShape`](elasticsearch://reference/elasticsearch/mapping-reference/geo-shape.md) are supported by converting their structure into the primitives available in the table above. For example, based on its storage a `geo_point` might be returned as a `String` or a `Traversable`.


## Spark Streaming support [spark-streaming]

::::{note}
Added in 5.0.
::::


[TBC: FANCY QUOTE]
Spark Streaming is an extension on top of the core Spark functionality that allows near real time processing of stream data. Spark Streaming works around the idea of `DStream`s, or *Discretized Streams*. `DStreams` operate by collecting newly arrived records into a small `RDD` and executing it. This repeats every few seconds with a new `RDD` in a process called *microbatching*. The `DStream` api includes many of the same processing operations as the `RDD` api, plus a few other streaming specific methods. elasticsearch-hadoop provides native integration with Spark Streaming as of version 5.0.

When using the elasticsearch-hadoop Spark Streaming support, {{es}} can be targeted as an output location to index data into from a Spark Streaming job in the same way that one might persist the results from an `RDD`. Though, unlike `RDD`s, you are unable to read data out of {{es}} using a `DStream` due to the continuous nature of it.

::::{important}
Spark Streaming support provides special optimizations to allow for conservation of network resources on Spark executors when running jobs with very small processing windows. For this reason, one should prefer to use this integration instead of invoking `saveToEs` on `RDD`s returned from the `foreachRDD` call on `DStream`.
::::



#### Writing `DStream` to {{es}} [spark-streaming-write]

Like `RDD`s, any `DStream` can be saved to {{es}} as long as its content can be translated into documents. In practice this means the `DStream` type needs to be a `Map` (either a Scala or a Java one), a [`JavaBean`](http://docs.oracle.com/javase/tutorial/javabeans/) or a Scala [case class](http://docs.scala-lang.org/tutorials/tour/case-classes.html). When that is not the case, one can easily *transform* the data in Spark or plug-in their own custom [`ValueWriter`](/reference/configuration.md#configuration-serialization).


##### Scala [spark-streaming-write-scala]

When using Scala, simply import the `org.elasticsearch.spark.streaming` package which, through the [*pimp my library*](http://www.artima.com/weblogs/viewpost.jsp?thread=179766) pattern, enriches the `DStream` API with `saveToEs` methods:

```scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._               <1>
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import org.elasticsearch.spark.streaming._           <2>

...

val conf = ...
val sc = new SparkContext(conf)                      <3>
val ssc = new StreamingContext(sc, Seconds(1))       <4>

val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

val rdd = sc.makeRDD(Seq(numbers, airports))
val microbatches = mutable.Queue(rdd)                <5>

ssc.queueStream(microbatches).saveToEs("spark/docs") <6>

ssc.start()
ssc.awaitTermination() <7>
```

1. Spark and Spark Streaming Scala imports
2. elasticsearch-hadoop Spark Streaming imports
3. start Spark through its Scala API
4. start SparkStreaming context by passing it the SparkContext. The microbatches will be processed every second.
5. `makeRDD` creates an ad-hoc `RDD` based on the collection specified; any other `RDD` (in Java or Scala) can be passed in. Create a queue of `RDD`s to signify the microbatches to perform.
6. Create a `DStream` out of the `RDD`s and index the content (namely the two _documents_ (numbers and airports)) in {{es}} under `spark/docs`
7. Start the spark Streaming Job and wait for it to eventually finish.


As an alternative to the *implicit* import above, one can use elasticsearch-hadoop Spark Streaming support in Scala through `EsSparkStreaming` in the `org.elasticsearch.spark.streaming` package which acts as a utility class allowing explicit method invocations. Additionally instead of `Map`s (which are convenient but require one mapping per instance due to their difference in structure), use a *case class* :

```scala
import org.apache.spark.SparkContext
import org.elasticsearch.spark.streaming.EsSparkStreaming         <1>

// define a case class
case class Trip(departure: String, arrival: String)               <2>

val upcomingTrip = Trip("OTP", "SFO")
val lastWeekTrip = Trip("MUC", "OTP")

val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
val microbatches = mutable.Queue(rdd)                             <3>
val dstream = ssc.queueStream(microbatches)

EsSparkStreaming.saveToEs(dstream, "spark/docs")                  <4>

ssc.start()                                                       <5>
```

1. `EsSparkStreaming` import
2. Define a case class named `Trip`
3. Create a `DStream` around the `RDD` of `Trip` instances
4. Configure the `DStream` to be indexed explicitly through `EsSparkStreaming`
5. Start the streaming process


::::{important}
Once a SparkStreamingContext is started, no new `DStream`s can be added or configured. Once a context has stopped, it cannot be restarted. There can only be one active SparkStreamingContext at a time per JVM. Also note that when stopping a SparkStreamingContext programmatically, it stops the underlying SparkContext unless instructed not to.
::::


For cases where the id (or other metadata fields like `ttl` or `timestamp`) of the document needs to be specified, one can do so by setting the appropriate [mapping](/reference/configuration.md#cfg-mapping) namely `es.mapping.id`. Following the previous example, to indicate to {{es}} to use the field `id` as the document id, update the `DStream` configuration (it is also possible to set the property on the `SparkConf` though due to its global effect it is discouraged):

```scala
EsSparkStreaming.saveToEs(dstream, "spark/docs", Map("es.mapping.id" -> "id"))
```


##### Java [spark-streaming-write-java]

Java users have a dedicated class that provides a similar functionality to `EsSparkStreaming`, namely `JavaEsSparkStreaming` in the package `org.elasticsearch.spark.streaming.api.java` (a package similar to Spark’s [Java API](https://spark.apache.org/docs/1.6.1/api/java/index.md?org/apache/spark/streaming/api/java/package-summary.md)):

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;                                              <1>
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;         <2>
...

SparkConf conf = ...
JavaSparkContext jsc = new JavaSparkContext(conf);                              <3>
JavaStreamingContext jssc = new JavaSparkStreamingContext(jsc, Seconds.apply(1));

Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);                   <4>
Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
Queue<JavaRDD<Map<String, ?>>> microbatches = new LinkedList<>();
microbatches.add(javaRDD);                                                      <5>
JavaDStream<Map<String, ?>> javaDStream = jssc.queueStream(microbatches);

JavaEsSparkStreaming.saveToEs(javaDStream, "spark/docs");                       <6>

jssc.start()                                                                    <7>
```

1. Spark and Spark Streaming Java imports
2. elasticsearch-hadoop Java imports
3. start Spark and Spark Streaming through its Java API. The microbatches will be processed every second.
4. to simplify the example, use [Guava](https://code.google.com/p/guava-libraries/)(a dependency of Spark) `Immutable`* methods for simple `Map`, `List` creation
5. create a simple `DStream` over the microbatch; any other `RDD`s (in Java or Scala) can be passed in
6. index the content (namely the two *documents* (numbers and airports)) in {{es}} under `spark/docs`
7. execute the streaming job.


The code can be further simplified by using Java 5 *static* imports. Additionally, the `Map` (who’s mapping is dynamic due to its *loose* structure) can be replaced with a `JavaBean`:

```java
public class TripBean implements Serializable {
   private String departure, arrival;

   public TripBean(String departure, String arrival) {
       setDeparture(departure);
       setArrival(arrival);
   }

   public TripBean() {}

   public String getDeparture() { return departure; }
   public String getArrival() { return arrival; }
   public void setDeparture(String dep) { departure = dep; }
   public void setArrival(String arr) { arrival = arr; }
}
```

```java
import static org.elasticsearch.spark.rdd.api.java.JavaEsSparkStreaming;  <1>
...

TripBean upcoming = new TripBean("OTP", "SFO");
TripBean lastWeek = new TripBean("MUC", "OTP");

JavaRDD<TripBean> javaRDD = jsc.parallelize(ImmutableList.of(upcoming, lastWeek));
Queue<JavaRDD<TripBean>> microbatches = new LinkedList<JavaRDD<TripBean>>();
microbatches.add(javaRDD);
JavaDStream<TripBean> javaDStream = jssc.queueStream(microbatches);       <2>

saveToEs(javaDStream, "spark/docs");                                          <3>

jssc.start()                                                              <4>
```

1. statically import `JavaEsSparkStreaming`
2. define a `DStream` containing `TripBean` instances (`TripBean` is a `JavaBean`)
3. call `saveToEs` method without having to type `JavaEsSparkStreaming` again
4. run that Streaming job


Setting the document id (or other metadata fields like `ttl` or `timestamp`) is similar to its Scala counterpart, though potentially a bit more verbose depending on whether you are using the JDK classes or some other utilities (like Guava):

```java
JavaEsSparkStreaming.saveToEs(javaDStream, "spark/docs", ImmutableMap.of("es.mapping.id", "id"));
```


#### Writing Existing JSON to {{es}} [spark-streaming-write-json]

For cases where the data being streamed by the `DStream` is already serialized as JSON, elasticsearch-hadoop allows direct indexing *without* applying any transformation; the data is taken as is and sent directly to {{es}}. As such, in this case, elasticsearch-hadoop expects either a `DStream` containing `String` or byte arrays (`byte[]`/`Array[Byte]`), assuming each entry represents a JSON document. If the `DStream` does not have the proper signature, the `saveJsonToEs` methods cannot be applied (in Scala they will not be available).


##### Scala [spark-streaming-write-json-scala]

```scala
val json1 = """{"reason" : "business", "airport" : "SFO"}"""      <1>
val json2 = """{"participants" : 5, "airport" : "OTP"}"""

val sc = new SparkContext(conf)
val ssc = new StreamingContext(sc, Seconds(1))

val rdd = sc.makeRDD(Seq(json1, json2))
val microbatch = mutable.Queue(rdd)
ssc.queueStream(microbatch).saveJsonToEs("spark/json-trips")      <2>

ssc.start()                                                       <3>
```

1. example of an entry within the `DStream` - the JSON is *written* as is, without any transformation
2. configure the stream to index the JSON data through the dedicated `saveJsonToEs` method
3. start the streaming job



##### Java [spark-streaming-write-json-java]

```java
String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";  <1>
String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";

JavaSparkContext jsc = ...
JavaStreamingContext jssc = ...
JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2));
Queue<JavaRDD<String>> microbatches = new LinkedList<JavaRDD<String>>();      <2>
microbatches.add(stringRDD);
JavaDStream<String> stringDStream = jssc.queueStream(microbatches);  <3>

JavaEsSparkStreaming.saveJsonToEs(stringRDD, "spark/json-trips");    <4>

jssc.start()                                                         <5>
```

1. example of an entry within the `DStream` - the JSON is *written* as is, without any transformation
2. creating an `RDD`, placing it into a queue, and creating a `DStream` out of the queued `RDD`s, treating each as a microbatch.
3. notice the `JavaDStream<String>` signature
4. configure stream to index the JSON data through the dedicated `saveJsonToEs` method
5. launch stream job



#### Writing to dynamic/multi-resources [spark-streaming-write-dyn]

For cases when the data being written to {{es}} needs to be indexed under different buckets (based on the data content) one can use the `es.resource.write` field which accepts a pattern that is resolved from the document content, at runtime. Following the aforementioned [media example](/reference/configuration.md#cfg-multi-writes), one could configure it as follows:


##### Scala [spark-streaming-write-dyn-scala]

```scala
val game = Map(
  "media_type" -> "game", <1>
       "title" -> "FF VI",
        "year" -> "1994")
val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

val batch = sc.makeRDD(Seq(game, book, cd))
val microbatches = mutable.Queue(batch)
ssc.queueStream(microbatches).saveToEs("my-collection-{media_type}/doc")  <2>
ssc.start()
```

1. Document *key* used for splitting the data. Any field can be declared (but make sure it is available in all documents)
2. Save each object based on its resource pattern, in this example based on `media_type`


For each document/object about to be written, elasticsearch-hadoop will extract the `media_type` field and use its value to determine the target resource.


##### Java [spark-streaming-write-dyn-java]

As expected, things in Java are strikingly similar:

```java
Map<String, ?> game =
  ImmutableMap.of("media_type", "game", "title", "FF VI", "year", "1994");
Map<String, ?> book = ...
Map<String, ?> cd = ...

JavaRDD<Map<String, ?>> javaRDD =
                jsc.parallelize(ImmutableList.of(game, book, cd));
Queue<JavaRDD<Map<String, ?>>> microbatches = ...
JavaDStream<Map<String, ?>> javaDStream =
                jssc.queueStream(microbatches);

saveToEs(javaDStream, "my-collection-{media_type}/doc");  <1>
jssc.start();
```

1. Save each object based on its resource pattern, `media_type` in this example



#### Handling document metadata [spark-streaming-write-meta]

{{es}} allows each document to have its own [metadata](elasticsearch://reference/elasticsearch/mapping-reference/document-metadata-fields.md). As explained above, through the various [mapping](/reference/configuration.md#cfg-mapping) options one can customize these parameters so that their values are extracted from their belonging document. Further more, one can even include/exclude what parts of the data are sent back to {{es}}. In Spark, elasticsearch-hadoop extends this functionality allowing metadata to be supplied *outside* the document itself through the use of [*pair* `RDD`s](http://spark.apache.org/docs/latest/programming-guide.html#working-with-key-value-pairs).

This is no different in Spark Streaming. For `DStreams`s containing a key-value tuple, the metadata can be extracted from the key and the value used as the document source.

The metadata is described through the `Metadata` Java [enum](http://docs.oracle.com/javase/tutorial/java/javaOO/enum.md) within `org.elasticsearch.spark.rdd` package which identifies its type - `id`, `ttl`, `version`, etc…​ Thus a `DStream’s keys can be a `Map` containing the `Metadata` for each document and its associated values. If the `DStream` key is not of type `Map`, elasticsearch-hadoop will consider the object as representing the document id and use it accordingly. This sounds more complicated than it is, so let us see some examples.


##### Scala [spark-streaming-write-meta-scala]

Pair `DStreams`s, or simply put `DStreams`s with the signature `DStream[(K,V)]` can take advantage of the `saveToEsWithMeta` methods that are available either through the *implicit* import of `org.elasticsearch.spark.streaming` package or `EsSparkStreaming` object. To manually specify the id for each document, simply pass in the `Object` (not of type `Map`) in your `DStream`:

```scala
val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
val muc = Map("iata" -> "MUC", "name" -> "Munich")
val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

// instance of SparkContext
val sc = ...
// instance of StreamingContext
val ssc = ...

val airportsRDD = <1>
  sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))  <2>
val microbatches = mutable.Queue(airportsRDD)

ssc.queueStream(microbatches)        <3>
  .saveToEsWithMeta("airports/2015") <4>
ssc.start()
```

1. `airportsRDD` is a *key-value* pair `RDD`; it is created from a `Seq` of `tuple`s
2. The key of each tuple within the `Seq` represents the *id* of its associated value/document; in other words, document `otp` has id `1`, `muc` `2` and `sfo` `3`
3. We construct a `DStream` which inherits the type signature of the `RDD`
4. Since the resulting `DStream` is a pair `DStream`, it has the `saveToEsWithMeta` method available. This tells elasticsearch-hadoop to pay special attention to the `DStream` keys and use them as metadata, in this case as document ids. If `saveToEs` would have been used instead, then elasticsearch-hadoop would consider the `DStream` tuple, that is both the key and the value, as part of the document.


When more than just the id needs to be specified, one should use a `scala.collection.Map` with keys of type `org.elasticsearch.spark.rdd.Metadata`:

```scala
import org.elasticsearch.spark.rdd.Metadata._          <1>

val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
val muc = Map("iata" -> "MUC", "name" -> "Munich")
val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

// metadata for each document
// note it's not required for them to have the same structure
val otpMeta = Map(ID -> 1, TTL -> "3h")                <2>
val mucMeta = Map(ID -> 2, VERSION -> "23")            <3>
val sfoMeta = Map(ID -> 3)                             <4>

// instance of SparkContext
val sc = ...
// instance of StreamingContext
val ssc = ...

val airportsRDD = sc.makeRDD( <5>
  Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
val microbatches = mutable.Queue(airportsRDD)

ssc.queueStream(microbatches)        <6>
  .saveToEsWithMeta("airports/2015") <7>
ssc.start()
```

1. Import the `Metadata` enum
2. The metadata used for `otp` document. In this case, `ID` with a value of 1 and `TTL` with a value of `3h`
3. The metadata used for `muc` document. In this case, `ID` with a value of 2 and `VERSION` with a value of `23`
4. The metadata used for `sfo` document. In this case, `ID` with a value of 3
5. The metadata and the documents are assembled into a *pair* `RDD`
6. The `DStream` inherits the signature from the `RDD`, becoming a pair `DStream`
7. The `DStream` is configured to index the data accordingly using the `saveToEsWithMeta` method



##### Java [spark-streaming-write-meta-java]

In a similar fashion, on the Java side, `JavaEsSparkStreaming` provides `saveToEsWithMeta` methods that are applied to `JavaPairDStream` (the equivalent in Java of `DStream[(K,V)]`).

This tends to involve a little more work due to the Java API’s limitations. For instance, you cannot create a `JavaPairDStream` directly from a queue of `JavaPairRDD`s. Instead, you must create a regular `JavaDStream` of `Tuple2` objects and convert the `JavaDStream` into a `JavaPairDStream`. This sounds complex, but it’s a simple work around for a limitation of the API.

First, we’ll create a pair function, that takes a `Tuple2` object in, and returns it right back to the framework:

```java
public static class ExtractTuples implements PairFunction<Tuple2<Object, Object>, Object, Object>, Serializable {
    @Override
    public Tuple2<Object, Object> call(Tuple2<Object, Object> tuple2) throws Exception {
        return tuple2;
    }
}
```

Then we’ll apply the pair function to a `JavaDStream` of `Tuple2`s to create a `JavaPairDStream` and save it:

```java
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

// data to be saved
Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
Map<String, ?> jfk = ImmutableMap.of("iata", "JFK", "name", "JFK NYC");

JavaSparkContext jsc = ...
JavaStreamingContext jssc = ...

// create an RDD of between the id and the docs
JavaRDD<Tuple2<?, ?>> rdd = jsc.parallelize(         <1>
      ImmutableList.of(
        new Tuple2<Object, Object>(1, otp),          <2>
        new Tuple2<Object, Object>(2, jfk)));        <3>

Queue<JavaRDD<Tuple2<?, ?>>> microbatches = ...
JavaDStream<Tuple2<?, ?>> dStream = jssc.queueStream(microbatches); <4>

JavaPairDStream<?, ?> pairDStream = dstream.mapToPair(new ExtractTuples()); <5>

JavaEsSparkStreaming.saveToEsWithMeta(pairDStream, target);       <6>
jssc.start();
```

1. Create a regular `JavaRDD` of Scala `Tuple2`s wrapped around the document id and the document itself
2. Tuple for the first document wrapped around the id (`1`) and the doc (`otp`) itself
3. Tuple for the second document wrapped around the id (`2`) and `jfk`
4. Assemble a regular `JavaDStream` out of the tuple `RDD`
5. Transform the `JavaDStream` into a `JavaPairDStream` by passing our `Tuple2` identity function to the `mapToPair` method. This will allow the type to be converted to a `JavaPairDStream`. This function could be replaced by anything in your job that would extract both the id and the document to be indexed from a single entry.
6. The `JavaPairRDD` is configured to index the data accordingly using the keys as a id and the values as documents


When more than just the id needs to be specified, one can choose to use a `java.util.Map` populated with keys of type `org.elasticsearch.spark.rdd.Metadata`. We’ll use the same typing trick to repack the `JavaDStream` as a `JavaPairDStream`:

```java
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.elasticsearch.spark.rdd.Metadata;          <1>

import static org.elasticsearch.spark.rdd.Metadata.*; <2>

// data to be saved
Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
Map<String, ?> sfo = ImmutableMap.of("iata", "SFO", "name", "San Fran");

// metadata for each document
// note it's not required for them to have the same structure
Map<Metadata, Object> otpMeta = ImmutableMap.<Metadata, Object>of(ID, 1, TTL, "1d"); <3>
Map<Metadata, Object> sfoMeta = ImmutableMap.<Metadata, Object> of(ID, "2", VERSION, "23"); <4>

JavaSparkContext jsc = ...

// create a pair RDD between the id and the docs
JavaRDD<Tuple2<?, ?>> pairRdd = jsc.parallelize<(ImmutableList.of(
        new Tuple2<Object, Object>(otpMeta, otp),    <5>
        new Tuple2<Object, Object>(sfoMeta, sfo)));  <6>

Queue<JavaRDD<Tuple2<?, ?>>> microbatches = ...
JavaDStream<Tuple2<?, ?>> dStream = jssc.queueStream(microbatches); <7>

JavaPairDStream<?, ?> pairDStream = dstream.mapToPair(new ExtractTuples()) <8>

JavaEsSparkStreaming.saveToEsWithMeta(pairDStream, target);       <9>
jssc.start();
```

1. `Metadata` `enum` describing the document metadata that can be declared
2. static import for the `enum` to refer to its values in short format (`ID`, `TTL`, etc…​)
3. Metadata for `otp` document
4. Metadata for `sfo` document
5. Tuple between `otp` (as the value) and its metadata (as the key)
6. Tuple associating `sfo` and its metadata
7. Create a `JavaDStream` out of the `JavaRDD`
8. Repack the `JavaDStream` into a `JavaPairDStream` by mapping the `Tuple2` identity function over it.
9. `saveToEsWithMeta` invoked over the `JavaPairDStream` containing documents and their respective metadata



#### Spark Streaming Type Conversion [spark-streaming-type-conversion]

The elasticsearch-hadoop Spark Streaming support leverages the same type mapping as the regular Spark type mapping. The mappings are repeated here for consistency:

| Scala type | {{es}} type |
| --- | --- |
| `None` | `null` |
| `Unit` | `null` |
| `Nil` | empty `array` |
| `Some[T]` | `T` according to the table |
| `Map` | `object` |
| `Traversable` | `array` |
| *case class* | `object` (see `Map`) |
| `Product` | `array` |

in addition, the following *implied* conversion applies for Java types:

| Java type | {{es}} type |
| --- | --- |
| `null` | `null` |
| `String` | `string` |
| `Boolean` | `boolean` |
| `Byte` | `byte` |
| `Short` | `short` |
| `Integer` | `int` |
| `Long` | `long` |
| `Double` | `double` |
| `Float` | `float` |
| `Number` | `float` or `double` (depending on size) |
| `java.util.Calendar` | `date`  (`string` format) |
| `java.util.Date` | `date`  (`string` format) |
| `java.util.Timestamp` | `date`  (`string` format) |
| `byte[]` | `string` (BASE64) |
| `Object[]` | `array` |
| `Iterable` | `array` |
| `Map` | `object` |
| *Java Bean* | `object` (see `Map`) |

It is worth re-mentioning that rich data types available only in {{es}}, such as [`GeoPoint`](elasticsearch://reference/elasticsearch/mapping-reference/geo-point.md) or [`GeoShape`](elasticsearch://reference/elasticsearch/mapping-reference/geo-shape.md) are supported by converting their structure into the primitives available in the table above. For example, based on its storage a `geo_point` might be returned as a `String` or a `Traversable`.


## Spark SQL support [spark-sql]

::::{note}
Added in 2.1.
::::


[TBC: FANCY QUOTE]
On top of the core Spark support, elasticsearch-hadoop also provides integration with Spark SQL. In other words, {{es}} becomes a *native* source for Spark SQL so that data can be indexed and queried from Spark SQL *transparently*.

::::{important}
Spark SQL works with *structured* data - in other words, all entries are expected to have the *same* structure (same number of fields, of the same type and name). Using unstructured data (documents with different structures) is *not* supported and will cause problems. For such cases, use `PairRDD`s.
::::



#### Supported Spark SQL versions [spark-sql-versions]

Spark SQL while becoming a mature component, is still going through significant changes between releases. Spark SQL became a stable component in version 1.3, however it is [**not** backwards compatible](https://spark.apache.org/docs/latest/sql-programming-guide.html#migration-guide) with the previous releases. Further more Spark 2.0 introduced significant changed which broke backwards compatibility, through the `Dataset` API. elasticsearch-hadoop supports both version Spark SQL 1.3-1.6 and Spark SQL 2.0 through two different jars: `elasticsearch-spark-1.x-<version>.jar` and `elasticsearch-hadoop-<version>.jar` support Spark SQL 1.3-1.6 (or higher) while `elasticsearch-spark-2.0-<version>.jar` supports Spark SQL 2.0. In other words, unless you are using Spark 2.0, use `elasticsearch-spark-1.x-<version>.jar`

Spark SQL support is available under `org.elasticsearch.spark.sql` package.

From the elasticsearch-hadoop user perspectives, the differences between Spark SQL 1.3-1.6 and Spark 2.0 are fairly consolidated. This [document](http://spark.apache.org/docs/2.0.0/sql-programming-guide.md#upgrading-from-spark-sql-16-to-20) describes at length the differences which are briefly mentioned below:

`DataFrame` vs `Dataset`
:   The core unit of Spark SQL in 1.3+ is a `DataFrame`. This API remains in Spark 2.0 however underneath it is based on a `Dataset`

Unified API vs dedicated Java/Scala APIs
:   In Spark SQL 2.0, the APIs are further [unified](http://spark.apache.org/docs/2.0.0/sql-programming-guide.md#datasets-and-dataframes) by introducing `SparkSession` and by using the same backing code for both `Dataset`s, `DataFrame`s and `RDD`s.

As conceptually, a `DataFrame` is a `Dataset[Row]`, the documentation below will focus on Spark SQL 1.3-1.6.


#### Writing `DataFrame` (Spark SQL 1.3+) to {{es}} [spark-sql-write]

With elasticsearch-hadoop, `DataFrame`s (or any `Dataset` for that matter) can be indexed to {{es}}.


##### Scala [spark-sql-write-scala]

In Scala, simply import `org.elasticsearch.spark.sql` package which enriches the given `DataFrame` class with `saveToEs` methods; while these have the same signature as the `org.elasticsearch.spark` package, they are designed for `DataFrame` implementations:

```scala
// reusing the example from Spark SQL documentation

import org.apache.spark.sql.SQLContext    <1>
import org.apache.spark.sql.SQLContext._

import org.elasticsearch.spark.sql._      <2>

...

// sc = existing SparkContext
val sqlContext = new SQLContext(sc)

// case class used to define the DataFrame
case class Person(name: String, surname: String, age: Int)

//  create DataFrame
val people = sc.textFile("people.txt")    <3>
        .map(_.split(","))
        .map(p => Person(p(0), p(1), p(2).trim.toInt))
        .toDF()

people.saveToEs("spark/people")           <4>
```

1. Spark SQL package import
2. elasticsearch-hadoop Spark package import
3. Read a text file as *normal* `RDD` and map it to a `DataFrame` (using the `Person` case class)
4. Index the resulting `DataFrame` to {{es}} through the `saveToEs` method


::::{note}
By default, elasticsearch-hadoop will ignore null values in favor of not writing any field at all. Since a `DataFrame` is meant to be treated as structured tabular data, you can enable writing nulls as null valued fields for `DataFrame` Objects only by toggling the `es.spark.dataframe.write.null` setting to `true`.
::::



##### Java [spark-sql-write-java]

In a similar fashion, for Java usage the dedicated package `org.elasticsearch.spark.sql.api.java` provides similar functionality through the `JavaEsSpark SQL` :

```java
import org.apache.spark.sql.api.java.*;                      <1>
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;  <2>
...

DataFrame people = ...
JavaEsSparkSQL.saveToEs(people, "spark/people");                     <3>
```

1. Spark SQL Java imports
2. elasticsearch-hadoop Spark SQL Java imports
3. index the `DataFrame` in {{es}} under `spark/people`


Again, with Java 5 *static* imports this can be further simplied to:

```java
import static org.elasticsearch.spark.sql.api.java.JavaEsSpark SQL; <1>
...
saveToEs("spark/people");                                          <2>
```

1. statically import `JavaEsSpark SQL`
2. call `saveToEs` method without having to type `JavaEsSpark` again


::::{important}
For maximum control over the mapping of your `DataFrame` in {{es}}, it is highly recommended to create the mapping before hand. See [this](/reference/mapping-types.md) chapter for more information.
::::



#### Writing existing JSON to {{es}} [spark-sql-json]

When using Spark SQL, if the input data is in JSON format, simply convert it to a `DataFrame` (in Spark SQL 1.3) or a `Dataset` (for Spark SQL 2.0) (as described in Spark [documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#json-datasets)) through `SQLContext`/`JavaSQLContext` `jsonFile` methods.


#### Using pure SQL to read from {{es}} [spark-sql-read-ds]

::::{important}
The index and its mapping, have to exist prior to creating the temporary table
::::


Spark SQL 1.2 [introduced](http://spark.apache.org/releases/spark-release-1-2-0.html) a new [API](https://github.com/apache/spark/pull/2475) for reading from external data sources, which is supported by elasticsearch-hadoop simplifying the SQL configured needed for interacting with {{es}}. Further more, behind the scenes it understands the operations executed by Spark and thus can optimize the data and queries made (such as filtering or pruning), improving performance.


#### Data Sources in Spark SQL [spark-data-sources]

When using Spark SQL, elasticsearch-hadoop allows access to {{es}} through `SQLContext` `load` method. In other words, to create a `DataFrame`/`Dataset` backed by {{es}} in a *declarative* manner:

```scala
val sql = new SQLContext...
// Spark 1.3 style
val df = sql.load( <1>
  "spark/index",   <2>
  "org.elasticsearch.spark.sql") <3>
```

1. `SQLContext` *experimental* `load` method for arbitrary data sources
2. path or resource to load - in this case the index/type in {es}
3. the data source provider - `org.elasticsearch.spark.sql`


In Spark 1.4, one would use the following similar API calls:

```scala
// Spark 1.4 style
val df = sql.read      <1>
  .format("org.elasticsearch.spark.sql") <2>
  .load("spark/index") <3>
```

1. `SQLContext` *experimental* `read` method for arbitrary data sources
2. the data source provider - `org.elasticsearch.spark.sql`
3. path or resource to load - in this case the index/type in {es}


In Spark 1.5, this can be further simplified to:

```scala
// Spark 1.5 style
val df = sql.read.format("es")<1>
  .load("spark/index")
```

1. Use `es` as an alias instead of the full package name for the `DataSource` provider


Whatever API is used, once created, the `DataFrame` can be accessed freely to manipulate the data.

The *sources* declaration also allows specific options to be passed in, namely:

| Name | Default value | Description |
| --- | --- | --- |
| `path` | *required* | {{es}} index/type |
| `pushdown` | `true` | Whether to translate (*push-down*) Spark SQL into {{es}} Query DSL |
| `strict` | `false` | Whether to use *exact* (not analyzed) matching or not (analyzed) |
| Usable in Spark 1.6 or higher |
| `double.filtering` | `true` | Whether to tell Spark apply its own filtering on the filters pushed down |

Both options are explained in the next section. To specify the options (including the generic elasticsearch-hadoop ones), one simply passes a `Map` to the aforementioned methods:

For example:

```scala
val sql = new SQLContext...
// options for Spark 1.3 need to include the target path/resource
val options13 = Map("path" -> "spark/index",
                    "pushdown" -> "true",     <1>
                    "es.nodes" -> "someNode", <2>
                     "es.port" -> "9200")

// Spark 1.3 style
val spark13DF = sql.load("org.elasticsearch.spark.sql", options13) <3>

// options for Spark 1.4 - the path/resource is specified separately
val options = Map("pushdown" -> "true",     <1>
                  "es.nodes" -> "someNode", <2>
                   "es.port" -> "9200")

// Spark 1.4 style
val spark14DF = sql.read.format("org.elasticsearch.spark.sql")
                        .options(options) <3>
                        .load("spark/index")
```

1. `pushdown` option - specific to Spark data sources
2. `es.nodes` configuration option
3. pass the options when definition/loading the source


```scala
sqlContext.sql(
   "CREATE TEMPORARY TABLE myIndex    " + <1>
   "USING org.elasticsearch.spark.sql " + <2>
   "OPTIONS (resource 'spark/index', nodes 'someNode')" ) " <3>
```

1. Spark’s temporary table name
2. `USING` clause identifying the data source provider, in this case `org.elasticsearch.spark.sql`
3. elasticsearch-hadoop [configuration options](/reference/configuration.md), the mandatory one being `resource`. The `es.` prefix is fixed due to the SQL parser


Do note that due to the SQL parser, the `.` (among other common characters used for delimiting) is not allowed; the connector tries to work around it by append the `es.` prefix automatically however this works only for specifying the configuration options with only one `.` (like `es.nodes` above). Because of this, if properties with multiple `.` are needed, one should use the `SQLContext.load` or `SQLContext.read` methods above and pass the properties as a `Map`.


#### Push-Down operations [spark-pushdown]

An important *hidden* feature of using elasticsearch-hadoop as a Spark `source` is that the connector understand the operations performed within the `DataFrame`/SQL and, by default, will *translate* them into the appropriate [QueryDSL](docs-content://explore-analyze/query-filter/languages/querydsl.md). In other words, the connector *pushes* down the operations directly at the source, where the data is efficiently filtered out so that *only* the required data is streamed back to Spark. This significantly increases the queries performance and minimizes the CPU, memory and I/O on both Spark and {{es}} clusters as only the needed data is returned (as oppose to returning the data in bulk only to be processed and discarded by Spark). Note the push down operations apply even when one specifies a query - the connector will *enhance* it according to the specified SQL.

As a side note, elasticsearch-hadoop supports *all* the `Filter`s available in Spark (1.3.0 and higher) while retaining backwards binary-compatibility with Spark 1.3.0, pushing down to full extent the SQL operations to {{es}} without any user interference.

Operators those have been optimized as pushdown filters:

| SQL syntax | ES 1.x/2.x syntax | ES 5.x syntax |
| --- | --- | --- |
| = null , is_null | missing | must_not.exists |
| = (strict) | term | term |
| = (not strict) | match | match |
| > , < , >= , ⇐ | range | range |
| is_not_null | exists | exists |
| in (strict) | terms | terms |
| in (not strict) | or.filters | bool.should |
| and | and.filters | bool.filter |
| or | or.filters | bool.should [bool.filter] |
| not | not.filter | bool.must_not |
| StringStartsWith | wildcard(arg*) | wildcard(arg*) |
| StringEndsWith | wildcard(*arg) | wildcard(*arg) |
| StringContains | wildcard(*arg*) | wildcard(*arg*) |
| EqualNullSafe (strict) | term | term |
| EqualNullSafe (not strict) | match | match |

To wit, consider the following Spark SQL:

```scala
// as a DataFrame
val df = sqlContext.read().format("org.elasticsearch.spark.sql").load("spark/trips")

df.printSchema()
// root
//|-- departure: string (nullable = true)
//|-- arrival: string (nullable = true)
//|-- days: long (nullable = true)

val filter = df.filter(df("arrival").equalTo("OTP").and(df("days").gt(3))
```

or in pure SQL:

```sql
CREATE TEMPORARY TABLE trips USING org.elasticsearch.spark.sql OPTIONS (path "spark/trips")
SELECT departure FROM trips WHERE arrival = "OTP" and days > 3
```

The connector translates the query into:

```json
{
  "query" : {
    "filtered" : {
      "query" : {
        "match_all" : {}

      },
      "filter" : {
        "and" : [{
            "query" : {
              "match" : {
                "arrival" : "OTP"
              }
            }
          }, {
            "days" : {
              "gt" : 3
            }
          }
        ]
      }
    }
  }
}
```

Further more, the pushdown filters can work on `analyzed` terms (the default) or can be configured to be *strict* and provide `exact` matches (work only on `not-analyzed` fields). Unless one manually specifies the mapping, it is highly recommended to leave the defaults as they are.  This and other topics are discussed at length in the {{es}} [Reference Documentation](elasticsearch://reference/query-languages/query-dsl/query-dsl-term-query.md).

Note that `double.filtering`, available since elasticsearch-hadoop 2.2 for Spark 1.6 or higher, allows filters that are already pushed down to {{es}} to be processed/evaluated by Spark as well (default) or not. Turning this feature off, especially when dealing with large data sizes speed things up. However one should pay attention to the semantics as turning this off, might return different results (depending on how the data is indexed, `analyzed` vs `not_analyzed`). In general, when turning *strict* on, one can disable `double.filtering` as well.


#### Data Sources as tables [spark-data-sources-12]

Available since Spark SQL 1.2, one can also access a data source by declaring it as a Spark temporary table (backed by elasticsearch-hadoop):

```scala
sqlContext.sql(
   "CREATE TEMPORARY TABLE myIndex    " + <1>
   "USING org.elasticsearch.spark.sql " + <2>
   "OPTIONS (resource 'spark/index', " + <3>
            "scroll_size '20')" ) <4>
```

1. Spark’s temporary table name
2. `USING` clause identifying the data source provider, in this case `org.elasticsearch.spark.sql`
3. elasticsearch-hadoop [configuration options](/reference/configuration.md), the mandatory one being `resource`. One can use the `es` prefix or skip it for convenience.
4. Since using `.` can cause syntax exceptions, one should replace it instead with `_` style. Thus, in this example `es.scroll.size` becomes `scroll_size` (as the leading `es` can be removed). Do note this only works in Spark 1.3 as the Spark 1.4 has a stricter parser. See the chapter above for more information.


Once defined, the schema is picked up automatically. So one can issue queries, right away:

```sql
val all = sqlContext.sql("SELECT * FROM myIndex WHERE id <= 10")
```

As elasticsearch-hadoop is aware of the queries being made, it can *optimize* the requests done to {{es}}. For example, given the following query:

```sql
val names = sqlContext.sql("SELECT name FROM myIndex WHERE id >=1 AND id <= 10")
```

it knows only the `name` and `id` fields are required (the first to be returned to the user, the second for Spark’s internal filtering) and thus will ask *only* for this data, making the queries quite efficient.


#### Reading `DataFrame`s (Spark SQL 1.3) from {{es}} [spark-sql-read]

As you might have guessed, one can define a `DataFrame` backed by {{es}} documents. Or even better, have them backed by a query result, effectively creating dynamic, real-time *views* over your data.


##### Scala [spark-sql-read-scala]

Through the `org.elasticsearch.spark.sql` package, `esDF` methods are available on the `SQLContext` API:

```scala
import org.apache.spark.sql.SQLContext        <1>

import org.elasticsearch.spark.sql._          <2>
...

val sql = new SQLContext(sc)

val people = sql.esDF("spark/people")         <3>

// check the associated schema
println(people.schema.treeString)             <4>
// root
//  |-- name: string (nullable = true)
//  |-- surname: string (nullable = true)
//  |-- age: long (nullable = true)           <5>
```

1. Spark SQL Scala imports
2. elasticsearch-hadoop SQL Scala imports
3. create a `DataFrame` backed by the `spark/people` index in {es}
4. the `DataFrame` associated schema discovered from {es}
5. notice how the `age` field was transformed into a `Long` when using the default {{es}} mapping as discussed in the [*Mapping and Types*](/reference/mapping-types.md) chapter.


And just as with the Spark *core* support, additional parameters can be specified such as a query. This is quite a *powerful* concept as one can filter the data at the source ({{es}}) and use Spark only on the results:

```scala
// get only the Smiths
val smiths = sqlContext.esDF("spark/people","?q=Smith") <1>
```

1. {{es}} query whose results comprise the `DataFrame`


In some cases, especially when the index in {{es}} contains a lot of fields, it is desireable to create a `DataFrame` that contains only a *subset* of them. While one can modify the `DataFrame` (by working on its backing `RDD`) through the official Spark API or through dedicated queries, elasticsearch-hadoop allows the user to specify what fields to include and exclude from {{es}} when creating the `DataFrame`.

Through `es.read.field.include` and `es.read.field.exclude` properties, one can indicate what fields to include or exclude from the index mapping. The syntax is similar to that of {{es}} [include/exclude](elasticsearch://reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering). Multiple values can be specified by using a comma. By default, no value is specified meaning all properties/fields are included and no properties/fields are excluded. Note that these properties can include leading and trailing wildcards. Including part of a hierarchy of fields without a trailing wildcard does not imply that the entire hierarcy is included. However in most cases it does not make sense to include only part of a hierarchy, so a trailing wildcard should be included.

For example:

```ini
# include
es.read.field.include = *name, address.*
# exclude
es.read.field.exclude = *.created
```

::::{important}
Due to the way SparkSQL works with a `DataFrame` schema, elasticsearch-hadoop needs to be aware of what fields are returned from {{es}} *before* executing the actual queries. While one can restrict the fields manually through the underlying {{es}} query, elasticsearch-hadoop is unaware of this and the results are likely to be different or worse, errors will occur. Use the properties above instead, which {{es}} will properly use alongside the user query.
::::



##### Java [spark-sql-read-java]

For Java users, a dedicated API exists through `JavaEsSpark SQL`. It is strikingly similar to `EsSpark SQL` however it allows configuration options to be passed in through Java collections instead of Scala ones; other than that using the two is exactly the same:

```java
import org.apache.spark.sql.api.java.JavaSQLContext;          <1>
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;   <2>
...
SQLContext sql = new SQLContext(sc);

DataFrame people = JavaEsSparkSQL.esDF(sql, "spark/people");  <3>
```

1. Spark SQL import
2. elasticsearch-hadoop import
3. create a Java `DataFrame` backed by an {{es}} index


Better yet, the `DataFrame` can be backed by a query result:

```java
DataFrame people = JavaEsSparkSQL.esDF(sql, "spark/people", "?q=Smith"); <1>
```

1. {{es}} query backing the elasticsearch-hadoop `DataFrame`



#### Spark SQL Type conversion [spark-sql-type-conversion]

::::{important}
When dealing with multi-value/array fields, please see [this](/reference/mapping-types.md#mapping-multi-values) section and in particular [these](/reference/configuration.md#cfg-field-info) configuration options. IMPORTANT: If automatic index creation is used, please review [this](/reference/mapping-types.md#auto-mapping-type-loss) section for more information.
::::


elasticsearch-hadoop automatically converts Spark built-in types to {{es}} [types](elasticsearch://reference/elasticsearch/mapping-reference/field-data-types.md) (and back) as shown in the table below:

While Spark SQL [`DataType`s](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types) have an equivalent in both Scala and Java and thus the [RDD](#spark-type-conversion) conversion can apply, there are slightly different semantics - in particular with the `java.sql` types due to the way Spark SQL handles them:

| Spark SQL `DataType` | {{es}} type |
| --- | --- |
| `null` | `null` |
| `ByteType` | `byte` |
| `ShortType` | `short` |
| `IntegerType` | `int` |
| `LongType` | `long` |
| `FloatType` | `float` |
| `DoubleType` | `double` |
| `StringType` | `string` |
| `BinaryType` | `string` (BASE64) |
| `BooleanType` | `boolean` |
| `DateType` | `date` (`string` format) |
| `TimestampType` | `long` (unix time) |
| `ArrayType` | `array` |
| `MapType` | `object` |
| `StructType` | `object` |

In addition to the table above, for Spark SQL 1.3 or higher, elasticsearch-hadoop performs automatic schema detection for geo types, namely {{es}} `geo_point` and `geo_shape`. Since each type allows multiple formats (`geo_point` accepts latitude and longitude to be specified in 4 different ways, while `geo_shape` allows a variety of types (currently 9)) and the mapping does not provide such information, elasticsearch-hadoop will *sample* the determined geo fields at startup and retrieve an arbitrary document that contains all the relevant fields; it will parse it and thus determine the necessary schema (so for example it can tell whether a `geo_point` is specified as a `StringType` or as an `ArrayType`).

::::{important}
Since Spark SQL is strongly-typed, each geo field needs to have the same format across *all* documents. Shy of that, the returned data will not fit the detected schema and thus lead to errors.
::::



## Spark Structured Streaming support [spark-sql-streaming]

::::{note}
Added in 6.0.
::::


[TBC: FANCY QUOTE]
Released as an experimental feature in Spark 2.0, Spark Structured Streaming provides a unified streaming and batch interface built into the Spark SQL integration. As of elasticsearch-hadoop 6.0, we provide native functionality to index streaming data into {{es}}.

::::{important}
Like Spark SQL, Structured Streaming works with *structured* data. All entries are expected to have the *same* structure (same number of fields, of the same type and name). Using unstructured data (documents with different structures) is *not* supported and will cause problems. For such cases, use `DStream`s.
::::



#### Supported Spark Structured Streaming versions [spark-sql-streaming-versions]

Spark Structured Streaming is considered *generally available* as of Spark v2.2.0. As such, elasticsearch-hadoop support for Structured Streaming (available in elasticsearch-hadoop 6.0+) is only compatible with Spark versions 2.2.0 and onward. Similar to Spark SQL before it, Structured Streaming may be subject to significant changes between releases before its interfaces are considered *stable*.

Spark Structured Streaming support is available under the `org.elasticsearch.spark.sql` and `org.elasticsearch.spark.sql.streaming` packages. It shares a unified interface with Spark SQL in the form of the `Dataset[_]` api. Clients can interact with streaming `Dataset`s in almost exactly the same way as regular batch `Dataset`s with only a [few exceptions](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations).


#### Writing Streaming `Datasets` (Spark SQL 2.0+) to {{es}} [spark-sql-streaming-write]

With elasticsearch-hadoop, Stream-backed `Dataset`s can be indexed to {{es}}.


##### Scala [spark-sql-streaming-write-scala]

In Scala, to save your streaming based `Dataset`s and `DataFrame`s to Elasticsearch, simply configure the stream to write out using the `"es"` format, like so:

```scala
import org.apache.spark.sql.SparkSession    <1>

...

val spark = SparkSession.builder()
   .appName("EsStreamingExample")           <2>
   .getOrCreate()

// case class used to define the DataFrame
case class Person(name: String, surname: String, age: Int)

//  create DataFrame
val people = spark.readStream                   <3>
        .textFile("/path/to/people/files/*")    <4>
        .map(_.split(","))
        .map(p => Person(p(0), p(1), p(2).trim.toInt))

people.writeStream
      .option("checkpointLocation", "/save/location")      <5>
      .format("es")
      .start("spark/people")                               <6>
```

1. Spark SQL import
2. Create `SparkSession`
3. Instead of calling `read`, call `readStream` to get instance of `DataStreamReader`
4. Read a directory of text files continuously and convert them into `Person` objects
5. Provide a location to save the offsets and commit logs for the streaming query
6. Start the stream using the `"es"` format to index the contents of the `Dataset` continuously to {es}


::::{warning}
Spark makes no type-based differentiation between batch and streaming based `Dataset`s. While you may be able to import the `org.elasticsearch.spark.sql` package to add `saveToEs` methods to your `Dataset` or `DataFrame`, it will throw an illegal argument exception if those methods are called on streaming based `Dataset`s or `DataFrame`s.
::::



##### Java [spark-sql-streaming-write-java]

In a similar fashion, the `"es"` format is available for Java usage as well:

```java
import org.apache.spark.sql.SparkSession    <1>

...

SparkSession spark = SparkSession
  .builder()
  .appName("JavaStructuredNetworkWordCount")     <2>
  .getOrCreate();

// java bean style class
public static class PersonBean {
  private String name;
  private String surname;                        <3>
  private int age;

  ...
}

Dataset<PersonBean> people = spark.readStream()         <4>
        .textFile("/path/to/people/files/*")
        .map(new MapFunction<String, PersonBean>() {
            @Override
            public PersonBean call(String value) throws Exception {
                return someFunctionThatParsesStringToJavaBeans(value.split(","));         <5>
            }
        }, Encoders.<PersonBean>bean(PersonBean.class));

people.writeStream()
    .option("checkpointLocation", "/save/location")       <6>
    .format("es")
    .start("spark/people");                               <7>
```

1. Spark SQL Java imports. Can use the same session class as Scala
2. Create SparkSession. Can also use the legacy `SQLContext` api
3. We create a java bean class to be used as our data format
4. Use the `readStream()` method to get a `DataStreamReader` to begin building our stream
5. Convert our string data into our PersonBean
6. Set a place to save the state of our stream
7. Using the `"es"` format, we continuously index the `Dataset` in {{es}} under `spark/people`



#### Writing existing JSON to {{es}} [spark-sql-streaming-json]

When using Spark SQL, if the input data is in JSON format, simply convert it to a `Dataset` (for Spark SQL 2.0) (as described in Spark [documentation](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources)) through the `DataStreamReader’s `json` format.


#### Sink commit log in Spark Structured Streaming [spark-sql-streaming-commit-log]

Spark Structured Streaming advertises an end-to-end fault-tolerant exactly-once processing model that is made possible through the usage of offset checkpoints and maintaining commit logs for each streaming query. When executing a streaming query, most sources and sinks require you to specify a "checkpointLocation" in order to persist the state of your job. In the event of an interruption, launching a new streaming query with the same checkpoint location will recover the state of the job and pick up where it left off. We maintain a commit log for elasticsearch-hadoop’s {{es}} sink implementation in a special directory under the configured checkpoint location:

```sh
$> ls /path/to/checkpoint/location
metadata  offsets/  sinks/
$> ls /path/to/checkpoint/location/sinks
elasticsearch/
$> ls /path/to/checkpoint/location/sinks/elasticsearch
12.compact  13  14  15 16  17  18
```

Each file in the commit log directory corresponds to a batch id that has been committed. The log implementation periodically compacts the logs down to avoid clutter. You can set the location for the log directory a number of ways:

1. Set the explicit log location with `es.spark.sql.streaming.sink.log.path` (see below).
2. If that is not set, then the path specified by `checkpointLocation` will be used.
3. If that is not set, then a path will be constructed by combining the value of `spark.sql.streaming.checkpointLocation` from the SparkSession with the `Dataset’s given query name.
4. If no query name is present, then a random UUID will be used in the above case instead of the query name
5. If none of the above settings are provided then the `start` call will throw an exception

Here is a list of configurations that affect the behavior of {{es}}'s commit log:

`es.spark.sql.streaming.sink.log.enabled` (default `true`)
:   Enables or disables the commit log for a streaming job. By default, the log is enabled, and output batches with the same batch id will be skipped to avoid double-writes. When this is set to `false`, the commit log is disabled, and all outputs will be sent to {{es}}, regardless if they have been sent in a previous execution.

`es.spark.sql.streaming.sink.log.path`
:   Sets the location to store the log data for this streaming query. If this value is not set, then the {{es}} sink will store its commit logs under the path given in `checkpointLocation`. Any HDFS Client compatible URI is acceptable.

`es.spark.sql.streaming.sink.log.cleanupDelay` (default `10m`)
:   The commit log is managed through Spark’s HDFS Client. Some HDFS compatible filesystems (like Amazon’s S3) propagate file changes in an asynchronous manner. To get around this, after a set of log files have been compacted, the client will wait for this amount of time before cleaning up the old files.

`es.spark.sql.streaming.sink.log.deletion` (default `true`)
:   Determines if the log should delete old logs that are no longer needed. After every batch is committed, the client will check to see if there are any commit logs that have been compacted and are safe to be removed. If set to `false`, the log will skip this cleanup step, leaving behind a commit file for each batch.

`es.spark.sql.streaming.sink.log.compactInterval` (default `10`)
:   Sets the number of batches to process before compacting the log files. By default, every 10 batches the commit log will be compacted down into a single file that contains all previously committed batch ids.


#### Spark Structured Streaming Type conversion [spark-sql-streaming-type-conversion]

Structured Streaming uses the exact same type conversion rules as the Spark SQL integration.

::::{important}
When dealing with multi-value/array fields, please see [this](/reference/mapping-types.md#mapping-multi-values) section and in particular [these](/reference/configuration.md#cfg-field-info) configuration options.
::::


::::{important}
If automatic index creation is used, please review [this](/reference/mapping-types.md#auto-mapping-type-loss) section for more information.
::::


elasticsearch-hadoop automatically converts Spark built-in types to {{es}} [types](elasticsearch://reference/elasticsearch/mapping-reference/field-data-types.md) as shown in the table below:

While Spark SQL [`DataType`s](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types) have an equivalent in both Scala and Java and thus the [RDD](#spark-type-conversion) conversion can apply, there are slightly different semantics - in particular with the `java.sql` types due to the way Spark SQL handles them:

| Spark SQL `DataType` | {{es}} type |
| --- | --- |
| `null` | `null` |
| `ByteType` | `byte` |
| `ShortType` | `short` |
| `IntegerType` | `int` |
| `LongType` | `long` |
| `FloatType` | `float` |
| `DoubleType` | `double` |
| `StringType` | `string` |
| `BinaryType` | `string` (BASE64) |
| `BooleanType` | `boolean` |
| `DateType` | `date` (`string` format) |
| `TimestampType` | `long` (unix time) |
| `ArrayType` | `array` |
| `MapType` | `object` |
| `StructType` | `object` |


### Using the Map/Reduce layer [spark-mr]

Another way of using Spark with {{es}} is through the Map/Reduce layer, that is by leveraging the dedicated `Input/OuputFormat` in elasticsearch-hadoop. However, unless one is stuck on elasticsearch-hadoop 2.0, we *strongly* recommend using the native integration as it offers significantly better performance and flexibility.


#### Configuration [_configuration_2]

Through elasticsearch-hadoop, Spark can integrate with {{es}} through its dedicated `InputFormat`, and in case of writing, through `OutputFormat`. These are described at length in the [Map/Reduce](/reference/mapreduce-integration.md) chapter so please refer to that for an in-depth explanation.

In short, one needs to setup a basic Hadoop `Configuration` object with the target {{es}} cluster and index, potentially a query, and she’s good to go.

From Spark’s perspective, the only thing required is setting up serialization - Spark relies by default on Java serialization which is convenient but fairly inefficient. This is the reason why Hadoop itself introduced its own serialization mechanism and its own types - namely `Writable`s. As such, `InputFormat` and `OutputFormat`s are required to return `Writables` which, out of the box, Spark does not understand. The good news is, one can easily enable a different serialization ([Kryo](https://github.com/EsotericSoftware/kryo)) which handles the conversion automatically and also does this quite efficiently.

```java
SparkConf sc = new SparkConf(); //.setMaster("local");
sc.set("spark.serializer", KryoSerializer.class.getName()); <1>

// needed only when using the Java API
JavaSparkContext jsc = new JavaSparkContext(sc);
```

1. Enable the Kryo serialization support with Spark


Or if you prefer Scala

```scala
val sc = new SparkConf(...)
sc.set("spark.serializer", classOf[KryoSerializer].getName) <1>
```

1. Enable the Kryo serialization support with Spark


Note that the Kryo serialization is used as a work-around for dealing with `Writable` types; one can choose to convert the types directly (from `Writable` to `Serializable` types) - which is fine however for getting started, the one liner above seems to be the most effective.


#### Reading data from {{es}} [_reading_data_from_es_2]

To read data, simply pass in the `org.elasticsearch.hadoop.mr.EsInputFormat` class - since it supports both the `old` and the `new` Map/Reduce APIs, you are free to use either method on `SparkContext’s, `hadoopRDD` (which we recommend for conciseness reasons) or `newAPIHadoopRDD`. Which ever you chose, stick with it to avoid confusion and problems down the road.


##### *Old* (`org.apache.hadoop.mapred`) API [_old_org_apache_hadoop_mapred_api_3]

```java
JobConf conf = new JobConf();                             <1>
conf.set("es.resource", "radio/artists");                 <2>
conf.set("es.query", "?q=me*");                           <3>

JavaPairRDD esRDD = jsc.hadoopRDD(conf, EsInputFormat.class,
                          Text.class, MapWritable.class); <4>
long docCount = esRDD.count();
```

1. Create the Hadoop object (use the old API)
2. Configure the source (index)
3. Setup the query (optional)
4. Create a Spark `RDD` on top of {{es}} through `EsInputFormat` - the key represents the doc id, the value the doc itself


The Scala version is below:

```scala
val conf = new JobConf()                                   <1>
conf.set("es.resource", "radio/artists")                   <2>
conf.set("es.query", "?q=me*")                             <3>
val esRDD = sc.hadoopRDD(conf,
                classOf[EsInputFormat[Text, MapWritable]], <4>
                classOf[Text], classOf[MapWritable]))
val docCount = esRDD.count();
```

1. Create the Hadoop object (use the old API)
2. Configure the source (index)
3. Setup the query (optional)
4. Create a Spark `RDD` on top of {{es}} through `EsInputFormat`



##### *New* (`org.apache.hadoop.mapreduce`) API [_new_org_apache_hadoop_mapreduce_api_3]

As expected, the `mapreduce` API version is strikingly similar - replace `hadoopRDD` with `newAPIHadoopRDD` and `JobConf` with `Configuration`. That’s about it.

```java
Configuration conf = new Configuration();       <1>
conf.set("es.resource", "radio/artists");       <2>
conf.set("es.query", "?q=me*");                 <3>

JavaPairRDD esRDD = jsc.newAPIHadoopRDD(conf, EsInputFormat.class,
                Text.class, MapWritable.class); <4>
long docCount = esRDD.count();
```

1. Create the Hadoop object (use the new API)
2. Configure the source (index)
3. Setup the query (optional)
4. Create a Spark `RDD` on top of {{es}} through `EsInputFormat` - the key represent the doc id, the value the doc itself


The Scala version is below:

```scala
val conf = new Configuration()                             <1>
conf.set("es.resource", "radio/artists")                   <2>
conf.set("es.query", "?q=me*")                             <3>
val esRDD = sc.newAPIHadoopRDD(conf,
                classOf[EsInputFormat[Text, MapWritable]], <4>
                classOf[Text], classOf[MapWritable]))
val docCount = esRDD.count();
```

1. Create the Hadoop object (use the new API)
2. Configure the source (index)
3. Setup the query (optional)
4. Create a Spark `RDD` on top of {{es}} through `EsInputFormat`



## Using the connector from PySpark [spark-python]

Thanks to its [Map/Reduce](/reference/mapreduce-integration.md) layer, elasticsearch-hadoop can be used from PySpark as well to both read and write data to {{es}}. To wit, below is a snippet from the [Spark documentation](https://spark.apache.org/docs/1.5.1/programming-guide.md#external-datasets) (make sure to switch to the Python snippet):

```python
$ ./bin/pyspark --driver-class-path=/path/to/elasticsearch-hadoop.jar
>>> conf = {"es.resource" : "index/type"}   # assume Elasticsearch is running on localhost defaults
>>> rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
>>> rdd.first()         # the result is a MapWritable that is converted to a Python dict
(u'Elasticsearch ID',
 {u'field1': True,
  u'field2': u'Some Text',
  u'field3': 12345})
```

Also, the SQL loader can be used as well:

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.format("org.elasticsearch.spark.sql").load("index/type")
df.printSchema()
```


