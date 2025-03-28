---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/mapping.html
navigation_title: Mapping and types
---
# Mapping and types in {{esh-full}}

As explained in the previous sections, elasticsearch-hadoop integrates closely with the Hadoop ecosystem and performs close introspection of the type information so that the data flow between {{es}} and Hadoop is as transparent as possible. This section takes a closer look at how the type conversion takes place and how data is mapped between the two systems.


## Converting data to {{es}} [_converting_data_to_es]

By design, elasticsearch-hadoop provides no data transformation or mapping layer itself simply because there is no need for them: Hadoop is designed to do ETL and some libraries (like Pig and Hive) provide type information themselves. Furthermore, {{es}} has rich support for mapping out of the box including automatic detection,  dynamic/schema-less mapping, templates and full manual control. Need to split strings into token, do data validation or eliminate unneeded data? There are plenty of ways to do that in Hadoop before reading/writing data from/to {{es}}. Need control over how data is stored in {{es}}? Use {{es}} APIs to define the  [mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping), to update [settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) or add generic [meta-data](elasticsearch://reference/elasticsearch/mapping-reference/document-metadata-fields.md).


## Time/Date mapping [mapping-date]

When it comes to handling dates, {{es}} always uses the [ISO 8601](http://en.wikipedia.org/wiki/ISO_8601) format for date/time. This is the default date format of {{es}} - if a custom one is needed, please **add** it to the default option rather then just replacing it. See the [date format](elasticsearch://reference/elasticsearch/mapping-reference/mapping-date-format.md) section in {{es}} reference documentation for more information. Note that when reading data, if the date is not in ISO8601 format, by default elasticsearch-hadoop will likely not understand it as it does not replicate the elaborate date parsing in {{es}}. In these cases one can simply disable the date conversion and pass the raw information as a `long` or `String`, through the `es.mapping.date.rich` [property](/reference/configuration.md#cfg-field-info).

As a side note, elasticsearch-hadoop tries to detect whether dedicated date parsing libraries (in particular Joda, used also by {{es}}) are available at runtime and if so, will use them. If not, it will default to parsing using JDK classes which are not as rich. Going forward especially with the advent of JDK 8, elasticsearch-hadoop will try to migrate to `javax.time` library to have the same behaviour regardless of the classpath available at runtime.


## Ordering and Mapping [mapping-arrays]

It is important to note that JSON objects (delimited by `{}` and typically associated with maps) are **unordered**, in other words, they do **not** maintain order. JSON arrays (typically associated with lists or sequences) are **ordered**, that is, they **do** preserve the initial insertion order. This impacts the way objects are read from {{es}} as one might find the insertion structure to be different than the extraction one. It is however easy to circumvent this problem - as JSON objects (maps) contain fields, use the field names (or keys) instead of their position inside the document to reliably get their values (in Java terms think of a JSON object as a `HashMap` as oppose to a `LinkedHashMap`).


## Geo types [mapping-geo]

For geolocation {{es}} provides two dedicated types, namely [`geo_point`](elasticsearch://reference/elasticsearch/mapping-reference/geo-point.md) and [`geo_shape`](elasticsearch://reference/elasticsearch/mapping-reference/geo-shape.md) which do not have a direct equivalent in any of the libraries elasticsearch-hadoop integrates with. Further more, {{es}} accepts multiple formats for each type (as there are different ways to represent data), in fact there are 4 different representations for `geo_point` and 9 for `geo_shape`. To go around this, the connector breaks down the geo types into primitives depending on the actual format used for their respective types.

For strongly-typed libraries (like SparkSQL `DataFrame`s), the format needs to be known before hand and thus, elasticsearch-hadoop will *sample* the data asking elasticsearch-hadoop for one random document that is representative of the mapping, parse it and based on the values found, identify the format used and create the necessary schema. This happens automatically at start-up without any user interference. As always, the user data must all be using the *same* format (a requirement from SparkSQL) otherwise reading a different format will trigger an exception.

Note that typically handling of these types poses no issues for the user whether reading or writing data.


## Handling array/multi-values fields [mapping-multi-values]

{{es}} treats fields with single or multi-values the same; in fact, the mapping provides no information about this. As a client, it means one cannot tell whether a field is single-valued or not until is actually being read. In most cases this is not an issue and elasticsearch-hadoop automatically creates the necessary list/array on the fly. However in environments with strict schema such as Spark SQL, changing a field actual value from its declared type is not allowed. Worse yet, this information needs to be available even before reading the data. Since the mapping is not conclusive enough, elasticsearch-hadoop allows the user to specify the extra information through [field information](/reference/configuration.md#cfg-field-info), specifically `es.read.field.as.array.include` and `es.read.field.as.array.exclude`.


## Automatic mapping [_automatic_mapping]

By default, {{es}} provides [automatic index and mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) when data is added under an index that has not been created before. In other words, data can be added into {{es}} without the index and the mappings being defined a priori. This is quite convenient since {{es}} automatically adapts to the data being fed to it - moreover, if certain entries have extra fields, {{es}} schema-less nature allows them to be indexed without any issues.

$$$auto-mapping-type-loss$$$
It is important to remember that automatic mapping uses the payload values to identify the [field types](elasticsearch://reference/elasticsearch/mapping-reference/field-data-types.md), using the **first document** that adds each field. elasticsearch-hadoop communicates with {{es}} through JSON which does not provide any type information, rather only the field names and their values. One can think of it as *type erasure* or information loss; for example JSON does not differentiate integer numeric types - `byte`, `short`, `int`, `long` are all placed in the same `long` *bucket*. this can have unexpected side-effects since the type information is *guessed* such as:


#### numbers mapped only as `long`/`double` [_numbers_mapped_only_as_longdouble]

Whenever {{es}} encounters a number, it will allocate the largest type for it since it does not know the exact number type of the field. Allocating a small type (such as `byte`, `int` or `float`) can lead to problems if a future document is larger, so {{es}} uses a safe default. For example, the document:

```json
{
    "tweet" {
        "user" : "kimchy",
        "message" : "This is a tweet!",
        "postDate" : "2009-11-15T14:12:12",
        "priority" : 4,
        "rank" : 12.3
    }
}
```

triggers the following mapping:

```json
{ "test" : {
    "mappings" : {
      "index" : {
        "properties" : {
          "message" : {
            "type" : "string"
          },
          "postDate" : {
            "type" : "date",
            "format" : "dateOptionalTime"      <1>
          },
          "priority" : {
            "type" : "long"                    <2>
          },
          "rank" : {
            "type" : "double"                  <3>
          },
          "user" : {
            "type" : "string"
          }
        }
      }
    }
  }
}
```

1. The *postDate* field was recognized as a date in ISO 8601 format (`dateOptionalTime`)
2. The *integer* number (`4`) was mapped to the largest available type (`long`)
3. The *fractional* number (`12.3`) was mapped to the largest available type (`double`)



#### incorrect mapping [_incorrect_mapping]

This happens when a string field contains numbers (say `1234`) - {{es}} has no information that the number is actually a string and thus it map the field as a number, causing a parsing exception when a string is encountered. For example, this document:

```json
{ "array":[123, "string"] }
```

causes an exception with automatic mapping:

```json
{"error":"MapperParsingException[failed to parse [array]]; nested: NumberFormatException[For input string: \"string\"]; ","status":400}
```

because the field `array` is initially detected as a number (because of `123`) which causes `"string"` to trigger the parsing exception since clearly it is not a number. The same issue tends to occur with strings might be interpreted as dates.

Hence if the defaults need to be overridden and/or if you experience the problems exposed above, potentially due to a diverse dataset, consider using [Explicit mapping](#explicit-mapping).


### Disabling automatic mapping [_disabling_automatic_mapping]

{{es}} allows *automatic index creation* as well as *dynamic mapping* (for extra fields present in documents) to be disabled through the `action.auto_create_index` and `index.mapper.dynamic` settings on the nodes config files. As a safety net, elasticsearch-hadoop provides a dedicated configuration [option](/reference/configuration.md#configuration-options-index) `es.index.auto.create` which allows elasticsearch-hadoop to either create the index or not without having to modify the {{es}} cluster options.


## Explicit mapping [explicit-mapping]

Explicit or manual mapping should be considered when the defaults need to be overridden, if the data is detected incorrectly (as explained above) or, in most cases, to customize the index analysis. Refer to {{es}} [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) and [mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) documentation on how to define an index and its types - note that these need to be present **before** data is being uploaded to {{es}} (otherwise automatic mapping will be used by {{es}}, if enabled).

::::{tip}
In most cases, [templates](docs-content://manage-data/data-store/templates.md) are quite handy as they are automatically applied to new indices created that match the pattern; in other words instead of defining the mapping per index, one can just define the template once and then have it applied to all indices that match its pattern.
::::



## Limitations [limitations]

{{es}} allows field names to contain dots (*.*). But {{esh}} does not support them, and fails when reading or writing fields with dots. Refer to {{es}} [Dot Expander Processor](elasticsearch://reference/enrich-processor/dot-expand-processor.md) for tooling to assist replacing dots in field names.

