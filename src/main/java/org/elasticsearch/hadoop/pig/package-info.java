/**
 * Package providing integration between ElasticSearch and
 * <a href="http://pig.apache.org/">Apache Pig</a>.
 *
 * <p/>Provides REST-based, Pig Loader and Storage for reading and writing
 * data to and from ElasticSearch.
 *
 * <p/>
 *
 * Defaults and settings can be configured through Hadoop/Pig properties.
 *
 * <br/>
 * These are:
 * <pre>
 * 	es.host - default ES host, used by the Map/Reduce Input/OutputFormat and Pig loader/storage (when not specified)
 *  es.port - default ES port, used by the Map/Reduce Input/OutputFormat and Pig loader/storage (when not specified)
 * </pre>
 *
 */
package org.elasticsearch.hadoop.pig;
