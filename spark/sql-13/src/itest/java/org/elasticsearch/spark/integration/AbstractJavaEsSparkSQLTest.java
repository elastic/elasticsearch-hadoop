/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.integration;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.TestData;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.collect.ImmutableMap;

import static org.elasticsearch.hadoop.util.TestUtils.docEndpoint;
import static org.elasticsearch.hadoop.util.TestUtils.resource;
import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;

import static org.hamcrest.Matchers.*;

import static scala.collection.JavaConversions.*;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractJavaEsSparkSQLTest implements Serializable {

	private static final transient SparkConf conf = new SparkConf()
			.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS))
			.set("spark.io.compression.codec", "lz4")
			.setMaster("local").setAppName("estest");
	
	private static transient JavaSparkContext sc = null;
	private static transient SQLContext sqc = null;
	private EsMajorVersion version = TestUtils.getEsClusterInfo().getMajorVersion();

	@ClassRule
	public static TestData testData = new TestData();

	@BeforeClass
	public static void setup() {
		sc = new JavaSparkContext(conf);
		sqc = new SQLContext(sc);
	}

	@AfterClass
	public static void clean() throws Exception {
		if (sc != null) {
			sc.stop();
			// wait for jetty & spark to properly shutdown
			Thread.sleep(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	public void testBasicRead() throws Exception {
		DataFrame dataFrame = artistsAsDataFrame();
		assertTrue(dataFrame.count() > 300);
		dataFrame.registerTempTable("datfile");
		System.out.println(dataFrame.schema().toString());
		assertEquals(5, dataFrame.take(5).length);
		DataFrame results = sqc
				.sql("SELECT name FROM datfile WHERE id >=1 AND id <=10");
		assertEquals(10, results.take(10).length);
		assertEquals(10L, results.count());
	}

	@Test
	public void testEsdataFrame1Write() throws Exception {
		DataFrame dataFrame = artistsAsDataFrame();

		String target = resource("sparksql-test-scala-basic-write", "data", version);
		JavaEsSparkSQL.saveToEs(dataFrame, target);
		assertTrue(RestUtils.exists(target));
		assertThat(RestUtils.get(target + "/_search?"), containsString("345"));
	}

	@Test
	public void testEsdataFrame1WriteWithId() throws Exception {
		DataFrame dataFrame = artistsAsDataFrame();

		String target = resource("sparksql-test-scala-basic-write-id-mapping", "data", version);
		String docEndpoint = docEndpoint("sparksql-test-scala-basic-write-id-mapping", "data", version);

		JavaEsSparkSQL.saveToEs(dataFrame, target,
				ImmutableMap.of(ES_MAPPING_ID, "id"));
		assertTrue(RestUtils.exists(target));
		assertThat(RestUtils.get(target + "/_search?"), containsString("345"));
		assertThat(RestUtils.exists(docEndpoint + "/1"), is(true));
	}

    @Test
    public void testEsSchemaRDD1WriteWithMappingExclude() throws Exception {
    	DataFrame dataFrame = artistsAsDataFrame();

        String target = resource("sparksql-test-scala-basic-write-exclude-mapping", "data", version);
        JavaEsSparkSQL.saveToEs(dataFrame, target,ImmutableMap.of(ES_MAPPING_EXCLUDE, "url"));
        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), not(containsString("url")));
    }
    
	@Test
	public void testEsdataFrame2Read() throws Exception {
		String target = resource("sparksql-test-scala-basic-write", "data", version);

        // DataFrame dataFrame = JavaEsSparkSQL.esDF(sqc, target);
        DataFrame dataFrame = sqc.read().format("es").load(target);
		assertTrue(dataFrame.count() > 300);
		String schema = dataFrame.schema().treeString();
		System.out.println(schema);
		assertTrue(schema.contains("id: long"));
		assertTrue(schema.contains("name: string"));
		assertTrue(schema.contains("pictures: string"));
		assertTrue(schema.contains("time: long"));
		assertTrue(schema.contains("url: string"));

		// dataFrame.take(5).foreach(println)

		dataFrame.registerTempTable("basicRead");
		DataFrame nameRDD = sqc
				.sql("SELECT name FROM basicRead WHERE id >= 1 AND id <=10");
		assertEquals(10, nameRDD.count());
	}

	@Test
	public void testEsDataFrameReadMetadata() throws Exception {
		DataFrame artists = artistsAsDataFrame();
		String target = resource("sparksql-test-scala-dataframe-read-metadata", "data", version);
		JavaEsSparkSQL.saveToEs(artists, target);

		DataFrame dataframe = sqc.read().format("es").option("es.read.metadata", "true").load(target).where("id = 1");

		// Since _metadata field isn't a part of _source,
		// we want to check that it could be fetched in any position.
		assertEquals("sparksql-test-scala-dataframe-read-metadata", dataframe.selectExpr("_metadata['_index']").takeAsList(1).get(0).get(0));
		assertEquals("sparksql-test-scala-dataframe-read-metadata", dataframe.selectExpr("_metadata['_index']", "name").takeAsList(1).get(0).get(0));
		assertEquals("MALICE MIZER", dataframe.selectExpr("_metadata['_index']", "name").takeAsList(1).get(0).get(1));
		assertEquals("MALICE MIZER", dataframe.selectExpr("name", "_metadata['_index']").takeAsList(1).get(0).get(0));
		assertEquals("sparksql-test-scala-dataframe-read-metadata", dataframe.selectExpr("name", "_metadata['_index']").takeAsList(1).get(0).get(1));
	}

	private DataFrame artistsAsDataFrame() throws IOException {
		String input = testData.sampleArtistsDatUri().toString();
		JavaRDD<String> data = sc.textFile(input);

		StructType schema = DataTypes
				.createStructType(new StructField[] {
						DataTypes.createStructField("id", DataTypes.IntegerType, false),
						DataTypes.createStructField("name", DataTypes.StringType, false),
						DataTypes.createStructField("url", DataTypes.StringType, true),
						DataTypes.createStructField("pictures", DataTypes.StringType, true),
						DataTypes.createStructField("time", DataTypes.TimestampType, true) });

		JavaRDD<Row> rowData = data.map(new Function<String, String[]>() {
			@Override
			public String[] call(String line) throws Exception {
				return line.split("\t");
			}
		}).map(new Function<String[], Row>() {
			@Override
			public Row call(String[] r) throws Exception {
				return RowFactory.create(Integer.parseInt(r[0]), r[1], r[2], r[3],
						new Timestamp(DatatypeConverter.parseDateTime(r[4]).getTimeInMillis()));
			}
		});

		return sqc.createDataFrame(rowData, schema);
	}
}
