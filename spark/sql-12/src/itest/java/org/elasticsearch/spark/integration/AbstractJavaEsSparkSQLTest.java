package org.elasticsearch.spark.integration;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;

import static org.hamcrest.Matchers.*;

import static scala.collection.JavaConversions.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractJavaEsSparkSQLTest implements Serializable {

    private static final transient SparkConf conf = new SparkConf()
            .setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS))
            .setMaster("local").setAppName("estest")
            .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=256m");

    private static transient JavaSparkContext sc = null;
    private static transient JavaSQLContext sqc = null;

    @BeforeClass
    public static void setup() {
        sc = new JavaSparkContext(conf);
        sqc = new JavaSQLContext(sc);
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
        JavaSchemaRDD schemaRDD = artistsAsSchemaRDD();
        assertTrue(schemaRDD.count() > 300);
        schemaRDD.registerTempTable("datfile");
        System.out.println(schemaRDD.schemaString());
        assertEquals(5, schemaRDD.take(5).size());
        JavaSchemaRDD results = sqc
                .sql("SELECT name FROM datfile WHERE id >=1 AND id <=10");
        assertEquals(10, schemaRDD.take(10).size());
    }

    @Test
    public void testEsSchemaRDD1Write() throws Exception {
        JavaSchemaRDD schemaRDD = artistsAsSchemaRDD();

        String target = "sparksql-test/scala-basic-write";
        JavaEsSparkSQL.saveToEs(schemaRDD, target);
        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), containsString("345"));
    }

    @Test
    public void testEsSchemaRDD1WriteWithId() throws Exception {
        JavaSchemaRDD schemaRDD = artistsAsSchemaRDD();

        String target = "sparksql-test/scala-basic-write-id-mapping";
        JavaEsSparkSQL.saveToEs(schemaRDD, target, ImmutableMap.of(ES_MAPPING_ID, "id"));
        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), containsString("345"));
        assertThat(RestUtils.exists(target + "/1"), is(true));
    }

    @Test
    public void testEsSchemaRDD1WriteWithMappingExclude() throws Exception {
        JavaSchemaRDD schemaRDD = artistsAsSchemaRDD();

        String target = "sparksql-test/scala-basic-write-exclude-mapping";
        JavaEsSparkSQL.saveToEs(schemaRDD, target,ImmutableMap.of(ES_MAPPING_EXCLUDE, "url"));
        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), not(containsString("url")));
    }

    @Test
    public void testEsSchemaRDD2Read() throws Exception {
        String target = "sparksql-test/scala-basic-write";

        JavaSchemaRDD schemaRDD = JavaEsSparkSQL.esRDD(sqc, target);
        assertTrue(schemaRDD.count() > 300);
        String schema = schemaRDD.schemaString();
        assertTrue(schema.contains("id: long"));
        assertTrue(schema.contains("name: string"));
        assertTrue(schema.contains("pictures: string"));
        assertTrue(schema.contains("time: long"));
        assertTrue(schema.contains("url: string"));

        // schemaRDD.take(5).foreach(println)

        schemaRDD.registerTempTable("basicRead");
        JavaSchemaRDD nameRDD = sqc.sql("SELECT name FROM basicRead WHERE id >= 1 AND id <=10");
        assertEquals(10, nameRDD.count());

    }

    private JavaSchemaRDD artistsAsSchemaRDD() {
        String input = TestUtils.sampleArtistsDat();
        JavaRDD<String> data = sc.textFile(input);

        StructType schema = DataType
                .createStructType(new StructField[] {
                        DataType.createStructField("id", DataType.IntegerType, false),
                        DataType.createStructField("name", DataType.StringType, false),
                        DataType.createStructField("url", DataType.StringType, true),
                        DataType.createStructField("pictures", DataType.StringType, true),
                        DataType.createStructField("time", DataType.TimestampType, true) });

        JavaRDD<Row> rowData = data.map(new Function<String, String[]>() {
            @Override
            public String[] call(String line) throws Exception {
                return line.split("\t");
            }
        }).map(new Function<String[], Row>() {
            @Override
            public Row call(String[] r) throws Exception {
                return Row.create(Integer.parseInt(r[0]), r[1], r[2], r[3],
                        new Timestamp(DatatypeConverter.parseDateTime(r[4]).getTimeInMillis()));
            }
        });

        return sqc.applySchema(rowData, schema);
    }
}