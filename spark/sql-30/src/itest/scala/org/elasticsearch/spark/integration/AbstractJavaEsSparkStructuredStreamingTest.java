package org.elasticsearch.spark.integration;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.mr.EsAssume;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.streaming.api.java.JavaStreamingQueryTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INDEX_AUTO_CREATE;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_INGEST_PIPELINE;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_MAPPING_EXCLUDE;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NODES_INGEST_ONLY;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static scala.collection.JavaConversions.propertiesAsScalaMap;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractJavaEsSparkStructuredStreamingTest {

    private static final transient SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("es-structured-streaming-test")
            .setJars(SparkUtils.ES_SPARK_TESTING_JAR);

    private static transient SparkSession spark = null;

    @Parameterized.Parameters
    public static Collection<Object[]> testParams() {
        Collection<Object[]> params = new ArrayList<>();
        params.add(new Object[] {"java-struct-stream-default"});
        return params;
    }

    @BeforeClass
    public static void setup() {
        conf.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void clean() throws Exception {
        if (spark != null) {
            spark.stop();
            // wait for jetty & spark to properly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    private String prefix;
    private String commitLogDir;
    private EsMajorVersion version = TestUtils.getEsVersion();

    public AbstractJavaEsSparkStructuredStreamingTest(String prefix) throws Exception {
        this.prefix = prefix;

        // Set up the commit log directory that we'll use for the test:
        File tempDir = File.createTempFile("es-spark-structured-streaming", "");
        tempDir.delete();
        tempDir.mkdir();
        File logDir = new File(tempDir, "logs");
        logDir.mkdir();
        this.commitLogDir = logDir.getAbsolutePath();
    }

    private String wrapIndex(String index) {
        return prefix + index;
    }

    private String checkpoint(String target) {
        return commitLogDir + "/$target";
    }

    /**
     * Data object for most of the basic tests in here
     */
    public static class RecordBean implements Serializable {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void test0FailOnIndexCreationDisabled() throws Exception {
        String target = wrapIndex("test-nonexisting/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .expectingToThrow(EsHadoopIllegalArgumentException.class)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(ES_INDEX_AUTO_CREATE, "no")
                        .format("es"),
                target
        );

        assertTrue(!RestUtils.exists(target));
    }

    @Test
    public void test1BasicWrite() throws Exception {
        String target = wrapIndex("test-write/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .format("es"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), containsString("Spark"));
        assertThat(RestUtils.get(target + "/_search?"), containsString("Hadoop"));
        assertThat(RestUtils.get(target + "/_search?"), containsString("YARN"));
    }

    @Test
    public void test1WriteWithMappingId() throws Exception {
        String target = wrapIndex("test-write-id/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option("es.mapping.id", "id")
                        .format("es"),
                target
        );

        assertEquals(3, JavaEsSpark.esRDD(new JavaSparkContext(spark.sparkContext()), target).count());
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));
        assertTrue(RestUtils.exists(target + "/3"));

        assertThat(RestUtils.get(target + "/_search?"), containsString("Spark"));
    }

    @Test
    public void test1WriteWithMappingExclude() throws Exception {
        String target = wrapIndex("test-mapping-exclude/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(ES_MAPPING_EXCLUDE, "name")
                        .format("es"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), not(containsString("Spark")));
        assertThat(RestUtils.get(target +  "/_search?"), not(containsString("Hadoop")));
        assertThat(RestUtils.get(target +  "/_search?"), not(containsString("YARN")));
    }

    @Test
    public void test2WriteToIngestPipeline() throws Exception {
        EsAssume.versionOnOrAfter(EsMajorVersion.V_5_X, "Ingest Supported in 5.x and above only");

        String pipelineName =  prefix + "-pipeline";
        String pipeline = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}";
        RestUtils.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline));

        String target = wrapIndex("test-write-ingest/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(ES_INGEST_PIPELINE, pipelineName)
                        .option(ES_NODES_INGEST_ONLY, "true")
                        .format("es"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target+"/_search?"), containsString("\"pipeTEST\":true"));
    }

    @Test
    public void test1MultiIndexWrite() throws Exception {
        String target = wrapIndex("test-write-tech-{name}/data");
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("hadoop");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .format("es"),
                target
        );

        assertTrue(RestUtils.exists(wrapIndex("test-write-tech-spark/data")));
        assertTrue(RestUtils.exists(wrapIndex("test-write-tech-hadoop/data")));

        assertThat(RestUtils.get(wrapIndex("test-write-tech-spark/data/_search?")), containsString("\"name\":\"spark\""));
        assertThat(RestUtils.get(wrapIndex("test-write-tech-hadoop/data/_search?")), containsString("\"name\":\"hadoop\""));
    }

    public static class ContactBean implements Serializable {
        private String id;
        private String note;
        private AddressBean address;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getNote() {
            return note;
        }

        public void setNote(String note) {
            this.note = note;
        }

        public AddressBean getAddress() {
            return address;
        }

        public void setAddress(AddressBean address) {
            this.address = address;
        }
    }

    public static class AddressBean implements Serializable {
        private String id;
        private String zipcode;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getZipcode() {
            return zipcode;
        }

        public void setZipcode(String zipcode) {
            this.zipcode = zipcode;
        }
    }

    @Test
    @Ignore("Serialization issues in DataFrameValueWriter when trying to serialize an object for use in parameters")
    public void test3WriteWithUpsertScript() throws Exception {
        // BWC
        String keyword = "keyword";
        String lang = "painless";
        if (version.onOrBefore(EsMajorVersion.V_2_X)) {
            keyword = "string";
            lang = "groovy";
        }

        // Init
        String mapping = "{\"data\":{\"properties\":{\"id\":{\"type\":\""+keyword+"\"},\"note\":{\"type\":\""+keyword+"\"},\"address\":{\"type\":\"nested\",\"properties\":{\"id\":{\"type\":\""+keyword+"\"},\"zipcode\":{\"type\":\""+keyword+"\"}}}}}}";
        String index = wrapIndex("test-script-upsert");
        String type = "data";
        String target = index + "/" + type;

        RestUtils.touch(index);
        RestUtils.putMapping(index, type, mapping.getBytes());
        RestUtils.postData(target+"/1", "{\"id\":\"1\",\"note\":\"First\",\"address\":[]}".getBytes());
        RestUtils.postData(target+"/2", "{\"id\":\"2\",\"note\":\"First\",\"address\":[]}".getBytes());

        // Common configurations
        Map<String, String> updateProperties = new HashMap<>();
        updateProperties.put("es.write.operation", "upsert");
        updateProperties.put("es.mapping.id", "id");
        updateProperties.put("es.update.script.lang", lang);

        // Run 1
        ContactBean doc1;
        {
            AddressBean address = new AddressBean();
            address.setId("1");
            address.setZipcode("12345");
            doc1 = new ContactBean();
            doc1.setId("1");
            doc1.setAddress(address);
        }

        String script1;
        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            script1 = "ctx._source.address.add(params.new_address)";
        } else {
            script1 = "ctx._source.address+=new_address";
        }

        JavaStreamingQueryTestHarness<ContactBean> test1 = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(ContactBean.class));

        test1
            .withInput(doc1)
            .run(
                test1.stream()
                    .writeStream()
                    .option("checkpointLocation", checkpoint(target))
                    .options(updateProperties)
                    .option("es.update.script.params", "new_address:address")
                    .option("es.update.script", script1)
                    .format("es"),
                target
            );

        // Run 2
        ContactBean doc2;
        {
            doc2 = new ContactBean();
            doc2.setId("2");
            doc2.setNote("Second");
        }

        String script2;
        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            script2 = "ctx._source.note = params.new_note";
        } else {
            script2 = "ctx._source.note=new_note";
        }

        JavaStreamingQueryTestHarness<ContactBean> test2 = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(ContactBean.class));

        test2
            .withInput(doc2)
            .run(
                test2.stream()
                    .writeStream()
                    .option("checkpointLocation", checkpoint(target))
                    .options(updateProperties)
                    .option("es.update.script.params", "new_note:note")
                    .option("es.update.script", script2)
                    .format("es"),
                target
            );

        // Validate
        assertTrue(RestUtils.exists(target + "/1"));
        assertThat(RestUtils.get(target + "/1"), both(containsString("\"zipcode\":\"12345\"")).and(containsString("\"note\":\"First\"")));

        assertTrue(RestUtils.exists(target + "/2"));
        assertThat(RestUtils.get(target + "/2"), both(not(containsString("\"zipcode\":\"12345\""))).and(containsString("\"note\":\"Second\"")));
    }
}
