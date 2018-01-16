package org.elasticsearch.hadoop.rest.bulk;

import com.google.common.base.Charsets;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.BulkResponse;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.bulk.bwc.BulkOutputGeneratorV5;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.Assert.assertEquals;

public class BulkProcessorTest {

    private Settings testSettings;
    private String inputEntry;
    private Resource resource;
    private BulkOutputGenerator generator;

    @Before
    public void setUp() throws Exception {
        inputEntry = IOUtils.asString(getClass().getResourceAsStream("/org/elasticsearch/hadoop/rest/bulk-retry-input-template.json"));

        testSettings = new TestSettings();
        testSettings.setResourceWrite("foo/bar");
        testSettings.setInternalVersion(EsMajorVersion.V_7_X);
        testSettings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "10");

        resource = new Resource(testSettings, false);

        generator = new BulkOutputGeneratorV5();
//        generator = new BulkOutputGeneratorV2();
//        generator = new BulkOutputGeneratorV1();
    }

    @Test
    public void testBulk00_Empty() throws Exception {
        BulkProcessor processor = getBulkProcessor();

        BulkResponse bulkResponse = processor.tryFlush();

        assertEquals(0, bulkResponse.getDocsSent());
        assertEquals(0, bulkResponse.getDocsSkipped());
        assertEquals(0, bulkResponse.getDocsAborted());

        processor.close();
        Stats stats = processor.stats();

        assertEquals(0, stats.bulkRetries);
        assertEquals(0, stats.docsRetried);
        assertEquals(0, stats.docsAccepted);
    }

    @Test
    public void testBulk00_Success() throws Exception {
        BulkProcessor processor = getBulkProcessor(
                generator.setInfo(resource, 56)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .generate()
        );

        processData(processor);

        BulkResponse bulkResponse = processor.tryFlush();

        assertEquals(5, bulkResponse.getDocsSent());
        assertEquals(0, bulkResponse.getDocsSkipped());
        assertEquals(0, bulkResponse.getDocsAborted());

        processor.close();
        Stats stats = processor.stats();

        assertEquals(0, stats.bulkRetries);
        assertEquals(0, stats.docsRetried);
        assertEquals(5, stats.docsAccepted);
    }

    @Test
    public void testBulk01_OneRetry() throws Exception {
        BulkProcessor processor = getBulkProcessor(
                generator.setInfo(resource, 56)
                        .addSuccess("index", 201)
                        .addRejection("index")
                        .addSuccess("index", 201)
                        .addRejection("index")
                        .addRejection("index")
                        .generate(),
                generator.setInfo(resource, 56)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .addSuccess("index", 201)
                        .generate()
        );

        processData(processor);

        BulkResponse bulkResponse = processor.tryFlush();

        assertEquals(5, bulkResponse.getDocsSent());
        assertEquals(0, bulkResponse.getDocsSkipped());
        assertEquals(0, bulkResponse.getDocsAborted());

        processor.close();
        Stats stats = processor.stats();

        assertEquals(1, stats.bulkRetries);
        assertEquals(3, stats.docsRetried);
        assertEquals(5, stats.docsAccepted);
    }

    private BulkProcessor getBulkProcessor(RestClient.BulkActionResponse... responses) {
        return new BulkProcessor(mockClientResponses(responses), resource, testSettings);
    }

    private RestClient mockClientResponses(RestClient.BulkActionResponse... responses) {
        RestClient mockClient = Mockito.mock(RestClient.class);

        OngoingStubbing<RestClient.BulkActionResponse> stubb = Mockito.when(
                mockClient.bulk(Mockito.eq(resource), Mockito.any(TrackingBytesArray.class))
        );

        for (RestClient.BulkActionResponse response : responses) {
            stubb = stubb.thenReturn(response);
        }

        stubb.thenThrow(new AssertionError("Exhausted all given test responses."));

        return mockClient;
    }

    private void processData(BulkProcessor processor) {
        BytesRef data = new BytesRef();

        data.add(renderEntry("A"));
        processor.add(data);
        data.reset();

        data.add(renderEntry("B"));
        processor.add(data);
        data.reset();

        data.add(renderEntry("C"));
        processor.add(data);
        data.reset();

        data.add(renderEntry("D"));
        processor.add(data);
        data.reset();

        data.add(renderEntry("E"));
        processor.add(data);
        data.reset();
    }

    private byte[] renderEntry(String data) {
        return inputEntry.replace("w", data).getBytes(Charsets.UTF_8);
    }
}