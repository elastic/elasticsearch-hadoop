package org.elasticsearch.hadoop.rest.bulk;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;

/**
 * Test class for producing bulk endpoint output data programatically for different versions.
 */
public interface BulkOutputGenerator {

    public BulkOutputGenerator setInfo(Resource resource, long took);

    public BulkOutputGenerator addSuccess(String operation, int status);

    public BulkOutputGenerator addFailure(String operation, int status, String type, String errorMessage);

    public BulkOutputGenerator addRejection(String operation);

    public RestClient.BulkActionResponse generate() throws IOException;

}
