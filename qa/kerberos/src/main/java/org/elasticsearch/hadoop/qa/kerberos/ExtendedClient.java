package org.elasticsearch.hadoop.qa.kerberos;

import java.io.IOException;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.IOUtils;

import static org.elasticsearch.hadoop.rest.Request.Method.PUT;

public class ExtendedClient extends RestClient {

    public ExtendedClient(Settings settings) {
        super(settings);
    }

    @Override
    public Response execute(Request.Method method, String path, ByteSequence buffer) {
        return super.execute(method, path, buffer);
    }

    public String get(String index) throws IOException {
        return IOUtils.asString(execute(Request.Method.GET, index));
    }

    public String post(String index, byte[] buffer) throws IOException {
        return IOUtils.asString(execute(Request.Method.POST, index, new BytesArray(buffer)).body());
    }

    public String put(String index, byte[] buffer) throws IOException {
        return IOUtils.asString(execute(PUT, index, new BytesArray(buffer)).body());
    }

}
