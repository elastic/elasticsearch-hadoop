package org.elasticsearch.hadoop.rest.bulk.handler.impl;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.HttpRetryPolicy;
import org.elasticsearch.hadoop.rest.NoHttpRetryPolicy;
import org.elasticsearch.hadoop.rest.Retry;
import org.elasticsearch.hadoop.rest.SimpleHttpRetryPolicy;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.DelayableErrorCollector;
import org.elasticsearch.hadoop.util.ObjectUtils;

/**
 * Instantiates the configured HTTP Retry Policy and uses it to determine if a request should be retried.
 *
 * Do not load through the default handler loader, as this requires access to legacy settings.
 */
public class HttpRetryHandler extends BulkWriteErrorHandler {

    public HttpRetryHandler() {
        throw new EsHadoopIllegalArgumentException("HttpRetryHandler is not loadable through the default handler " +
                "loader logic. Set the HttpRetryPolicy instead.");
    }

    private final Retry retry;
    private int retryLimit;
    private long retryTime;

    public HttpRetryHandler(Settings settings) {
        String retryPolicyName = settings.getBatchWriteRetryPolicy();

        if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_SIMPLE.equals(retryPolicyName)) {
            retryPolicyName = SimpleHttpRetryPolicy.class.getName();
        }
        else if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_NONE.equals(retryPolicyName)) {
            retryPolicyName = NoHttpRetryPolicy.class.getName();
        }

        HttpRetryPolicy retryPolicy = ObjectUtils.instantiate(retryPolicyName, settings);
        this.retry = retryPolicy.init();

        this.retryLimit = settings.getBatchWriteRetryCount();
        this.retryTime = settings.getBatchWriteRetryWait();
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception {
        // FIXHERE: On 1.x versions, this should be changed to check the error message for the exception name.
        if (retry.retry(entry.getResponseCode())) {
            if (entry.getNumberOfAttempts() <= retryLimit) {
                return collector.backoffAndRetry(retryTime, TimeUnit.MILLISECONDS);
            } else {
                return collector.pass("Document bulk write attempts [" + entry.getNumberOfAttempts() +
                        "] exceeds retry limit of [" + retryLimit + "]");
            }
        } else {
            return collector.pass("Non retryable code [" + entry.getResponseCode() + "] encountered.");
        }
    }
}
