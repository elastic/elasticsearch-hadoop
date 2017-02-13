package org.elasticsearch.hadoop.mr;

import org.elasticsearch.hadoop.util.EsMajorVersion;

import static org.junit.Assume.assumeTrue;

/**
 * Class for bringing together common assume calls into one place
 */
public class EsAssume {

    public static void versionOnOrAfter(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrAfter(version));
    }

    public static void versionOn(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrAfter(version));
    }

    public static void versionOnOrBefore(EsMajorVersion version, String message) {
        assumeTrue(message, getVersion().onOrBefore(version));
    }

    private static EsMajorVersion getVersion() {
        try (RestUtils.ExtendedRestClient versionTestingClient = new RestUtils.ExtendedRestClient()) {
            return versionTestingClient.remoteEsVersion();
        }
    }
}
