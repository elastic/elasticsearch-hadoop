package org.elasticsearch.hadoop.mr;

import org.elasticsearch.hadoop.util.EsMajorVersion;

/**
 * Class for bringing together common assume calls into one place
 */
public class Assume {

    public static void versionOnOrAfter(EsMajorVersion version, String message) {
        org.junit.Assume.assumeTrue(message, getVersion().onOrAfter(version));
    }

    private static EsMajorVersion getVersion() {
        try (RestUtils.ExtendedRestClient versionTestingClient = new RestUtils.ExtendedRestClient()) {
            return versionTestingClient.remoteEsVersion();
        }
    }
}
