package org.elasticsearch.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility to detect different OS runtime environments
 */
public final class OsUtil {

    private static final Log LOG = LogFactory.getLog(OsUtil.class);

    /**
     * Name of the current operating system configured in the 'os.name' system property, or null if the property
     * could not be read.
     */
    public static final String OS_NAME;
    static {
        String osName;
        try {
            osName = System.getProperty("os.name");
        } catch (SecurityException ex) {
            // For whatever reason we cannot get this property
            LOG.error("Caught SecurityException reading 'os.name' system property. Defaulting to null.", ex);
            osName = null;
        }
        OS_NAME = osName;
    }

    private static final String WINDOWS_PREFIX = "Windows";

    /**
     * True if the underlying operating system is part of the Windows Family
     */
    public static final boolean IS_OS_WINDOWS = osMatches(WINDOWS_PREFIX);

    private static boolean osMatches(String osPrefix) {
        return OS_NAME != null && OS_NAME.startsWith(osPrefix);
    }

    private OsUtil() { /* No instance */ }
}
