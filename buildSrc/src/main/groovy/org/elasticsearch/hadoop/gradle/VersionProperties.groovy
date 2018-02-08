package org.elasticsearch.hadoop.gradle

/**
 * Loads the locally available version information from the build source.
 */
class VersionProperties {

    public static final String ESHADOOP_VERSION
    public static final String ELASTICSEARCH_VERSION
    public static final Map<String, String> VERSIONS
    static {
        Properties versionProperties = new Properties()
        InputStream propertyStream = VersionProperties.class.getResourceAsStream('/esh-version.properties')
        if (propertyStream == null) {
            throw new RuntimeException("Could not locate the esh-version.properties file!")
        }
        versionProperties.load(propertyStream)
        ESHADOOP_VERSION = versionProperties.getProperty('eshadoop')
        ELASTICSEARCH_VERSION = versionProperties.getProperty('elasticsearch')
        VERSIONS = new HashMap<>()
        for (String propertyName: versionProperties.stringPropertyNames()) {
            VERSIONS.put(propertyName, versionProperties.getProperty(propertyName))
        }
    }
}
