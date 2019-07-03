package org.elasticsearch.hadoop.gradle

/**
 * Loads the locally available version information from the build source.
 */
class EshVersionProperties {

    public static final String ESHADOOP_VERSION
    public static final String ELASTICSEARCH_VERSION
    public static final String LUCENE_VERSION
    public static final String BUILD_TOOLS_VERSION
    public static final Map<String, String> VERSIONS
    static {
        Properties versionProperties = new Properties()
        InputStream propertyStream = EshVersionProperties.class.getResourceAsStream('/esh-version.properties')
        if (propertyStream == null) {
            throw new RuntimeException("Could not locate the esh-version.properties file!")
        }
        versionProperties.load(propertyStream)
        ESHADOOP_VERSION = versionProperties.getProperty('eshadoop')
        ELASTICSEARCH_VERSION = versionProperties.getProperty('elasticsearch')
        LUCENE_VERSION = versionProperties.getProperty('lucene')
        BUILD_TOOLS_VERSION = versionProperties.getProperty('build-tools')
        VERSIONS = new HashMap<>()
        for (String propertyName: versionProperties.stringPropertyNames()) {
            VERSIONS.put(propertyName, versionProperties.getProperty(propertyName))
        }
    }
}
