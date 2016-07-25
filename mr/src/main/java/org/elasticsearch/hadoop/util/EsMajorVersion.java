package org.elasticsearch.hadoop.util;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

/**
 * Elasticsearch major version information, useful to check client's query compatibility with the Rest API.
 */
public class EsMajorVersion {
    public static final EsMajorVersion V_0_X = new EsMajorVersion((byte) 0, "0.x");
    public static final EsMajorVersion V_1_X = new EsMajorVersion((byte) 1, "1.x");
    public static final EsMajorVersion V_2_X = new EsMajorVersion((byte) 2, "2.x");
    public static final EsMajorVersion V_5_X = new EsMajorVersion((byte) 5, "5.x");
    public static final EsMajorVersion LATEST = V_5_X;


    public final byte major;
    private final String version;

    private EsMajorVersion(byte major, String version) {
        this.major = major;
        this.version = version;
    }

    public boolean after(EsMajorVersion version) {
        return version.major < major;
    }

    public boolean on(EsMajorVersion version) {
        return version.major == major;
    }

    public boolean notOn(EsMajorVersion version) {
        return !on(version);
    }

    public boolean onOrAfter(EsMajorVersion version) {
        return version.major <= major;
    }

    public boolean before(EsMajorVersion version) {
        return version.major > major;
    }

    public boolean onOrBefore(EsMajorVersion version) {
        return version.major >= major;
    }

    public static EsMajorVersion parse(String version) {
        if (version.startsWith("0.")) {
            return new EsMajorVersion((byte) 0, version);
        }
        if (version.startsWith("1.")) {
            return new EsMajorVersion((byte) 1, version);
        }
        if (version.startsWith("2.")) {
            return new EsMajorVersion((byte) 2, version);
        }
        if (version.startsWith("5.")) {
            return new EsMajorVersion((byte) 5, version);
        }
        throw new EsHadoopIllegalArgumentException("Unsupported/Unknown Elasticsearch version " + version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EsMajorVersion version = (EsMajorVersion) o;

        return major == version.major &&
                version.equals(version.version);
    }

    @Override
    public int hashCode() {
        return major;
    }

    @Override
    public String toString() {
        return version;
    }
}
