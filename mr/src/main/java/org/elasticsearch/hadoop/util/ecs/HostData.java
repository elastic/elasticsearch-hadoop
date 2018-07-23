/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.util.ecs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Collects and stores Host information for use across the entire active JVM
 */
public final class HostData {

    private static final Log LOG = LogFactory.getLog(HostData.class);
    private static volatile HostData INSTANCE;

    public static HostData getInstance() {
        if (INSTANCE == null) {
            synchronized (HostData.class) {
                if (INSTANCE == null) {
                    INSTANCE = collectHostData();
                }
            }
        }
        return INSTANCE;
    }

    private static HostData collectHostData() {
        String osName = getPropOrNull("os.name");
        String osVersion = getPropOrNull("os.version");
        String osArch = getPropOrNull("os.arch");

        InetAddress localAddress;
        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            // Swallow and set the host info to null;
            LOG.warn("Could not collect host information for error tracing. Continuing with null host info.", e);
            localAddress = null;
        }

        String hostName;
        String hostip;
        if (localAddress != null) {
            hostName = localAddress.getHostName();
            hostip = localAddress.getHostAddress();
        } else {
            hostName = null;
            hostip = null;
        }

        Long hostTz = TimeUnit.SECONDS.convert(TimeZone.getDefault().getRawOffset(), TimeUnit.MILLISECONDS);

        return new HostData(hostName, hostip, hostTz, osName, osVersion, osArch);
    }

    private static String getPropOrNull(String prop) {
        try {
            // TODO: Should these privileged blocks be moved further up the call stack?
            return AccessController.doPrivileged(new GetPropAction(prop));
        } catch (SecurityException se) {
            return null;
        }
    }

    private static class GetPropAction implements PrivilegedAction<String>{
        private final String propName;

        GetPropAction(String propName) {
            this.propName = propName;
        }

        @Override
        public String run() {
            return System.getProperty(propName);
        }
    }

    public static HostData emptyHostData() {
        return new HostData(null, null, null, null, null, null);
    }

    private final boolean hasData;
    private final String name;
    private final String ip;
    private final Long timezoneOffsetSec;
    private final String osName;
    private final String osVersion;
    private final String architecture;

    private HostData(String hostName, String hostIp, Long hostTzOffset, String osName, String osVersion, String osArch) {
        this.name = hostName;
        this.ip = hostIp;
        this.timezoneOffsetSec = hostTzOffset;
        this.osName = osName;
        this.osVersion = osVersion;
        this.architecture = osArch;

        boolean hasData = (hostName != null);
        hasData |= (hostIp != null);
        hasData |= (hostTzOffset != null);
        hasData |= (osName != null);
        hasData |= (osVersion != null);
        hasData |= (osArch != null);
        this.hasData = hasData;
    }

    public boolean hasData() {
        return hasData;
    }

    public String getName() {
        return name;
    }

    public String getIp() {
        return ip;
    }

    public Long getTimezoneOffsetSec() {
        return timezoneOffsetSec;
    }

    public String getOsName() {
        return osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getArchitecture() {
        return architecture;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostData hostData = (HostData) o;

        if (name != null ? !name.equals(hostData.name) : hostData.name != null) return false;
        if (ip != null ? !ip.equals(hostData.ip) : hostData.ip != null) return false;
        if (timezoneOffsetSec != null ? !timezoneOffsetSec.equals(hostData.timezoneOffsetSec) : hostData.timezoneOffsetSec != null)
            return false;
        if (osName != null ? !osName.equals(hostData.osName) : hostData.osName != null) return false;
        if (osVersion != null ? !osVersion.equals(hostData.osVersion) : hostData.osVersion != null) return false;
        return architecture != null ? architecture.equals(hostData.architecture) : hostData.architecture == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (ip != null ? ip.hashCode() : 0);
        result = 31 * result + (timezoneOffsetSec != null ? timezoneOffsetSec.hashCode() : 0);
        result = 31 * result + (osName != null ? osName.hashCode() : 0);
        result = 31 * result + (osVersion != null ? osVersion.hashCode() : 0);
        result = 31 * result + (architecture != null ? architecture.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HostData{" +
                "hostName='" + name + '\'' +
                ", hostIp='" + ip + '\'' +
                ", hostTzOffset=" + timezoneOffsetSec +
                ", osName='" + osName + '\'' +
                ", osVersion='" + osVersion + '\'' +
                ", osArch='" + architecture + '\'' +
                '}';
    }
}
