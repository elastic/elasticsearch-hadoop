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
package org.elasticsearch.hadoop.rest;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class NetworkUtils {
    private NetworkUtils() {
    }

    /** Helper for getInterfaces, recursively adds subinterfaces to {@code target} */
    private static void addAllInterfaces(List<NetworkInterface> target, List<NetworkInterface> level) {
        if (!level.isEmpty()) {
            target.addAll(level);
            for (NetworkInterface intf : level) {
                addAllInterfaces(target, Collections.list(intf.getSubInterfaces()));
            }
        }
    }

    /** Return all interfaces (and subinterfaces) on the system */
    static List<NetworkInterface> getInterfaces() throws SocketException {
        List<NetworkInterface> all = new ArrayList<NetworkInterface>();
        addAllInterfaces(all, Collections.list(NetworkInterface.getNetworkInterfaces()));
        return all;
    }

    /** Returns all global scope addresses for interfaces that are up. */
    static InetAddress[] getGlobalInterfaces() throws SocketException {
        List<InetAddress> list = new ArrayList<InetAddress> ();
        for (NetworkInterface intf : getInterfaces()) {
            if (intf.isUp()) {
                for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                    if (address.isLoopbackAddress() == false &&
                            address.isSiteLocalAddress() == false &&
                            address.isLinkLocalAddress() == false) {
                        list.add(address);
                    }
                }
            }
        }
        return list.toArray(new InetAddress[list.size()]);
    }
}
