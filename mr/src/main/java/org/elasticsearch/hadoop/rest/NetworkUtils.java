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
