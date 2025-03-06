---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/cloud.html
navigation_title: Cloud or restricted environments
---
# {{esh-full}} Cloud or restricted environments [cloud]

In an ideal setup, elasticsearch-hadoop achieves best performance when {{es}} and Hadoop are fully accessible from every other, that is each node on the Hadoop side can access every node inside the {{es}} cluster. This allows maximum parallelism between the two system and thus, as the clusters scale *out* so does the communication between them.

However not all environments are setup like that, in particular cloud platforms such as Amazon Web Services, Microsoft Azure or Google Compute Engine or dedicated {{es}} services like [Cloud](https://www.elastic.co/cloud) that allow computing resources to be rented when needed. The typical setup here is for the spawned nodes to be started in the *cloud*, within a dedicated private network and be made available over the Internet at a dedicated address. This effectively means the two systems, {{es}} and Hadoop/Spark, are running on two *separate* networks that do not fully see each other (if at all); rather all access to it goes through a publicly exposed *gateway*.

Running elasticsearch-hadoop against such an {{es}} instance will quickly run into issues simply because the connector once connected, will discover the cluster nodes, their IPs and try to connect to them to read and/or write. However as the {{es}} nodes are using non-routeable, private IPs and are not accessible from *outside* the cloud infrastructure, the connection to the nodes will fail.

There are several possible workarounds for this problem:

## Collocate the two clusters [_collocate_the_two_clusters]

The obvious solution is to run the two systems on the same network. This will get rid of all the network logistics hops, improve security (no need to open up ports) and most importantly have huge performance benefits since everything will be co-located. Even if your data is not in the cloud, moving a piece of it to quickly iterate on it can yield large benefits. The data will likely go back and forth between the two networks anyway so you might as well do so directly, in bulk.


## Make the cluster accessible [_make_the_cluster_accessible]

As the nodes are not accessible from outside, fix the problem by assigning them public IPs. This means that at least one of the clusters (likely {{es}}) will be fully accessible from outside which typically is a challenge both in terms of logistics (large number of public IPs required) and security.


## Use a dedicated series of proxies/gateways [_use_a_dedicated_series_of_proxiesgateways]

If exposing the cluster is not an option, one can chose to use a proxy or a VPN so that the Hadoop cluster can transparently access the {{es}} cluster in a different network. By using an indirection layer, the two networks can *transparently* communicate with each other. Do note that usually this means the two networks would know how to properly route the IPs from one to the other. If the proxy/VPN solution does not handle this automatically, {{es}} might help through its [network settings](elasticsearch://reference/elasticsearch/configuration-reference/networking-settings.md) in particular `network.host` and `network.publish_host` which control what IP the nodes bind and in particular *publish* or *advertise* to their clients. This allows a certain publicly-accessible IP to be broadcasted to the clients to allow access to a node, even if the node itself does not run on that IP.


## Configure the connector to run in WAN mode [_configure_the_connector_to_run_in_wan_mode]

Introduced in 2.2, elasticsearch-hadoop can be configured to run in WAN mode that is to restrict or completely reduce its parallelism when connecting to {{es}}. By setting `es.nodes.wan.only`, the connector will limit its network usage and instead of connecting directly to the target resource shards, it will make connections to the {{es}} cluster **only** through the nodes declared in `es.nodes` settings. It will **not** perform any discovery, ignore data or client nodes and simply make network call through the aforementioned nodes. This effectively ensures that network access happens only through the declared network nodes.

Last but not least, the further the clusters are and the more data needs to go between them, the lower the performance will be since each network call is quite expensive.


