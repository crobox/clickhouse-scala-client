# Clickhouse Scala Client

[![Build Status](https://travis-ci.org/crobox/clickhouse-scala-client.svg?branch=master)](https://travis-ci.org/crobox/clickhouse-scala-client)

Clickhouse Scala Client that uses Akka Http to create a reactive streams implementation to access the [Clickhouse](https://clickhouse.yandex) database in a reactive way.

*Current implementation should be considered WIP with no guaranties on API back-compatibility*  

Artifacts:
https://mvnrepository.com/artifact/com.crobox/clickhouse-scala-client_2.11
https://oss.sonatype.org/content/repositories/snapshots/com/crobox/

for sbt you can use

```
// https://mvnrepository.com/artifact/com.crobox/clickhouse-scala-client_2.11
libraryDependencies += "com.crobox" % "clickhouse-scala-client_2.11" % "0.0.1"
```

## Configuration

  All the configuration keys are under the prefix `com.crobox.clickhouse.client`
### Client configuration

| Key | Default|Description |
| --- | ---|------------|
|httpCompression| false | If the client should use http compression in the communication with clickhouse. More info: http://clickhouse-docs.readthedocs.io/en/latest/interfaces/http_interface.html |
|bufferSize|1024|The size of the internal queue being used for the queries. If the queue is full then any new queries will be dropped|

### Connection configuration
The clickhouse connection is configured from the configuration file as well.
Three different connection modes are supported.

       The balancing connections use actors to monitor the connection health by doing a `SELECT 1` on it. If the query fails then the connection is no longer served untill the query succeeds.
       The cluster aware balancing connection also periodically requires the `system.cluster` table to update the connections with any added/removed cluster node.

#### Single host connection

Configuration options:

| Key | Default|Description |
| --- | ---|------------|
|type | N/A|"single-host"|
| host |localhost| The host of the clickhouse server |
| port| port | The port of the clickhouse server|

###### Example:

**This is also the default configuration**

```
com.crobox.clickhouse.client {
    connection: {
        type: "single-host",
        host: "localhost",
        port: 8123
    }
}

```

#### Multi host balancing connection

Round robin on the hosts lists, while keeping only connections that respond the the `SELECT 1` healthcheck.

| Key | Description|
| ---| ------|
| hosts| Array of config keys, with host and port subkeys|

|Subkeys| Default | Description|
|----|-----|-----|
|type | N/A|"balancing-hosts"|
|host|N/A|The host of the clickhouse server|
|port|N/A|The port of the clickhouse server|
|health-check-interval| 5 | Seconds between the healthchecks for each host |
###### Example:

```
com.crobox.clickhouse.client {
    connection: {
        type: "balancing-hosts"
        hosts: [
          {
            host: "localhost",
            port: 7415
          }
        ]
        health-check-interval = 5
    }
}

```

#### Cluster aware balancing connection

| Key | Default|Description |
| --- | ---|------------|
|type | N/A|"balancing-hosts"|
| host |localhost| The host of the clickhouse server |
| port| 8123 | The port of the clickhouse server|
| cluster| cluster | The cluster name for which to query for connections|
|health-check-interval| 5 | Seconds between the healthchecks for each host |

The host and the port will be used to continually update the list of clickhouse nodes by querying and using the `host-name` from the `system.cluster` clickhouse table.
Please do note that this connection type will default to using the port of 8123 for all nodes.

###### Example:

```
com.crobox.clickhouse.client {
    connection: {
        type: "cluster-aware"
        host: "localhost"
        port: 8123
        health-check-interval = 5
    }
}

```