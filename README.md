# Clickhouse Scala Client

[![Build Status](https://travis-ci.org/crobox/clickhouse-scala-client.svg?branch=master)](https://travis-ci.org/crobox/clickhouse-scala-client)

Clickhouse Scala Client that uses Akka Http to create a reactive streams implementation to access the [Clickhouse](https://clickhouse.yandex) database in a reactive way.

*Current implementation should be considered WIP with no guarantees on API back-compatibility*  

Scala versions: 
- 2.11
- 2.12

Artifacts:
https://mvnrepository.com/artifact/com.crobox.clickhouse/client_2.12
https://oss.sonatype.org/content/repositories/snapshots/com/crobox/clickhouse

for sbt you can use

```
// https://mvnrepository.com/artifact/com.crobox/clickhouse-scala-client_2.12 
libraryDependencies += "com.crobox.clickhouse" %% "client" % "0.7.4"
```

## Clickhouse query DSL 
For more information see: https://github.com/crobox/clickhouse-scala-client/wiki

## Usage example

### Client

```scala

val config: Config
val queryDatabase: String = "default"
implicit val system:ActorSystem

val client = new ClickhouseClient(config, queryDatabase)
client.query("SELECT 1 + 1").map(result => {
    println(s"Got query result $result")
})
```

### Indexer

```scala
val config: Config
val client: ClickhouseClient

val sink = ClickhouseSink.insertSink(config, client)
sink.runWith(Source.single(Insert("clicks", "{some_column: 3 }"))
```

## Configuration

 - Client: All the configuration keys are under the prefix `crobox.clickhouse.client`
 - Indexer: All the configuration keys are under the prefix `crobox.clickhouse.indexer`. You can also provide specific overrides based on the indexer name by using the same configs under the prefix `crobox.clickhouse.indexer.{indexer-name}`
### Client configuration

You can find all the configuration options in the `reference.conf` file, with explanatory comments about their usage.

### Connection configuration
The clickhouse connection is configured from the configuration file as well.
Three different connection modes are supported.

       The balancing connections use actors to monitor the connection health by doing a `SELECT 1` on it. If the query fails then the connection is no longer served until the query succeeds.
       The cluster aware balancing connection also periodically requires the `system.cluster` table to update the connections with any added/removed cluster node.

#### Single host connection

Configuration options:

| Key | Default|Description |
| --- | ---|------------|
|type | N/A|"single-host"|
| host |localhost| The host of the clickhouse server |
| port| N/A | The port of the clickhouse server, if required |

###### Example:
** The default configuration. **

```
crobox.clickhouse.client {
    connection: {
        type = "single-host",
        host = "localhost",
        port = 8123
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
|health-check.interval| 5 seconds | Seconds between the healthchecks for each host |
|health-check.timeout | 1 second | Timeout to wait for clickhouse to respond before considering healthcheck failed|
###### Example:

```
crobox.clickhouse.client {
    connection: {
        type = "balancing-hosts"
        hosts: [
          {
            host = "localhost",
            port = 7415
          }
        ]
        health-check {
              interval = 5 seconds
              timeout = 1 second
        }
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
|health-check.interval| 5 seconds | Seconds between the healthchecks for each host |
|health-check.timeout | 1 second | Timeout to wait for clickhouse to respond before considering healthcheck failed|

The host and the port will be used to continually update the list of clickhouse nodes by querying and using the `host-name` from the `system.cluster` clickhouse table.
Please do note that this connection type will default to using the port of 8123 for all nodes.

###### Example:

```
crobox.clickhouse.client {
    connection: {
        type = "cluster-aware"
        host = "localhost"
        port = 8123
        health-check {
              interval = 5 seconds
              timeout = 1 second
        }
    }
}

```
