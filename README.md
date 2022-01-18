# Clickhouse Scala Client

[![Build Status](https://github.com/crobox/clickhouse-scala-client/actions/workflows/ci.yml/badge.svg)](https://github.com/crobox/clickhouse-scala-client/actions/workflows/)
[![Gitter](https://img.shields.io/gitter/room/clickhouse-scala-client/lobby.svg)](https://gitter.im/clickhouse-scala-client/lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.crobox.clickhouse/client_2.12/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/com.crobox.clickhouse/client_2.12)

Clickhouse Scala Client that uses Akka Http to create a reactive streams implementation to access the [Clickhouse](https://clickhouse.yandex) database in a reactive way.

Features:
* read/write query execution
* akka streaming source for result parsing
* akka streaming sink for data insertion
* streaming query progress (experimental)
* all the http interface settings 
* load balancing with internal health checks (multi host and cluster aware host balancer)
* ability to retry queries

*We do not guarantee api-backwards compatibility, although the API has been very stable over the last years.*  

Scala version: 
- 2.13
- 2.12

Artifacts:
https://mvnrepository.com/artifact/com.crobox.clickhouse/client_2.12
https://oss.sonatype.org/content/repositories/snapshots/com/crobox/clickhouse

for sbt you can use

```
// https://mvnrepository.com/artifact/com.crobox/clickhouse-scala-client_2.12 
libraryDependencies += "com.crobox.clickhouse" %% "client" % "0.9.0"
```

## Documentation
- [Quick Setup](#quick-setup)
    - [Client](#client)
    - [Indexer (Akka Streams Sink)](#indexer)
- [Configuration](#configuration)
    - [Client](#client-configuration)
        - [Health checks](#health-checks)
        - [Single host connection](#single-host-connection)
        - [Multi host balancing connection](#multi-host-balancing-connection)
        - [Cluster aware balancing connection](#cluster-aware-balancing-connection)
    - [Indexer configuration](#indexer-configuration)
    - [Query settings](#query-settings)
- [Client API](#client-api)
    - [Query execution](#query-execution)
    - [Query progress](#query-progress)
    - [Query settings](#query-settings)
    - [Query retrying](#query-retrying)
- [DSL](#dsl)
- [Test Kit](#test-kit)

When in doubt about the documentation please read the tests to find the truth. 
    
## Quick Setup

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

You can find all the configuration options in the [reference file](https://github.com/crobox/clickhouse-scala-client/blob/master/client/src/main/resources/reference.conf), with explanatory comments about their usage.

### Connection configuration
Three different connection modes are supported.

* single-host
* balancing-hosts
* cluster-aware

#### Health checks

The `balancing-hosts` and `cluster-aware` connections are setting up health checks for each host, by running a simple http request on clickhouse host as specified in the clickhouse [docs](https://clickhouse.yandex/docs/en/interfaces/http_interface/). For the healthchecks we use separate `Cached Host Connection Pools` with a maximum of one connection to ensure we never run more than one health check at the same time for the same host. When a host fails the healthchecks we will no longer use it to run queries. If all the health checks are failing the queries will fail fast.

```
crobox.clickhouse.client.connection {
      health-check {
        interval = 5 seconds #minimum interval between two health checks
        timeout = 1 second #health check will fail if it exceed timeout
      }
}
```

#### Single host connection

```
crobox.clickhouse.client {
    connection: {
        type = "single-host",
        host = "localhost",
        port = 8123
    }
}

```
This will not setup a health check and will dispatch all queries to the configured host.



#### Multi host balancing connection

Round robin on the configured hosts.

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
        
    }
}

```

#### Cluster aware balancing connection

The host and the port will be used to continually update the list of clickhouse nodes by querying and using the `host-name` from the `system.cluster` clickhouse table. (check `scanning-interval`)
You can specify a specific clickhouse cluster to run queries only on the respective cluster.
Please do note that this connection type will default to using the port of 8123 for all nodes.

```
crobox.clickhouse.client {
    connection: {
        type = "cluster-aware"
        host = "localhost"
        port = 8123
        cluster = "cluster" # use only hosts which belong to the "cluster" cluster
        health-check {
              interval = 5 seconds
              timeout = 1 second
        }
        scanning-interval = 10 seconds # min interval between running a new query to update the list of hosts from the system.cluster table 
    }
}

```


### Indexer configuration

Inserting into clickhouse is done using an akka stream. All the settings are applied on a per table basis.
We will do one insert when the maximum number of items `batch-size` or the maximum time has been exceeded `flush-interval`. Based on the number of `concurrent-requests` we can run multiple inserts in parallel for the same table.

```
crobox.clickhouse {
  indexer {
    batch-size = 10000
    concurrent-requests = 1
    flush-interval = 5 seconds
    fast-indexer {
        flush-interval = 1 second
        batch-size = 1000
    }
  }
}
```

### Query settings

To set authentication or a settings profile for the client you can update the following configs.
You can also set custom settings as presented in the [clickhouse documentation](https://clickhouse.yandex/docs/en/operations/settings/settings/)

```
crobox.clickhouse.client{
    settings {
      authentication {
        user = "default"
        password = ""
      }
      profile = "default"
      http-compression = false
//      https://clickhouse.yandex/docs/en/operations/settings/settings/
      custom {
           distributed_product_mode = "local"
      }
    }
}
```

## Client API

### Query execution

Read only queries

```scala
val client: ClickhouseClient
client.query("SELECT 1").map(result => println(result))
```

Write queries

```scala
val client: ClickhouseClient
client.execute("ALTER TABLE my_table DELETE WHERE id = 'deleted'").map(result => println(result))
```

Streaming delimited result (by new line)

```scala
val client: ClickhouseClient
client.source("SELECT * FROM my_table").runWith(Sink.foreach(line => println(line)))
```

Streaming raw result (ByteString)

```scala
val client: ClickhouseClient
client.sourceByteString("SELECT * FROM my_table").runWith(Sink.foreach(byteString => println(byteString)))
```

Sink streaming body

```scala
val client: ClickhouseClient
client.sink("INSERT INTO my_table", Source.single(ByteString("el1"))).map(result => println(result))
```

### Query progress

@Experimental - might not be complete

We only expose progress when running read only queries. The current implementation is recommended to be used only for long running queries which return a result relatively small in size (fits easily in memory).
The returned source is materialized with the query result.

When running queries with progress we set a custom client transport for the super pool used by client to run the queries. Due to limitation in the akka implementation which does not allow for the headers to be streamed we are parsing the raw http output and intercept the http headers to receive the progress.

We expose multiple events for the progress:
 * QueryAccepted - clickhouse returned the http response with code 200 (query might still fail)
 * QueryRejected - clickhouse returned the http response with a code different than 200 (it has not started execution)
 * QueryFailed - clickhouse returned an exception in the body, after the query was accepted and it started execution
 * Progress - contains the numbers of rows read and the number of total rows 
 * QueryRetry - the same query is being retried by the client

```scala
val client: ClickhouseClient
client.queryWithProgress("SELECT uniq(timestamps), uniq(mosquito_name) FROM mosquito_bites")
      .toMat(Sink.forEach(progress => println(progress)))(Keep.left)
      .run()
      .map(result => println(result))
```
### Query settings

Every call to the client accepts an implicit `QuerySettings` object which can override settings for that specific query.

 - You can set the query id so that you can track/kill/replace running queries.
 - You can mark the query as idempotent and it will be retried for all exceptions when running the `ClickhouseSink`(Indexer), or running queries using `client.query/client.execute`.
 - You can set specific clickhouse query settings to override the default ones
 - You can use a different clickhouse profile
 - You can run the query as a different user

```scala
val client: ClickhouseClient
implicit val settings = QuerySettings(queryId = Some("expensive_query"),settings = Map("replace_running_query" -> "1"))
client.query("SELECT uniq(expensive) FROM huge_table")//start query
client.query("SELECT uniq(expensive) FROM huge_table")//replaces existing query
```

### Query retrying

Query retrying takes advantage of host balancing and will request another host for each retry.

The queries that use the client api `source`, `sink` are not going to be retried.

All the read only queries are considered idempotent and are retried up to a maximum number of configurable times. (3 times by default, so 4 total execution, 1 the initial execution and 3 retries)
```
crobox.clickhouse.retries = 3
```

By using the `ClickhouseSink` you can also retry inserts by setting the `idempotent` setting to true on the query settings.

# DSL

Typed/composable DSL that is interpreted and parsed into queries, with ofcourse full seamless integration into the driver.

For more information see [the wiki](https://github.com/crobox/clickhouse-scala-client/wiki)

# Test Kit

We also expose an utility test kit which provider a helpful spec with testing utilities. It automatically creates a single use database before all tests and drops it afterwards.

```
// https://mvnrepository.com/artifact/com.crobox/clickhouse-scala-client_2.12 
libraryDependencies += "com.crobox.clickhouse" %% "testkit" % <latest_version>
```

Check [the spec](https://github.com/crobox/clickhouse-scala-client/blob/master/testkit/src/main/scala/com/crobox/clickhouse/testkit/ClickhouseSpec.scala) for more details.


