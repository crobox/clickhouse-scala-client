# Clickhouse Scala Client

[![Build Status](https://travis-ci.org/crobox/clickhouse-scala-client.svg?branch=master)](https://travis-ci.org/crobox/clickhouse-scala-client)

Clickhouse Scala Client that uses Akka Http to create a reactive streams implementation to access the [Clickhouse](https://clickhouse.yandex) database in a reactive way.

*Current implementation should be considered WIP with no guaranties on API back-compatibility*  

Artifacts:
https://oss.sonatype.org/content/repositories/snapshots/com/crobox/

for sbt you can use

```
    libraryDependencies ++= Seq(
      "com.crobox" %% "clickhouse-scala-client" % "0.0.1-SNAPSHOT"
    )
    resolvers +=
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```