import Build._

lazy val root = Project("clickhouse-scala-client", file("."))
  .settings(
    organization := "com.crobox",
    name := "clickhouse-scala-client",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.11.9",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      "com.google.guava" % "guava" % "19.0",
      "com.fasterxml.jackson.core" % "jackson-core" % JacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % JacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % JacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % JacksonScalaVersion,

      "org.scalatest" %% "scalatest" % "3.0.0" % Test
    ),
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := {
      <url>https://github.com/crobox/clickhouse-scala-client</url>
        <licenses>
          <license>
            <name>The GNU Lesser General Public License, Version 3.0</name>
            <url>http://www.gnu.org/licenses/lgpl-3.0.txt</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:crobox/clickhouse-scala-client.git</url>
          <connection>scm:git@github.com:crobox/clickhouse-scala-client.git</connection>
        </scm>
    }
  )
