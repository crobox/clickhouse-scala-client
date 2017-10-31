import com.typesafe.sbt.pgp.PgpKeys
import Build._

lazy val root = Project("clickhouse-scala-client", file("."))
  .settings(
    organization := "com.crobox",
    name := "clickhouse-scala-client",
    scalaVersion := "2.11.11",
    crossScalaVersions := List("2.11.11", "2.12.3"),
    scalacOptions ++= List(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding", "UTF-8"
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.google.guava" % "guava" % "19.0",
      "com.fasterxml.jackson.core" % "jackson-core" % JacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % JacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % JacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % JacksonScalaVersion,

      "org.scalatest" %% "scalatest" % "3.0.0" % Test,
      "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
    ),
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
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
        <developers>
          <developer>
            <id>crobox</id>
            <name>crobox</name>
            <url>https://github.com/crobox</url>
          </developer>
        </developers>
    }
  )
