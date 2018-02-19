import Build._
import com.typesafe.sbt.pgp.PgpKeys

//scalafmt settings
scalafmtVersion in ThisBuild := "1.0.0"
scalafmtOnCompile in ThisBuild := false     // all projects
scalafmtTestOnCompile in ThisBuild := false // all projects

lazy val root = (project in file("."))
  .settings(
    publish := {},
    publishArtifact := false,
    inThisBuild(
      List(
        organization := "com.crobox.clickhouse",
        scalaVersion := "2.12.4",
        crossScalaVersions := List("2.11.12", "2.12.4"),
        scalacOptions ++= List(
          "-unchecked",
          "-deprecation",
          "-language:_",
          "-encoding",
          "UTF-8"
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
            <developers>
              <developer>
                <id>crobox</id>
                <name>crobox</name>
                <url>https://github.com/crobox</url>
              </developer>
            </developers>
        }
      )
    ),
    name := "clickhouse"
  )
  .aggregate(client, dsl, testkit)
lazy val client: Project = (project in file("client"))
  .settings(
    name := "client",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies ++= Seq(
      "com.typesafe.akka"          %% "akka-actor" % AkkaVersion,
      "com.typesafe.akka"          %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka"          %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "joda-time"                  % "joda-time" % "2.9.9"
    ) ++ testDependencies.map(_    % Test)
  )

lazy val testkit = (project in file("testkit"))
  .settings(
    name := "testkit",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies ++=
      Build.testDependencies
  )
  .dependsOn(client)

lazy val dsl = (project in file("dsl"))
  .settings(
    name := "dsl",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies := Seq(
      "io.spray" %% "spray-json" % "1.3.3",
      "com.google.guava" % "guava" % "19.0",
      "com.dongxiguo" %% "fastring" % "0.3.1"
    )
  )
  .dependsOn(client, testkit)
