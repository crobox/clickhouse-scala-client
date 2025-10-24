import Build.*

releaseCrossBuild := true

lazy val root = (project in file("."))
  .settings(
    publish         := {},
    publishArtifact := false,
    inThisBuild(
      List(
        organization := "com.crobox.clickhouse",
        homepage     := Some(url("https://github.com/crobox/clickhouse-scala-client")),
        licenses     := List(
          "The GNU Lesser General Public License, Version 3.0" -> url("http://www.gnu.org/licenses/lgpl-3.0.txt")
        ),
        developers := List(
          Developer(
            "crobox",
            "Crobox",
            "support@crobox.com",
            url("https://crobox.com")
          )
        ),
        scalaVersion       := "2.13.17",
        crossScalaVersions := List("2.13.17", "3.3.7"),
        javacOptions ++= Seq("-g", "-Xlint:unchecked", "-Xlint:deprecation", "-source", "11", "-target", "11"),
        scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:_", "-encoding", "UTF-8")
      )
    ),
    name := "clickhouse"
  )
  .aggregate(client, dsl, testkit)

lazy val client: Project = (project in file("client"))
  .configs(Config.CustomIntegrationTest)
  .settings(Config.testSettings: _*)
  .settings(
    name                                                              := "client",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies ++= Seq(
      "io.spray"                   %% "spray-json"    % "1.3.6",
      "org.apache.pekko"           %% "pekko-actor"   % PekkoVersion,
      "org.apache.pekko"           %% "pekko-stream"  % PekkoVersion,
      "org.apache.pekko"           %% "pekko-http"    % PekkoHttpVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6",
      "joda-time"                   % "joda-time"     % "2.14.0"
    ) ++ Seq("org.apache.pekko" %% "pekko-testkit" % PekkoVersion % Test) ++ Build.testDependencies.map(_ % Test)
  )

lazy val dsl = (project in file("dsl"))
  .dependsOn(client, client % "test->test", testkit % Test)
  .configs(Config.CustomIntegrationTest)
  .settings(Config.testSettings: _*)
  .settings(
    name                                                              := "dsl",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava"  % "33.5.0-jre",
      "com.typesafe"     % "config" % "1.4.5"
    )
  )
//  .settings(excludeDependencies ++= Seq(ExclusionRule("org.apache.pekko")))

lazy val testkit = (project in file("testkit"))
  .dependsOn(client)
  .settings(
    name                                                              := "testkit",
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    libraryDependencies ++= Build.testDependencies
  )
