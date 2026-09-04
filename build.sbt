import Build.*

lazy val root = (project in file("."))
  // sbt 2 only resolves a config-scoped key against configurations the current project declares, so the unscoped
  // `IntegrationTest/testFull` CI invokes needs the configuration here too, to aggregate down to client and dsl.
  .configs(Config.IntegrationTest)
  .settings(
    publish         := {},
    publishArtifact := false,
    inThisBuild(
      List(
        organization := "com.crobox.clickhouse",
        homepage     := Some(uri("https://github.com/crobox/clickhouse-scala-client")),
        licenses     := List(
          "The GNU Lesser General Public License, Version 3.0" -> uri("http://www.gnu.org/licenses/lgpl-3.0.txt")
        ),
        developers := List(
          Developer(
            "crobox",
            "Crobox",
            "support@crobox.com",
            uri("https://crobox.com")
          )
        ),
        // sbt-ci-release publishes with no version scheme otherwise, and warns on every task that reads it.
        versionScheme      := Some("early-semver"),
        scalaVersion       := "2.13.18",
        crossScalaVersions := List("2.13.18", "3.9.0"),
        javacOptions ++= Seq("-g", "-Xlint:unchecked", "-Xlint:deprecation", "-source", "11", "-target", "11"),
        scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:_", "-encoding", "UTF-8")
      )
    ),
    name := "clickhouse"
  )
  .aggregate(client, dsl, testkit)

lazy val client: Project = (project in file("client"))
  .configs(Config.IntegrationTest)
  .settings(Config.testSettings)
  .settings(
    name := "client",
    libraryDependencies ++= Seq(
      "io.spray"                   %% "spray-json"    % "1.3.6",
      "org.apache.pekko"           %% "pekko-actor"   % PekkoVersion,
      "org.apache.pekko"           %% "pekko-stream"  % PekkoVersion,
      "org.apache.pekko"           %% "pekko-http"    % PekkoHttpVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6",
      "com.typesafe"                % "config"        % "1.4.9"
    ) ++ Seq("org.apache.pekko" %% "pekko-testkit" % PekkoVersion % Test) ++ Build.testDependencies.map(_ % Test)
  )

lazy val dsl = (project in file("dsl"))
  .dependsOn(client, client % "test->test", testkit % Test)
  .configs(Config.IntegrationTest)
  .settings(Config.testSettings)
  .settings(
    name := "dsl"
  )
//  .settings(excludeDependencies ++= Seq(ExclusionRule("org.apache.pekko")))

lazy val testkit = (project in file("testkit"))
  .dependsOn(client)
  .settings(
    name := "testkit",
    // scalatest is part of the published API (ClickhouseSpec extends SuiteMixin); logback is only the backend our own
    // tests log through, and must not land on a consumer's compile classpath.
    libraryDependencies ++= Seq(Build.scalaTest, Build.logback % Test)
  )
