import Build._

lazy val root = Project("clickhouse-scala-client", file("."))
  .settings(
  	organization := "com.crobox",
  	name := "clickhouse-scala-client",
  	version := "0.0.1",
  	scalaVersion := "2.11.8",
  	publish := {},
  	publishArtifact := false,
  	libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
			"com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % Test
	)
  )
