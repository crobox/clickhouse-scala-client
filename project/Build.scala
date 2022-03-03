import sbt._

object Build {

  val AkkaVersion     = "2.6.18"
  val AkkaHttpVersion = "10.2.8"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.11",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.10")
}