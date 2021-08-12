import sbt._

object Build {

  val AkkaVersion     = "2.6.15"
  val AkkaHttpVersion = "10.2.6"
//  val AkkaHttpVersion = "10.2.6" # breaking changes with version 10.1

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.9",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.5")

}
