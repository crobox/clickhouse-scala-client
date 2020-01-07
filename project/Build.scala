import sbt._
object Build {

  val AkkaVersion     = "2.5.27"
  val AkkaHttpVersion = "10.1.11"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.1.0",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.3")

}
