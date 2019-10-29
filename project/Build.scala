import sbt._
object Build {

  val AkkaVersion     = "2.5.26"
  val AkkaHttpVersion = "10.1.10"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.0.8",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.3")

}
