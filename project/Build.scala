import sbt._
object Build {

  val AkkaVersion     = "2.5.19"
  val AkkaHttpVersion = "10.1.6"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.0.5",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.3")

}
