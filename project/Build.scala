import sbt._
object Build {

  val AkkaVersion     = "2.5.32"
  val AkkaHttpVersion = "10.2.4"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.6",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.3")

}
