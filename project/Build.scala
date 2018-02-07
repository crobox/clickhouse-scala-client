import sbt._
object Build {

  val AkkaVersion     = "2.5.9"
  val AkkaHttpVersion = "10.0.11"
  val JacksonVersion  = "2.7.9"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.0.0",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.2.3")

}
