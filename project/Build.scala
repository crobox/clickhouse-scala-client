import sbt._

object Build {

  val AkkaVersion     = "2.6.20"  // do *not* upgrade to 2.7+ (commercial license)
  val AkkaHttpVersion = "10.2.10" // do *not* upgrade to 10.4+ (commercial license)

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.15",
                             "com.typesafe.akka" %% "akka-testkit"   % AkkaVersion,
                             "ch.qos.logback"    % "logback-classic" % "1.4.6")
}
