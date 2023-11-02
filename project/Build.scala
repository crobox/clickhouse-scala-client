import sbt._

object Build {

  val PekkoVersion     = "1.0.1"
  val PekkoHttpVersion = "1.0.0"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.15", "ch.qos.logback" % "logback-classic" % "1.4.7")
}
