import sbt._

object Build {

  val PekkoVersion     = "1.0.2"
  val PekkoHttpVersion = "1.0.1"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.18", "ch.qos.logback" % "logback-classic" % "1.4.7")
}
