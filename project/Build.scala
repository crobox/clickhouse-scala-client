import sbt._

object Build {

  val PekkoVersion     = "1.1.4"
  val PekkoHttpVersion = "1.2.0"

  val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.19", "ch.qos.logback" % "logback-classic" % "1.5.18")
}
