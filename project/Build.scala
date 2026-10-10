import sbt.*

object Build {

  val PekkoVersion     = "1.7.1"
  val PekkoHttpVersion = "1.4.1"

  val scalaTest = "org.scalatest" %% "scalatest"       % "3.2.20"
  val logback   = "ch.qos.logback" % "logback-classic" % "1.6.5"

  val testDependencies = Seq(scalaTest, logback)
}
