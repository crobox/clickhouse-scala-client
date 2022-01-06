import sbt.Keys._
import sbt._

object Config {
  val CustomIntegrationTest = config("it") extend Test

  private lazy val testAll = TaskKey[Unit]("tests")

  private lazy val unitSettings = Seq(
    Test / fork := true,
    Test / parallelExecution := false
  )

  private lazy val itSettings =
  inConfig(CustomIntegrationTest)(Defaults.testSettings) ++
  Seq(
    CustomIntegrationTest / fork := false,
    CustomIntegrationTest / parallelExecution := false,
    CustomIntegrationTest / scalaSource := baseDirectory.value / "src/it/scala"
  ) ++ inConfig(IntegrationTest)(Defaults.testSettings)

  lazy val testSettings = itSettings ++ unitSettings ++ Seq(
    testAll := (CustomIntegrationTest / test).dependsOn(Test / test).value
  )
}
