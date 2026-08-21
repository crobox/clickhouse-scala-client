import sbt.Keys._
import sbt._
import sbt.librarymanagement.Configuration

object Config {

  /**
   * Our own integration-test configuration rather than sbt's built-in `IntegrationTest`, which is deprecated since sbt
   * 1.9 and removed in sbt 2. Declared with the same id and ivy name, so `IntegrationTest/test` keeps working -- that
   * is what CI invokes.
   *
   * The build previously declared `config("it") extend Test` *and* separately configured sbt's `IntegrationTest`. Those
   * are two distinct Configuration objects sharing the ivy name "it": the custom one was reachable as `it:test`, the
   * built-in as `IntegrationTest/test`. Both pointed at src/it/scala, so every integration test was reachable under two
   * names, and only the deprecated one served CI.
   */
  val IntegrationTest: Configuration = Configuration.of("IntegrationTest", "it").extend(Test)

  private lazy val testAll = TaskKey[Unit]("tests")

  private lazy val unitSettings = Seq(
    Test / fork              := true,
    Test / parallelExecution := false
  )

  private lazy val itSettings =
    inConfig(IntegrationTest)(Defaults.testSettings) ++
      Seq(
        IntegrationTest / fork              := false,
        IntegrationTest / parallelExecution := false,
        IntegrationTest / scalaSource       := baseDirectory.value / "src/it/scala"
      )

  lazy val testSettings = itSettings ++ unitSettings ++ Seq(
    testAll := (IntegrationTest / test).dependsOn(Test / test).value
  )
}
