package org.encalmo.aws

import scala.concurrent.duration.Duration
import scala.io.AnsiColor

class TestSuite extends munit.FunSuite {

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms // ++ Seq(failFastTestTransform)

  override val munitTimeout = Duration(30, "s")

  final val debugModeProperties = Map("AWS_CLIENT_DEBUG_MODE" -> "ON")

  final lazy val localAwsSecurityCredentials: Map[String, String] =
    SetupAwsCredentials("encalmo-sandbox")
      .map(_.toEnvironmentVariables)
      .getOrElse(Map.empty)

  final inline def assertMultipleTimes(test: => Unit): Unit =
    (1 to 100).foreach(_ => test)

  inline val BLUE_LIGHT = "\u001b[38;5;33m"

  override def beforeEach(context: BeforeEach): Unit =
    print(BLUE_LIGHT)
    println("-" * 90)
    println(
      s"${this.getClass
          .getSimpleName()}: ${BLUE_LIGHT}${AnsiColor.BOLD}${context.test.name}${AnsiColor.RESET}"
    )

  def dynamoDbDiff(startTag: String, endTag: String)(using stub: AwsClientStatefulStub): Option[String] =
    for {
      start <- stub.dynamoDb.getSnapshot(startTag)
      end <- stub.dynamoDb.getSnapshot(endTag)
    } yield new munit.diff.Diff(start, end).unifiedDiff

  def failFastTestTransform: TestTransform =
    new TestTransform(
      "failFast",
      { t =>
        t.withBodyMap {
          _.transform(
            identity,
            e => {
              try {
                print(s"${AnsiColor.RESET}${AnsiColor.RED_B}${AnsiColor.WHITE}")
                println(e)
                e.getStackTrace().take(5).foreach(println)
                println()
                println(
                  s"${getClass.getName} ${AnsiColor.BOLD}${t.name}${AnsiColor.RESET}${AnsiColor.RED_B}${AnsiColor.WHITE} at ${t.location}${AnsiColor.RESET}"
                )
                println()
              } catch { case _ => () /*ignore*/ }
              if (System.getenv("TESTS_DO_NOT_FAIL_FAST") == null) then System.exit(2)
              e
            }
          )(munitExecutionContext)
        }
      }
    )

}
