package botify

import org.scalatest.FunSuite
import spark.SparkContext
import org.apache.log4j.{Level, LogManager}

object SparkTest extends org.scalatest.Tag("SparkTest")

trait SparkTestBase extends FunSuite {

  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silence true to turn off spark logging
   */
  def sparkTest(name: String, silence: Boolean = true)(body: => Unit) {
    test(name, SparkTest){
      val origLogLevels = if (silence) {
        LogManager.getRootLogger.setLevel(Level.OFF)
        LogManager.getRootLogger.getLevel
      } else null
      sc = new SparkContext("local[2]", name)
      try {
        body
      }
      finally {
        sc.stop
        sc = null
        // To avoid Akka rebinding to the same port,
        // since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
        Thread.sleep(1000)
        if (silence) LogManager.getRootLogger.setLevel(origLogLevels)
      }
    }
  }
}