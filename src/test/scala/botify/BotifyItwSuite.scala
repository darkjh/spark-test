package botify

import org.scalatest.matchers.ShouldMatchers

/**
 * Test suite for Botify itw spark jobs
 */
class BotifyItwSuite extends SparkTestBase with ShouldMatchers {

  val fixturePath = "src/test/resources/fixtures/crawl/"

  sparkTest("page counts by http code") {
    val urlinfos = sc.textFile(fixturePath+"urlinfos")
    val result = BotifyItw.pagesByHttpCode(sc, urlinfos).collect()
    result.size should be (3)
    val sorted = result.sorted
    sorted(0) should be ("200\t3")
    sorted(1) should be ("301\t1")
    sorted(2) should be ("302\t1")
  }

  sparkTest("page counts by response time") {
    val urlinfos = sc.textFile(fixturePath+"urlinfos")
    val result = BotifyItw.pagesByResponseTime(sc, urlinfos).collect()
    result.size should be (3)
    val sorted = result.sorted
    sorted(0) should be ("1\t1")
    sorted(1) should be ("2\t1")
    sorted(2) should be ("3\t3")
  }

  sparkTest("response time by first dir") {
    val urlinfos = sc.textFile(fixturePath+"urlinfos")
    val urlids = sc.textFile(fixturePath+"urlids")
    val fd = BotifyItw.urlidWithFirstDir(sc, urlids)
    val result = BotifyItw.responseTimeByFirstDir(sc, urlids, urlinfos, fd).collect()

  }
}
