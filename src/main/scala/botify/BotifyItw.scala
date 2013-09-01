package botify

import spark.{RDD, SparkContext}
import SparkContext._
import scala.collection.Map

/**
 * Some simple Spark jobs for Botify
 */
object BotifyItw {

  // Operations
  def pagesByHttpCode(sc: SparkContext, urlinfos: RDD[String]) = {
    urlinfos.map(
      line => (line.split("\t")(3), 1) // 3 is http code column
    ).reduceByKey(_ + _)
  }

  def pagesByResponseTime(sc: SparkContext, urlinfos: RDD[String]) = {
    urlinfos.map(
      line => {
        val lineSplit = line.split("\t")
        // coloum 5 and 6 are delay times, take average here
        val delay = (lineSplit(5).toInt + lineSplit(6).toInt) / 2
        // 1 for > 1s, 2 for from 500 to 1s, 3 for < 500
        if (delay >= 1000)
          (1, 1)
        else if (delay > 500)
          (2, 1)
        else if (delay <= 500)
          (3, 1)
        else
          (4, 1)
      }
    ).reduceByKey(_ + _)
  }

  def urlidWithFirstDir(sc: SparkContext, urlids: RDD[String]) = {
    // regex to extract first directory
    val pattern =  """^(/[^/]*/?).*""".r

    urlids.map(
      line => {
        try {
          val lineSplit = line.split("\t")
          val urlid = lineSplit(0).toInt
          val path = lineSplit(3)
          val Some(all) = pattern.findFirstMatchIn(path)
          (urlid, all.group(1).toLowerCase)
        } catch {
          case e: Exception => (-1, "")
        }
      }
    )
  }

  def responseTimeByFirstDir(sc: SparkContext,
                             urlids: RDD[String],
                             urlinfos: RDD[String]) = {
    // map urls with its first directory
    // (urlid, firstDir)
    val firstDir = urlidWithFirstDir(sc, urlids)

    // map urls with its average delay in ms
    // (urlid, delay)
    val urlDelay = urlinfos.map(
      line => {
        val lineSplit = line.split("\t")
        val urlid = lineSplit(0).toInt
        val delay = (lineSplit(5).toInt + lineSplit(6).toInt) / 2
        (urlid, delay)
      }
    )

    // join them up
    val joined = firstDir.join(urlDelay).map(pair => pair._2)
    val averageDelay = joined.groupByKey.map(
      pair => (pair._1, pair._2.sum / pair._2.size)
    )

    averageDelay
  }

  def h1PercentageByFirstDir(sc: SparkContext, content: RDD[String],
                             firstDir: RDD[(Int,String)]) = {
    // filter only for h1 then map with urlids
    val urlWithH1 = content.filter(
      // 1 is the column of content type, 2 stands for h1
      line => line.split("\t")(1).toInt == 2
    ).map(
      line => {
        try {
          val lineSplit = line.split("\t")
          val urlid = lineSplit(0).toInt
          // val h1 = lineSplit(3)
          (urlid, 1)
        } catch {
          case e: Exception => (-1, "")
        }
      }
    ).distinct // multiple unique h1, repeated h1 are all considered only once

    // join them up
    val withH1CountsMap = firstDir.join(urlWithH1).map(_._2).countByKey
    // count #urls for each first directory
    val allCountsMap = firstDir.map(_.swap).countByKey
    val resultMap = withH1CountsMap.map {
      case (k,v) => k -> v.toDouble / allCountsMap.get(k).get
    }

    resultMap
  }

  // Helpers
  def parseJars(jars: String) = {
    jars.split(",").toSeq
  }

  def outputMap(m: Map[_, _], path: String) = {
    import java.io._
    val writer = new PrintWriter(new File(path))
    m.foreach {
      case (k,v) => {
        writer.write(k+"\t"+v+"\n")
      }
    }
    writer.close()
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(
        "Usage: BotifyItw <master> <in_folder> <out_folder> <operation> <jar_list>")
      System.exit(1)
    }

    val inputDir= args(1)
    val outputDir = args(2)
    val op = args(3).toInt
    val jars = args(4)

    val sc = new SparkContext(args(0), "BotifyItw",
      System.getenv("SPARK_HOME"), parseJars(jars))

    val urlinfos = sc.textFile(inputDir+"/urlinfos")
    val urlids = sc.textFile(inputDir+"/urlids")
    val content = sc.textFile(inputDir+"content")


    // pagesByHttpCode(sc, urlinfos).saveAsTextFile(outputDir+"res_httpcode")
    // pagesByResponseTime(sc, urlinfos).saveAsTextFile(outputDir+"res_responsetime")
    // responseTimeByFirstDir(sc, urlids, urlinfos).saveAsTextFile(outputDir+"res_averagetime")
    outputMap(h1PercentageByFirstDir(sc, content, urlidWithFirstDir(sc, urlids)),
      outputDir+"res_filledh1")
  }
}
