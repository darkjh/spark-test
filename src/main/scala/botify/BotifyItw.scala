package botify

import spark.{RDD, SparkContext}
import SparkContext._
import scala.collection.Map

/**
 * Some simple Spark jobs for Botify
 *
 * TODO type aliases, ex. type url = int
 * TODO better way to organize code
 * TODO unit testing with a mocked crawl data
 * TODO test diff. ways of output, maybe a custom outputFormat
 */
object BotifyItw {

  // Operations
  def pagesByHttpCode(sc: SparkContext, urlinfos: RDD[String]) = {
    urlinfos.map(
      line => (line.split("\t")(3), 1) // 3 is http code column
    ).reduceByKey(_ + _).map(
      p => p._1+"\t"+p._2  // tsv formatting
    )
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
    ).reduceByKey(_ + _).map(
      p => p._1+"\t"+p._2  // tsv formatting
    )
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
          case e: Exception => (-1, "unknown")
        }
      }
    )
  }

  def responseTimeByFirstDir(sc: SparkContext,
                             urlids: RDD[String],
                             urlinfos: RDD[String],
                             firstDir: RDD[(Int, String)]) = {
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

    averageDelay.map(
      p => p._1+"\t"+p._2  // tsv formatting
    )
  }

  def h1PercentageByFirstDir(sc: SparkContext,
                             content: RDD[String],
                             firstDir: RDD[(Int, String)]) = {
    // filter only for h1 then map with urlids
    val urlWithH1 = content.map {
      line => {
        try {
          val lineSplit = line.split("\t")
          val urlid = lineSplit(0).toInt
          val tagType = lineSplit(1).toInt
          tagType match {
            case 2 => (urlid, 1)  // 2 stands for h1 tag
            case _ => (-1 , -1)
          }
        } catch {
          case e: Exception => (-1, -1)
        }
      }
    }.distinct()

    // join them up
    val fdWithH1Count = firstDir.join(urlWithH1).map(_._2).reduceByKey(_ + _)
    // count #urls for each first directory
    val fdAllCountsMap = firstDir.map(p => (p._2, 1)).reduceByKey(_ + _)

    val result = fdWithH1Count.join(fdAllCountsMap).map {
      case (fd, (h1, all)) => {
        (fd, h1.toDouble / all.toDouble)
      }
    }

    result.map(
      p => p._1+"\t"+p._2  // tsv formatting
    )
  }

  // TODO master
  def linkRelationByFirstDir(sc: SparkContext,
                             links: RDD[String],
                             firstDir: RDD[(Int, String)]) = {
    // use a global lookup table, spark does not support nested-RDD
    // urlid (int) -> first directory (string)
    val firstDirUrlMap = firstDir.collectAsMap()

    // map links to (first directory, dest urlid) pairs
    val linkRelationship = links.map(
      line => {
        val lineSplit = line.split("\t")
        val from = lineSplit(2).toInt
        val to = lineSplit(3).toInt
        (firstDirUrlMap.getOrElse(from, "unknown"), to)      // for q6
        // (firstDirUrlMap.getOrElse(to, "external"), from)  // for q7
      }
    )
    // group up by first directory
    // result is (first directory, [urlids ...])
    val groupBySrc = linkRelationship.groupByKey

    // replace dest urlids with its first directory by using lookup table
    // then do a local groupby in the list
    val resultMap = groupBySrc.map {
      case (k, s) => {
        // if a dest url can't be looked up, then it's a external link
        val ss = s.map(k => firstDirUrlMap.getOrElse(k, "external"))
        (k, ss.groupBy(p => p).map{ case (kk,vv) => kk -> vv.size })
      }
    }

    resultMap.map {
      case (src, m) => {
        val sb = new StringBuilder
        m.foreach {
          case (k, v) => sb.append(src+"\t"+k.toString+"\t"+v.toString+"\n")
        }
        sb.deleteCharAt(sb.size - 1)
        sb.toString()
      }
    }
  }

//  def q7(sc: SparkContext,
//         links: RDD[String],
//         firstDir: RDD[(Int, String)]) = {
//    val l = links.map(f = line => {
//      val lineSplit = line.split("\t")
//      val from = lineSplit(2).toInt
//      val to = lineSplit(3).toInt
//      (from, to)
//    })
//
//    // (fd in, fd out)
//    val firstDirRelations = l.join(firstDir).map(_._2).join(firstDir).map(_._2)
//    firstDirRelations.groupByKey().map {
//      case (k, s) => {
//        (k, s.groupBy(p => p).map{ case (kk,vv) => kk -> vv.size })
//      }
//    }
//  }

  // Helpers
  def parseJars(jars: String) = {
    jars match {
      case "" => Seq.empty[String]
      case _ => jars.split(",").toSeq
    }
  }

  // process all operations in an efficient way
  def processAll(sc: SparkContext,
                 outputPath: String,
                 urlinfos: RDD[String],
                 urlids: RDD[String],
                 content: RDD[String],
                 links: RDD[String]) = {
    pagesByHttpCode(sc, urlinfos).saveAsTextFile(
      outputPath+"http_code")
    pagesByResponseTime(sc, urlinfos).saveAsTextFile(
      outputPath+"response_time")

    // cache fd for performance
    val fd = urlidWithFirstDir(sc, urlids)
    fd.cache()

    responseTimeByFirstDir(sc, urlids, urlinfos, fd)
      .saveAsTextFile(outputPath+"fd_time")
    h1PercentageByFirstDir(sc, content, fd).saveAsTextFile(
      outputPath+"fd_h1")
    linkRelationByFirstDir(sc, links, fd).saveAsTextFile(
      outputPath+"fd_outbound")
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(
        "Usage: BotifyItw <master> <in_folder> <out_folder> <operation> <jar_list>")
      System.exit(1)
    }

    val inputPath = args(1)
    val outputPath = args(2)
    val op = args(3).toInt
    val jars = args(4)

    val sc = new SparkContext(args(0), "BotifyItw",
      System.getenv("SPARK_HOME"), parseJars(jars))

    val urlinfos = sc.textFile(inputPath+"urlinfos.txt")
    val urlids = sc.textFile(inputPath+"urlids.txt")
    val content = sc.textFile(inputPath+"content.txt")
    val links = sc.textFile(inputPath+"links.txt")

    op match {
      case 0 => processAll(sc, outputPath, urlinfos, urlids, content, links)
      case 1 => pagesByHttpCode(sc, urlinfos)
        .saveAsTextFile(outputPath+"res_httpcode")
      case 2 => pagesByResponseTime(sc, urlinfos)
        .saveAsTextFile(outputPath+"res_responsetime")
      case 3 => responseTimeByFirstDir(sc, urlids, urlinfos, urlidWithFirstDir(sc, urlids))
        .saveAsTextFile(outputPath+"res_averagetime")
      case 4 | 5 => h1PercentageByFirstDir(sc, content, urlidWithFirstDir(sc, urlids)).
        saveAsTextFile(outputPath+"res_filledh1")
      case 6 | 7 => linkRelationByFirstDir(sc, links, urlidWithFirstDir(sc, urlids)).
        saveAsTextFile(outputPath+"res_outbound")
    }
  }
}