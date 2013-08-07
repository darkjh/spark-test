package projection

import spark.SparkContext
import SparkContext._

/**
 * The naive, SQL like projection approach using an inner join
 */
object JoinProjection extends App {
  if (args.length < 4) {
    System.err.println(
      "Usage: JoinProjection <master> <input> <output> <support> <jar_list>")
    System.exit(1)
  }

  val userPath = "/user/port"
  val inputPath = args(1)
  val outputPath = args(2)
  val jars = args(4)
  val hdfs = "hdfs://kxhdp1-master1:54310"
  val parallel = 32

  def parseJars(jars: String) = {
    jars.split(",").toSeq
  }

  val workerConf = Map(("SPARK_MEM", "2g"))

  val sc = new SparkContext(args(0), "JoinProjection",
    System.getenv("SPARK_HOME"), parseJars(jars), workerConf)

  val support = args(3).toInt

  val raw = sc.textFile(hdfs+userPath+inputPath)
  // val raw = sc.parallelize(LocalLoader.loadFile("target/scala-2.9.2/msd_test"))

  val filtered = raw.map(
    line => {
//      val lineSplit = try {
//        line.split(TAB)
//      } catch {
//        case _ =>
//          System.err.println(line)
//          Array("-1", "-1")  // workaround
//      }
      // System.err.println(TAB) // TAB here is null!
      val lineSplit = line.split("\t")
      (lineSplit(0), lineSplit(1))
    }
  )

  val projected = filtered.join(filtered, parallel).filter(
    pair => {
      val p = pair._2
      p._1.toInt < p._2.toInt
    }
  ).map(
    pair => (pair._2, 1)
  ).reduceByKey(_ + _, parallel).filter(
    pair => pair._2 >= 2
  )

  // projected.cache()
  println(projected.count)
  // projected.saveAsTextFile(hdfs+outputPath)
}