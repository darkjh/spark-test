package projection

import spark.SparkContext
import SparkContext._
import spark.storage.StorageLevel

/**
 * The naive, SQL like projection approach using an inner join
 */
object JoinProjection {
  def parseJars(jars: String) = {
    jars.split(",").toSeq
  }

  def main(args: Array[String]) {
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
    val support = args(3).toInt

    val sc = new SparkContext(args(0), "JoinProjection",
      System.getenv("SPARK_HOME"), parseJars(jars))

    val raw = sc.textFile(hdfs+userPath+inputPath)
    // val raw = sc.parallelize(LocalLoader.loadFile("target/scala-2.9.2/msd_test"))

    val filtered = raw.map(
      line => {
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
      pair => pair._2 >= support
    )

    // projected.persist(StorageLevel.MEMORY_AND_DISK)
    println(projected.count)
    // projected.saveAsTextFile(hdfs+outputPath)
  }
}