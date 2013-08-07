package projection

import scala.io.Source
import scala.collection.mutable

object LocalLoader extends App {
  // test a file for line TAB split
  def testFile(file: String) = {
    val res: mutable.Map[String, String] = mutable.Map()
    var i = 0
    Source.fromFile(file).getLines() foreach {
      line => {
        val spt = line.split("\t")
        res += ((spt(0), spt(1)))
        i += 1
      }
    }

    println(i)
    println(res.size)
  }

  // load a file in a sequence of String
  def loadFile(file: String): Seq[String] = {
    val res: mutable.ArrayBuffer[String] = mutable.ArrayBuffer()
    Source.fromFile(file).getLines() foreach {
      line => res += line
    }
    res
  }
}