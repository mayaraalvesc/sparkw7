import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
/** Find the movies with the most ratings. */
object Popular_1 {

    def movieName(): Map[Int, String] = {
      implicit val codec = Codec("UTF-8")

      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      var movieNames: Map[Int, String] = Map()
      val lines = Source.fromFile("ml-100k/u.item").getLines()
      for (line <- lines) {
        var fields = line.split('|')
        if (fields.length > 1) {
          movieNames += (fields(0).toInt -> fields(1))
        }
      }
      movieNames
    }

    def MovRat(line: String): (Int, Int)  = {
      val fields = line.split("\t")
      val movie = fields(1).toInt
      val rating = fields(2).toInt
      (movie, rating)
    }

    def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
      val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
      val sc = new SparkContext(conf)
      // Read in each rating line
      val lines = sc.textFile("ml-100k/u.data")
      val rdd = lines.map(MovRat)
      val totalRt = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      val avgbyrt = totalRt.mapValues(x => x._1 / x._2)
      val sortedAvg = avgbyrt.sortBy(_._2, false)
      sortedAvg.collect().foreach(movie => {
        val movieNameStr = movieName().find(x => x._1 == movie._1).map(x => x._2)
        println(movie._1, movie._2, movieNameStr.toString)
      })
  }

}

