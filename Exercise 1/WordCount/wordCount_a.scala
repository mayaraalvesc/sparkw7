import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
object wordCount_a {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val cont = new SparkContext("local[2]", "word_1")
    val input = cont.textFile("TheHungerGames.txt")
    val words = input.flatMap(x => x.split("\\W+")) //split words
    val lowercase = words.map(x => x.toLowerCase) // apply lower case to all
    val wcont = lowercase.map(x => (x, 1)).reduceByKey((x, y) => x + y) //count words
    val sorted = wcont.map(x => (x._2, x._1))
      .sortByKey(false,1).foreach(println) //print results descending order
      //.sortByKey(true,1).foreach(println) //print results asc

  }
}