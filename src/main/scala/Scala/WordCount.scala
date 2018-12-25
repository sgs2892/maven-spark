package Scala

import org.apache.spark._
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = "output"
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(_.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey(_+_)
    counts.repartition(3).saveAsTextFile(outputFile)
  }
}
