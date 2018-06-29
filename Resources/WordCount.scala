package comp9313.lab10

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val input =  sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(x => (x, 1)).reduceByKey(_+_) 
    
    counts.saveAsTextFile(outputFolder)    
  }
}
