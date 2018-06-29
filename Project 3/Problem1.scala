/* Written by Hao Chen
 * For COMP9313 Ass3 Problem1
 * 2018S1 UNSW
 * */

package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Problem1 
{
  def main(args: Array[String])
  {
    //Read the input from the commond line and set the master
    val inputFile = args(0)
    val ouputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
//    System.out.println(inputFile);
    
    //create the spark Context
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    
    //split the input line by space, and then get the words[1] and words[3], which are nodes and their out-going edges
    val words = input.flatMap(line => line.split("\n"))
    val out = words.map(words =>( words.split(" ")(1).toInt,words.split(" ")(3).toDouble))
  
    //First give all nodes with value 1 and sum all values with the same key, then we can get key-value pairs node - sum of edge - edge counts
    val out2 = out.mapValues(x => (x,1)).sortByKey()
    val out3 = out2.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
    val out4 = out3.sortByKey();
    //use values sum of edge - edge counts, and divide these two value, then we get the key-value pairs: nodes - average weights
    val out5 = out4.mapValues(x => x._1/x._2)
    
    //we reverse the key-value and sort the key(which is the average weights) by the descending order and reserve it again
    val out6 = out5.map(x => (x._2, x._1)).sortByKey(false)
    val out7 = out6.map(x => (x._2, x._1))
   
    
    //save the file as required
    out7.map(x => x._1 + "\t" + x._2).saveAsTextFile(ouputFolder)
    
  }
  
}