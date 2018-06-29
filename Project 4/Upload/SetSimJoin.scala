/* COMP9313
 * Assignment 4: SetSimJoin in scala
 * 
 * 18S1 UNSW
 * z5102446 Hao Chen
 * */  
package comp9313.ass4
 
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.function.ToLongFunction
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
 
/* Function for the second sort
 * */  
class SecondarySortKey(val second:Double, val first:Int) extends Ordered[SecondarySortKey] with Serializable
{
  def compare(that:SecondarySortKey):Int =
  {
    if(this.first - that.first != 0)
    {
      this.first.compareTo(that.first)
    }
    else
    {
      this.second.compareTo(that.second)
    }
  } 
}
 
object SetSimJoin 
{
    def main(args: Array[String])
  {
    /* Step 1:
     * 
     * Read the input from the command line and split the input
     * into every single line.
     * */  
    val inputFile = args(0)
    val ouputFolder = args(1)
    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin")//.setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFile).flatMap(line => line.split("\n"))
    
    /* Step 2:
     * 
     * Calculate the frequency of every appeared element,
     * then create a list in which the elements sorted in the frequency ascending order.
     * 
     * This step is skiped when running the code on AWS to faster the processing
     * */
    val elements = lines.map { x => x.drop(x.split(" ")(0).length.toInt + 1) }.flatMap { x => x.split(" ") }.map { x => (x, 1) }.reduceByKey(_+_)
    val tokens = elements.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1)).map(line => (new SecondarySortKey(line._1.toDouble, line._2.toInt),line)).sortByKey().map(x => x._2._1)
    
     /* Step 3:
      * 
      * Map every line obtained from input to the format of seq(i), (RID, seq)), 
      * and the seq(i) are the elements from the head of seq.
      * Group by key and map the x._2 as array
      * 
      * How much seq(i) are taken are based on the prefix length.
      */   
    val sortedLines1 = lines.map { line => line.split(" ") }.flatMap { x => {var RID = x(0)
                                                                             var seq = x.drop(1).sortBy(tokens => tokens)
                                                                             //var seq = x.drop(1)
                                                                             
                                                                             val PreLen = (seq.length - Math.ceil(seq.length*threshold) + 1).toInt
                                                                             for(i <- 0 until PreLen) yield(seq(i), (RID, seq))
                                                                            }
                                                                           }.groupByKey().map( x => {(x._1,x._2.toArray)})

     /* Step 4:
      * Use two "for" loop to compare each pair of value under the same key,
      * here we use hash map to record the computed similarity. 
      * 
      * Return the hash map in the end.
      */                                                                              
    val sortedLines2 = sortedLines1.flatMap(x => {
                                              var myMap: Map[(String, String),Double] = Map()
                                              for(i <- 0 until x._2.size)
                                              {//first for loop
                                                for(j <- i until x._2.size)
                                                {//second for loop
                                                  if(i != j)
                                                  {//compare
                                                    val inter = (x._2(i)._2.toSet & x._2(j)._2.toSet).size.toDouble
                                                    val union = (x._2(i)._2.toSet.size + x._2(j)._2.toSet.size).toDouble - inter
                                                    val div = inter/union
                                                    if(div >= threshold)
                                                    { 
                                                      if(!myMap.contains((x._2(i)._1, x._2(j)._1)) )
                                                      {
                                                          myMap += ( (x._2(i)._1, x._2(j)._1) -> div )
                                                      }
                                                    }
                                                  }
                                                 } 
                                               }
                                               myMap
                                              }
                                        )
                       
     /* Step 5:
      * Distinct the result get from the previous step,
      * then sort the key-value pair as required 
      * and get the value part of the final result.
      * 
      * Save it as required.
      */         
    sortedLines2.distinct().map(line => (new SecondarySortKey(line._1._2.toDouble, line._1._1.toInt),line)).sortByKey().map(x =>x._2._1+"\t"+x._2._2).saveAsTextFile(ouputFolder)

  }
}
