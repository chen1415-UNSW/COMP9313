package comp9313.ass3

import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.function.ToLongFunction

object Problem2 
{
    def main(args: Array[String])
    {
      //Read the input from the commond line and set the master
      val inputFile = args(0)
      val t = args(1).toLong
      val conf = new SparkConf().setAppName("Problem2").setMaster("local")
      
      //create the spark Context
      val sc = new SparkContext(conf)
      val data:RDD[String] = sc.textFile(inputFile)
      
      //split the given file into lines and split the lines to make fromNode, toNode, and Weight
      val lines = data.flatMap { x => x.split("\n") }
      val edgeRDD = lines.map(x => x.split(" ")).map { x => Edge(x(1).toLong, x(2).toLong, x(3).toDouble) }
      
      //construct the graph by the edge
      val graph = Graph.fromEdges(edgeRDD, 1L)
      
      //Copied from PPT to compute the aingle source shortest path
      val startGraph = graph.mapVertices((id, _) => if(id==t) 0.0 else Double.PositiveInfinity)
      val sssp = startGraph.pregel(Double.PositiveInfinity)(
          (id, dist, newDist) => math.min(dist, newDist), 
          triplet =>{
            if(triplet.srcAttr + triplet.attr < triplet.dstAttr)
            {
              Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
            }else{
              Iterator.empty
            }
          }, 
          (a, b) => math.min(a, b)
      )
     
      //if the distance is not inf, then it can be reached, print the result
      val num = sssp.vertices.map(x => if(x._2!=Double.PositiveInfinity)(t,1)else(t,0)).reduceByKey(_+_)
      val res = num.map(x => x._2-1).foreach { println }
//      res.foreach { println }
      
    }
}