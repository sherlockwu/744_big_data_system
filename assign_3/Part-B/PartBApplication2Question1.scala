package cs744.a3

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PartBApplication2Question1 {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // load edges and vertices and composite the graph
    val edges = GraphLoader.edgeListFile(sc, "/assignment3/PartB/Application2/data/edges.txt")
    val vertices = sc.textFile("/assignment3/PartB/Application2/data/vertices.txt").map(line => line.split(","))
                     .zipWithIndex().map(_.swap)

    val graph = edges.outerJoinVertices(vertices) { (_, _, vData) => 
      vData match { 
        case Some(data) => data
        case None => Array() 
      }
    }
    
    // filter
    val res = graph.triplets.filter {
      triplet => triplet.srcAttr.length > triplet.dstAttr.length
    }.count()
    println("Number of edges where the number of words in the source vertex is strictly larger than the number of words in the destination vertex: " + res)
  }
}
