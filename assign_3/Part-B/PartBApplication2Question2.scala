package cs744.a3

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object PartBApplication2Question2 {
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

    val graph = Graph(vertices, edges.edges)
    
    // build a new graph with VD = <outdegree, num of words>
    val outdegAndSizeGraph = graph.outerJoinVertices(graph.outDegrees){(_, vData, degOpt) => (degOpt.getOrElse(0), vData.length)}.persist()

    // reduce
    val res = outdegAndSizeGraph.vertices.reduce {
      (a, b) => if (a._2._1 > b._2._1 || (a._2._1 == b._2._1 && a._2._2 > b._2._2)) a else b
    }
    println("The most popular vertex: " + res._1 + " [outdegree=" + res._2._1 + ", numOfWords=" + res._2._2 + "]")
  }
}
