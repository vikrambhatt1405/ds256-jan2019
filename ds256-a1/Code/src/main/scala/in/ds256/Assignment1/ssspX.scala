package in.ds256.Assignment1;

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.Tuple2

object ssspX {
  def main(args: Array[String]): Unit = {
    
    val vertexFile = args(0); // Should be some file on HDFS
    val edgeFile = args(1); // Should be some file on HDFS
		val outputFile = args(2); // Should be some file on HDFS
    
    val spark = SparkSession.builder.appName("Single Source Shortest Path with GraphX").getOrCreate()
    val sc = spark.sparkContext
    
    val inputVertexRDD = sc.textFile(vertexFile)
    val inputEdgeRDD = sc.textFile(edgeFile)
    
    val vertexRDD: RDD[(Long, Double)] = inputVertexRDD.map(vertexID => { 
      ( vertexID.toLong, Double.PositiveInfinity) 
    })
    
    val edgeRDD: RDD[Edge[Double]] = inputEdgeRDD.map(edge => { 
      val tokens = edge.split(" ");
      Edge[Double]( tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble) 
    })
    
    val initGraph: Graph[Double, Double] = Graph(vertexRDD, edgeRDD)
    
    val sourceId = 1 //Source VertexID
    val graph = initGraph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    
    val initialMessage = Double.PositiveInfinity
    val maxIterations = 100
    
    val pregelGraph = Pregel(graph, initialMessage, maxIterations, EdgeDirection.Either)(
        vprog = (id, dist, msg) => { math.min(dist, msg) },
        sendMsg = edge => { 
          if( (edge.srcAttr + edge.attr) < edge.dstAttr)
            Iterator((edge.dstId, edge.srcAttr + edge.attr)) 
          else
            Iterator.empty
        },
        mergeMsg = (a, b) => math.min(a, b)
    )
    
    // store's computed vertexState to HDFS
    pregelGraph.vertices.saveAsTextFile(outputFile)
    
    sc.stop();
  }
}
