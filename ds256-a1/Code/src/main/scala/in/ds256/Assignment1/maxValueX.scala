package in.ds256.Assignment1;

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.Tuple2

object maxValueX {
  def main(args: Array[String]): Unit = {
    
    val vertexFile = args(0); // Should be some file on HDFS
    val edgeFile = args(1); // Should be some file on HDFS
		val outputFile = args(2); // Should be some file on HDFS
    
    val spark = SparkSession.builder.appName("Max Value Propogation with GraphX").getOrCreate()
    val sc = spark.sparkContext
    
    val inputVertexRDD = sc.textFile(vertexFile)
    val inputEdgeRDD = sc.textFile(edgeFile)
    
    val vertexRDD: RDD[(Long, Long)] = inputVertexRDD.map(vertexID => { 
      ( vertexID.toLong, vertexID.toLong) 
    })
    
    val edgeRDD: RDD[Edge[Long]] = inputEdgeRDD.map(edge => { 
      val tokens = edge.split(" ");
      Edge[Long]( tokens(0).toLong, tokens(1).toLong, 0L) 
    })
    
    val graph: Graph[Long, Long] = Graph(vertexRDD, edgeRDD)
    
    val initialMessage = -1L
    val maxIterations = 100
    val pregelGraph = Pregel(graph, initialMessage, maxIterations, EdgeDirection.Either)(
        vprog = (id, attr, msg) => { math.max(attr, msg) },
        sendMsg = edge => { 
          if(edge.dstId < edge.srcAttr)
            Iterator((edge.dstId, edge.srcAttr)) 
          else
            Iterator.empty
        },
        mergeMsg = (a, b) => math.max(a, b)
    )
    
    // store's computed vertexState to HDFS
    pregelGraph.vertices.saveAsTextFile(outputFile)
    
    sc.stop();
  }
}
