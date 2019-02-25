package in.ds256.Assignment1;

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.Tuple2

object ktrussX {
  
  def main(args: Array[String]): Unit = {
    
    val vertexFile = args(0); // Should be some file on HDFS
    val edgeFile = args(1); // Should be some file on HDFS
		val outputFile = args(2); // Should be some file on HDFS
    
    val spark = SparkSession.builder.appName("KTruss with GraphX").getOrCreate()
    val sc = spark.sparkContext
    
    val inputVertexRDD = sc.textFile(vertexFile)
    val inputEdgeRDD = sc.textFile(edgeFile)
    
    /**
     * Code goes here...
     */
  }
}
