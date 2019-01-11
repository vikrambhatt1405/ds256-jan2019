package in.ds256.Assignment0;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * DS-256 Assignment 0
 * Code for generating interaction graph
 */
public class InterGraph  {
	
	public static void main(String[] args) throws IOException {	
		
		String inputFile = args[0]; // Should be some file on HDFS
		String vertexFile = args[1]; // Should be some file on HDFS
		String edgeFile = args[2]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		/**
		 * Code goes here
	 	 */
		
		sc.stop();
		sc.close();
	}

}