package in.ds256.Assignment0;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * DS-256 Assignment 0
 * Code for finding frequent co-occuring hash-tags
 */
public class ​TopCoOccurrence  {
	
	public static void main(String[] args) throws IOException {	
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		/**
		 * Code goes here
	 	 */			
		
		sc.stop();
		sc.close();
	}

}