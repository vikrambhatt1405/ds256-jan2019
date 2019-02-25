package in.ds256.Assignment1;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class bipartite {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("Bipartite");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);
		
		/**
		 * Code goes here...
		 */
		
		sc.stop();
		sc.close();
		
	}
}
