package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * DS-256 Assignment 0
 * Code for generating frequency distribution per hashtag
 */
public class FreqTag  {
	
	public static void main(String[] args) throws IOException {	
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rdd1 = sc.textFile(inputFile);
		JavaRDD<String> rdd2 = rdd1.filter(s -> {
			try {
			JSONObject ob=(JSONObject)new JSONParser().parse(s);
			if(ob.get("delete")!=null)
			{
				return false;
			}
			else 
				return true;
			
			}catch(Exception e) {
				return false;
			}
		});
				
		
		
		JavaPairRDD<Integer,Integer> rdd4=rdd2.map(f ->{
			try {
			JSONObject ob1=(JSONObject)new JSONParser().parse(f);
			JSONArray ja=(JSONArray)((JSONObject)ob1.get("entities")).get("hashtags");
			ArrayList<String> ht=new ArrayList<String>();
			for(Object o: ja){
			    if ( o instanceof JSONObject ) {
			    	JSONObject q=(JSONObject)o;
			    	ht.add((String)q.get("text"));
			    }
			}
			return ht.size();
			}
			catch(Exception e){
				return -1;
			}
		}).mapToPair(size -> new Tuple2<>(size, 1));
			
		JavaPairRDD<Integer,Integer> rdd5 = 
					rdd4.reduceByKey((a,b) -> a+b);
			
		rdd5.repartition(1).saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
	}

}
