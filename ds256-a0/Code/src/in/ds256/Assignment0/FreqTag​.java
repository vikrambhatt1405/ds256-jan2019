package in.ds256.Assignment0;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.*;

import scala.Tuple2;

/**
 * DS-256 Assignment 0
 * Code for generating frequency distribution per hashtag
 */
public class FreqTag  {
	
	public static void main(String[] args) throws IOException {	
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		////spark connection made
		SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputFile);
		
		
		////this will filter out all the deleted tweets
		JavaRDD<String> counts = 
				lines.filter(line ->{
					try {
						JSONParser parser = new JSONParser();
						Object jsonObj = parser.parse(line);
						JSONObject jsonObject = (JSONObject) jsonObj;
						JSONObject entities = (JSONObject)jsonObject.get("entities");
						JSONArray hashtags =(JSONArray) entities.get("hashtags");
						Integer.toString(hashtags.size());
						return true;
						}
						catch(Exception e) {
							return false;
						}
					
				})
				;
		////counts2 has 
		JavaPairRDD<String,Integer> counts2 = 
				counts.map(line -> {
					JSONParser parser = new JSONParser();
					Object jsonObj = parser.parse(line);
					JSONObject jsonObject = (JSONObject) jsonObj;
					JSONObject entities = (JSONObject)jsonObject.get("entities");
					JSONArray hashtags =(JSONArray) entities.get("hashtags");
					return Integer.toString(hashtags.size());
				})
				.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a,b)-> a+b)
				.mapToPair(line -> new Tuple2<>(line._2,line._1))
				.sortByKey(false)
				.mapToPair(line -> new Tuple2<>(line._2,line._1))
				;
		////writing to file
		counts2.repartition(1).saveAsTextFile(outputFile);	
		
		
		sc.stop();
		sc.close();
	}



}