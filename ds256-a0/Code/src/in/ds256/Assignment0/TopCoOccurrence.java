package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
//http://turing.cds.iisc.ac.in:8088/proxy/application_1547011148574_0076/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * DS-256 Assignment 0
 * Code for finding frequent co-occuring hash-tags
 */
public class ​TopCoOccurrence  {
	
	public static void subset(String[] A, int k, int start, int currLen, boolean[] used,ArrayList<Tuple2<String, String>> tuples) {
		//Tuple2< String, String> t2=new Tuple2<String, String>(null, null);
		if (currLen == k) {
			ArrayList<String> arr= new ArrayList<>();
			for (int i = 0; i < A.length; i++) {
				if (used[i] == true) {
					//System.out.print(A[i] + " ");
					arr.add(A[i]);
				}
			}
			Collections.sort(arr);
			tuples.add(new Tuple2<String, String>(arr.get(0), arr.get(1)));
			//System.out.println();
			return;
		}
		if (start == A.length) {
			return;
		}
		// For every index we have two options,
		// 1.. Either we select it, means put true in used[] and make currLen+1
		used[start] = true;
		subset(A, k, start + 1, currLen + 1, used,tuples);
		// 2.. OR we dont select it, means put false in used[] and dont increase
		// currLen
		used[start] = false;
		subset(A, k, start + 1, currLen, used,tuples);
	}

	public static void main(String args[]) throws IOException, ParseException {
		
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		////spark connection made
		SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputFile);
		
		
		////here i will parse and filter out all the deleted tweets
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
		////count2 has all the pair of hashtags that appeared together
		JavaRDD<Tuple2<String, String>> count2 =
				counts.flatMap(line -> {
					JSONParser parser = new JSONParser();
					Object jsonObj = parser.parse(line);
					JSONObject jsonObject = (JSONObject) jsonObj;
					JSONObject entities = (JSONObject)jsonObject.get("entities");
					JSONArray hashtags =(JSONArray) entities.get("hashtags");
					//ArrayList<String> arr = new ArrayList<>();
					HashSet<String> hst = new HashSet<>();
					hashtags.forEach(n-> {
						JSONObject j = (JSONObject) n;
						hst.add((String)j.get("text"));
					});
					
					String A[] = hst.toString().replace("[", "").replace("]", "").split(",");
					boolean[] B = new boolean[A.length];
					ArrayList<Tuple2<String, String>> tuples = new ArrayList<>();
					subset(A, 2, 0, 0, B,tuples);
					return tuples.iterator();
					
				})
				;
		////count3 has the frequency of all the pairs(in sorted order) that appeared together
		JavaPairRDD<Tuple2<String,String>, Integer> count3 =
				count2
				.mapToPair(line -> new Tuple2<>(line,1))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(f-> new Tuple2<>(f._2,f._1))
				.sortByKey(false)
				.mapToPair(f-> new Tuple2<>(f._2,f._1))
				; 
		
		count3.repartition(1).saveAsTextFile(outputFile);
		sc.stop();
		sc.close();

	}
	

}