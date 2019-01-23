package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
 * Code for finding frequent co-occuring hash-tags
 */
public class ​TopCoOccurrence  {
	
	public static void main(String[] args) throws IOException {	
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rdd1 = sc.textFile(inputFile);
		JavaRDD<String> rdd2 = rdd1.filter(s -> {
			try {
			JSONObject ob=(JSONObject)new JSONParser().parse(s);
			JSONArray ja=(JSONArray)((JSONObject)ob.get("entities")).get("hashtags");
			return true;
			}catch(Exception e) {
				return false;
			}
		});
				
		
		
		JavaPairRDD<Tuple2<String,String>,Integer> rdd4=rdd2.flatMap(f ->{
			ArrayList<Tuple2<String,String>> ht=new ArrayList<Tuple2<String,String>>();
			
			JSONObject ob1=(JSONObject)new JSONParser().parse(f);
			JSONArray ja=(JSONArray)((JSONObject)ob1.get("entities")).get("hashtags");
			ArrayList<String> arr=new ArrayList<String>();
			for(Object o: ja){
			    if ( o instanceof JSONObject ) {
			    	JSONObject q=(JSONObject)o;
			    	arr.add((String)q.get("text"));
			    }
			}
			HashSet<String> set = new HashSet<String>(arr);
			arr.clear();
			arr.addAll(set);
			for(int i=0;i<arr.size();i++)
			{
				for(int j=i+1;j<arr.size();j++)
				{
		
					String s1=arr.get(i);
					String s2=arr.get(j);
					int cmp=s1.compareTo(s2);
					if(cmp<0)
					{
						Tuple2<String, String> t = new Tuple2<>(s1, s2);
						ht.add(t);
					}
					else
					{
						Tuple2<String, String> t = new Tuple2<>(s2, s1);
						ht.add(t);
					}
						
					
				}
			}
			return ht.iterator();
			
		}).mapToPair(size -> new Tuple2<>(size, 1)).reduceByKey((a,b) -> a+b);
			
		
		JavaPairRDD<Integer,Tuple2<String,String>> rdd5=
				rdd4.mapToPair(line -> new Tuple2<Integer,Tuple2<String,String>>(line._2, line._1))
				.sortByKey(false);
		
		rdd5.repartition(1).saveAsTextFile(outputFile);		
		
		sc.stop();
		sc.close();
	}

}
