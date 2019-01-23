package in.ds256.Assignment0;

iimport java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.JsonObject;

import scala.Tuple2;
import scala.Tuple3;

/**
 * DS-256 Assignment 0
 * Code for generating interaction graph
 */
public class InterGraph  {
	public static void main(String[] args) {
		String inputFile = args[0]; // Should be some file on HDFS
		String vertexFile = args[1]; // Should be some file on HDFS
		String edgeFile = args[2]; // Should be some file on HDFS
		
		////spark connection made
		SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd1 = sc.textFile(inputFile);//rdd1 just has all the jsons in String format
		
		////rdd2 contains tweets that are actually retweets
		JavaRDD<String> rdd2 = rdd1.filter(line -> {
			try {
				JSONParser parser = new JSONParser();
				Object jsonObj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) jsonObj;
				JSONObject retweeted_status = (JSONObject)jsonObject.get("retweeted_status");
				JSONObject ruser =(JSONObject) retweeted_status.get("user");
				String timestamp = (String)jsonObject.get("timestamp_ms");//Long.toString(date.getTime());
				
				return true;
			}catch(Exception e) {
				return false;
			}
		});
		JavaRDD<String> rddV = rdd1.filter(line -> {
			try {
				JSONParser parser = new JSONParser();
				Object jsonObj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) jsonObj;
				JSONObject user = (JSONObject)jsonObject.get("user");
				
				String userId = (String) user.get("id_str");
				String createdAtGMT = (String) user.get("created_at");
				SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
				Date date = df.parse(createdAtGMT);
				String createdAt = Long.toString(date.getTime());
//				String timestampGMT = (String) jsonObject.get("created_at");
//				date = df.parse(timestampGMT);
				String timestamp = (String)jsonObject.get("timestamp_ms");//Long.toString(date.getTime());
				String followers_count = (String) user.get("followers_count").toString();
				String friends_count = (String) user.get("friends_count").toString();
				return true;
			}catch(Exception e) {
				return false;
			}
		});
		JavaPairRDD<String,String> rdd3 = 
		rddV.flatMap(line -> {
			
				JSONParser parser = new JSONParser();
				Object jsonObj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) jsonObj;
				
//				JSONObject retweeted_status = (JSONObject)jsonObject.get("retweeted_status");
//				JSONObject ruser =(JSONObject) retweeted_status.get("user");
//				
//				String ruserId = (String)ruser.get("id_str");
				SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
//				
//				String  rcreatedAtGMT = (String) ruser.get("created_at");
//				Date date = df.parse(rcreatedAtGMT);
//				String rcreatedAt = Long.toString(date.getTime());
//				String rtimestampGMT = (String)retweeted_status.get("created_at");
//				date = df.parse(rtimestampGMT);
//				String rtimestamp = Long.toString(date.getTime());
//				String rfollowers_count =  ruser.get("followers_count").toString();
//				String rfriends_count = ruser.get("friends_count").toString();
				
				JSONObject user = (JSONObject)jsonObject.get("user");
				
				String userId = (String) user.get("id_str");
				String createdAtGMT = (String) user.get("created_at");
				Date date = df.parse(createdAtGMT);
				String createdAt = Long.toString(date.getTime());
//				String timestampGMT = (String) jsonObject.get("created_at");
//				date = df.parse(timestampGMT);
				String timestamp = (String)jsonObject.get("timestamp_ms");//Long.toString(date.getTime());
				String followers_count =  user.get("followers_count").toString();
				String friends_count = user.get("friends_count").toString();
				Tuple2<String, String> tu11 = new Tuple2<>(userId, createdAt) ;
				Tuple3<String, String, String> tu12 = new Tuple3<>(timestamp,followers_count,friends_count);
//				Tuple2<String, String> tu21 = new Tuple2<>(ruserId, rcreatedAt) ;
//				Tuple3<String, String, String> tu22 = new Tuple3<>(rtimestamp,rfollowers_count,rfriends_count);
				
				ArrayList<Tuple2<String,String>> arr= new ArrayList<>();
				arr.add(new Tuple2<String, String>(userId+","+createdAt , timestamp+","+followers_count+","+friends_count ));
				//arr.add(new Tuple2<String, String>(ruserId+","+rcreatedAt , rtimestamp+","+rfollowers_count+","+rfriends_count ));
				return arr.iterator();
				
				
		})
		.mapToPair(line ->new Tuple2<>(line._1,line._2));
		
		JavaPairRDD<String, Iterable<String> > rdd4 = 
				rdd3 . groupByKey();
		JavaRDD<String> vertices = rdd4.map(line -> line._1 + "," + String.join(",", line._2));
		vertices.repartition(1).saveAsTextFile(vertexFile);
//		JavaPairRDD<Tuple2<String,String>, ArrayList<Tuple3<String,String,String>>> rdd4=
//				rdd3.reduceByKey();
		
		//rdd4.map(x->x._1._1+","+ x._1._2 + "," + x._2.toString().substring(1, x._2.toString().length()-1)/*+String.join(",", x._2.get(0))*/)
		//.repartition(1).saveAsTextFile(outputFile);
//		
		JavaPairRDD<String,String> rddEdgelist = rdd2
			.filter(line ->{
				try {
					JSONParser parser = new JSONParser();
					Object jsonObj = parser.parse(line);
					JSONObject jsonObject = (JSONObject) jsonObj;
					JSONArray hashtags = (JSONArray) ((JSONObject)jsonObject.get("entities")).get("hashtags");
					hashtags.forEach(n -> ((JSONObject) n).get("text"));
					
					return true;
				}
				catch(Exception e) {
					return false;
				}
			})
			
			.mapToPair(line ->{
			String x="";
			JSONParser parser = new JSONParser();
			Object jsonObj = parser.parse(line);
			JSONObject jsonObject = (JSONObject) jsonObj;
			JSONObject user =  (JSONObject) jsonObject.get("user");
			x = x + (String) user.get("id_str") ;
			
			x = x + "," + ((JSONObject) ((JSONObject) jsonObject.get("retweeted_status")).get("user")).get("id_str");
			
			String y="";
			y = y  + jsonObject.get("timestamp_ms");
			y = y + "," + ((JSONObject) jsonObject.get("retweeted_status")).get("id_str");
			y = y + "," + jsonObject.get("id_str");
			JSONArray hashtags = (JSONArray) ((JSONObject)jsonObject.get("entities")).get("hashtags");
			for (Object i : hashtags) {
				y = y + "," + ((JSONObject)i).get("text");
			}
			return new Tuple2<>(x,y);
		});
		//.mapToPair(line -> new Tuple2(line._1, line._2));
		JavaPairRDD<String, Iterable<String>> rddEdgelist2 =
				rddEdgelist.groupByKey()
				;
		JavaRDD<String> edges = rddEdgelist2.map(line -> line._1 + ";" +String.join(";", line._2));
		edges.repartition(1).saveAsTextFile(edgeFile);
		//rddEdgelist2.repartition(1).saveAsTextFile(outputFile+"/edges");
		sc.stop();
		sc.close();
	}


}