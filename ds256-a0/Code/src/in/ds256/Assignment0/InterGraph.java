package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;

import org.json.simple.JSONArray; 
import org.json.simple.JSONObject; 
import org.json.simple.parser.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
		
		JavaRDD<String> rdd1 = sc.textFile(inputFile);
		JavaRDD<String> rdd2 = rdd1.filter(s -> {
			try {
			JSONObject ob1=(JSONObject)new JSONParser().parse(s);
			JSONObject ob=(JSONObject)ob1.get("user");
			String s1=(String)ob.get("id_str");
			String s3=(String)ob1.get("timestamp_ms");
			long l1 = (long)ob.get("followers_count");
			String s4 = Long.toString(l1);
			long l2 = (long)ob.get("friends_count");
			String s5 = Long.toString(l2);
			
			
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			String GMTDate = (String)ob.get("created_at");
			
			Date d = formatter.parse(GMTDate);
			long time_in_epoch = d.getTime();
			String s2 = Long.toString(time_in_epoch);
			return true;
			}catch(Exception e) {
				return false;
			}
		});
				
		
		
		JavaPairRDD<Tuple2<String,String>,Tuple3<String,String,String>> rdd4=rdd2.mapToPair(f ->{
			
			JSONObject ob1=(JSONObject)new JSONParser().parse(f);
			JSONObject ob=(JSONObject)ob1.get("user");
			String s1=(String)ob.get("id_str");
			String s3=(String)ob1.get("timestamp_ms");
			long l1 = (long)ob.get("followers_count");
			String s4 = Long.toString(l1);
			long l2 = (long)ob.get("friends_count");
			String s5 = Long.toString(l2);
			
			
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			String GMTDate = (String)ob.get("created_at");
			
			Date d = formatter.parse(GMTDate);
			long time_in_epoch = d.getTime();
			String s2 = Long.toString(time_in_epoch);
			Tuple2<String, String>t1 = new Tuple2<>(s1,s2);
			Tuple3<String, String, String> t2=new Tuple3<>(s3,s4,s5);
			Tuple2<Tuple2<String, String>,Tuple3<String, String, String>> t=new Tuple2<>(t1,t2);
			return(new Tuple2<>(t1,t2));
			
		});
			
				
			JavaPairRDD<Tuple2<String,String>, Iterable<Tuple3<String,String,String>>> rdd5 =
			rdd4.groupByKey();
			JavaRDD<String> finalV = 
					rdd5
					.map(x -> {
						String y = x._1._1 + "," + x._1._2 ;
						String z="";
						for(Tuple3<String,String,String> i : x._2) {
							z = z+  i._1()+ "," + i._2()+ "," + i._3()+ ",";
						}
						z = z.substring(0,z.length()-1);
						return y + "," + z;
					} );
			
		finalV.repartition(1).saveAsTextFile(vertexFile);
		
		
		JavaRDD<String> edgeFilter = rdd1.filter(s -> {
			try {
			JSONObject ob=(JSONObject)new JSONParser().parse(s);
			String retweet_id=(String)ob.get("id_str");
			String source_user_id=(String)((JSONObject)ob.get("user")).get("id_str");
			String timestamp=(String)ob.get("timestamp_ms");
			JSONArray ja=(JSONArray)((JSONObject)ob.get("entities")).get("hashtags");
			
			JSONObject orig=(JSONObject)ob.get("retweeted_status");
			String orig_tweet_id=(String)orig.get("id_str");
			String sink_user_id=(String)((JSONObject)orig.get("user")).get("id_str");
			
			return true;
			}catch(Exception e) {
				return false;
			}
		});
		
JavaPairRDD<Tuple2<String,String>,Tuple4<String,String,String,String>> rdd6=edgeFilter.mapToPair(f ->{
			
	JSONObject ob=(JSONObject)new JSONParser().parse(f);
	String retweet_id=(String)ob.get("id_str");
	String source_user_id=(String)((JSONObject)ob.get("user")).get("id_str");
	String timestamp=(String)ob.get("timestamp_ms");
	JSONArray ja=(JSONArray)((JSONObject)ob.get("entities")).get("hashtags");
	String ht="";
	for(Object o: ja){
	    if ( o instanceof JSONObject ) {
	    	JSONObject q=(JSONObject)o;
	    	ht+=(","+(String)q.get("text"));
	    }
	}
	
	JSONObject orig=(JSONObject)ob.get("retweeted_status");
	String orig_tweet_id=(String)orig.get("id_str");
	String sink_user_id=(String)((JSONObject)orig.get("user")).get("id_str");
			Tuple2<String, String>t1 = new Tuple2<>(source_user_id,sink_user_id);
			Tuple4<String, String, String,String> t2=new Tuple4<>(timestamp,orig_tweet_id,retweet_id,ht);
			Tuple2<Tuple2<String, String>,Tuple4<String, String, String,String>> t=new Tuple2<>(t1,t2);
			return(new Tuple2<>(t1,t2));
			
		});
		JavaPairRDD<Tuple2<String,String>, Iterable<Tuple4<String,String,String,String>>> rdd7 =
		rdd6.groupByKey();
		
		JavaRDD<String> finalE = 
		rdd7
		.map(x -> {
			String y = x._1._1 + "," + x._1._2 ;
			String z="";
			for(Tuple4<String,String,String,String> i : x._2) {
				z = z+ ";"+ i._1()+ "," + i._2()+ "," + i._3()+ i._4();
			}
			
			return y + z;
		} );

		finalE.repartition(1).saveAsTextFile(edgeFile);
		
		sc.stop();
		sc.close();
	}

}
