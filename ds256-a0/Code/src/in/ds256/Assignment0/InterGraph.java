package in.ds256.Assignment0;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Tuple2;

public class InterGraph{
	public static void main(String[] args) throws IOException{
		
		String inputFile = args[0]; // Should be some file on HDFS
		String vertexFile = args[1]; // Should be some file on HDFS
		String edgeFile = args[2]; // Should be some file on HDFS
		Integer vertex_count=0;
		Integer edge_count=0;
		
		SparkConf sparkConf = new SparkConf().setAppName("InterGraph");//.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> text=sc.textFile(inputFile);
		
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> vertex  = text.mapToPair(new PairFunction< String,Tuple2<String,String>,String>(){

			@Override
			public Tuple2<Tuple2<String, String>, String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONParser parser = new JSONParser();
				JSONObject js = null;
				try {
					js = (JSONObject) parser.parse(t);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					return null;
				}
				
				
				String userId="";
				String createdAt="";
				String timestamp_ms="";
				String followersCount="";
				String friendsCount="";
				try {
					JSONObject user=(JSONObject)js.get("user");
					userId = (String)user.get("id_str");
					createdAt = (String)user.get("created_at");
					 timestamp_ms=(String)js.get("timestamp_ms");
					 //followersCount=String.valueOf((user.get("followers_count"));
					 //friendsCount=((int)user.get("friends_count")).toString();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>("7","8"),"w");
					
				}

				if(js.containsKey("retweeted_status"))
				{
					
					String rest = timestamp_ms;//+" @@ " +followersCount + " ## " +friendsCount;
					return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(userId,createdAt),rest);
				}
				
				else 
				{
					String rest = timestamp_ms;
					return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(userId,createdAt),rest);
				}
			}
			
		}).filter(x->x!=null).groupByKey();
		

		
		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(vertexFile));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.setOut(out);
		System.out.println(vertex.collect());
		
		vertex_count=(int) vertex.count();
		
		//edge yaha se shuru hui...
		
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> edge  = text.mapToPair(new PairFunction< String,Tuple2<String,String>,String>(){

			@Override
			public Tuple2<Tuple2<String, String>, String> call (String t) throws Exception {
				JSONParser parser = new JSONParser();
				JSONObject js = null;
				try {
					js = (JSONObject) parser.parse(t);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					return null;
				}
				if(js.containsKey("retweeted_status"))
				{
					
					JSONObject source_user=(JSONObject)js.get("user");
					String sourceId=(String)source_user.get("id_str");
					
					JSONObject retweeted_tweet=(JSONObject)js.get("retweeted_status");
					JSONObject sink_user=(JSONObject)retweeted_tweet.get("user");
					String sinkId=(String)retweeted_tweet.get("id_str");
					
					JSONObject entities=(JSONObject)js.get("entities");
					String hashtags=""; 
					JSONArray jsonArray = null;
                    String timestamp_ms=(String)js.get("timestamp_ms");
					
					String tweetId=(String)retweeted_tweet.get("id_str");
					String retweetId=(String)js.get("id_str");
					try {
						jsonArray = (JSONArray) entities.get("hashtags");
						for(int i = 0; i < jsonArray.size(); i++)
						{
						      JSONObject objects = (JSONObject)jsonArray.get(i);
						      hashtags=hashtags+","+((String)objects.get("text"));
						}
						
					} catch (Exception e) {
						
					}
					
					
					
					String rest = timestamp_ms + "," +tweetId+","+retweetId+"," +hashtags + "\n";
					return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(sourceId,sinkId),rest);	
				}
				else
				{
					return null;
				}
		
			}
			
		}).filter(x->x!=null).groupByKey();
		

		
		edge_count=(int) edge.count();
		//PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(edgeFile));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.setOut(out);
		System.out.println(edge.collect());
		
		try {
			out = new PrintStream(new FileOutputStream("/home/nileshrathi/count.txt"));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.setOut(out);
		System.out.println(vertex_count);
		System.out.println(edge_count);
		
		
		
		
		
		
		
	}
}
