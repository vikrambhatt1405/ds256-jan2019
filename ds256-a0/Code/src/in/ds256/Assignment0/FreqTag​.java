package in.ds256.Assignment0;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class FreqTag {

	public static void main(String[] args)
	{
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		//String percentage = args[2];
	//	Double per= Double.parseDouble(percentage);
		
		SparkConf sparkConf = new SparkConf().setAppName("FreqTag");//.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> text=sc.textFile(inputFile);//.sample(false,per );
		JavaPairRDD<Integer, Integer> counts = text.mapToPair(
		          new PairFunction<String, Integer, Integer>(){
			            public Tuple2<Integer, Integer> call(String x) throws ParseException{
			            	JSONParser parser = new JSONParser();
			            	
			            	
			            	JSONObject entities;
							try {
								JSONObject json = (JSONObject) parser.parse(x);
								entities = (JSONObject)json.get("entities");
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								return new Tuple2<Integer, Integer>(0,0);
							}
			            	JSONArray jsonArray = null;
							try {
								jsonArray = (JSONArray) entities.get("hashtags");
							} catch (Exception e) {
								return new Tuple2<Integer, Integer>(0,0);
							}
			            	
			             return new Tuple2<Integer, Integer>(jsonArray.size(), 1);
		          }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
		                public Integer call(Integer x, Integer y){ return x + y;}}).sortByKey();;
		
		
		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(outputFile));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.setOut(out);
		System.out.println(counts.take(200));
		
		
		sc.close();
		
	}
}
