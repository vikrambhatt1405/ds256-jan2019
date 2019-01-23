package in.ds256.Assignment0;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for generating interaction graph
 */
public class InterGraph {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		String vertexFile = args[1]; // Should be some file on HDFS
		String edgeFile = args[2]; // Should be some file on HDFS

		SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		System.out.println("O_NUM_LINES_BEFORE : " + lines.count());
		JavaRDD<String> filteredLines = lines.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) {
				try {
					JsonParser parser = new JsonParser();
					JsonObject twitterObject = parser.parse(line).getAsJsonObject();
					if (twitterObject.get("user") == null) {
						return false;
					}
					return true;
				} catch (JsonSyntaxException e) {
					return false;
				}
			}
		});
		System.out.println("O_NUM_LINES_AFTER : " + filteredLines.count());
		JavaRDD<String> vertices = filteredLines
				.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, String>() {

					private static final long serialVersionUID = 1L;

					private String timestampToUTCEpoch(String timestamp) throws ParseException {
						SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
						Date date = format.parse(timestamp);
						return Long.toString(date.getTime());
					}

					private Tuple2<Tuple2<String, String>, String> getTupleFromUserJsonObject(JsonObject userObject,
							String timestamp) throws ParseException {
						String id = userObject.get("id_str").getAsString();
						String createdAt = userObject.get("created_at").getAsString();
						if (createdAt == null) {
							createdAt = "0";
						} else {
							createdAt = timestampToUTCEpoch(createdAt);
						}
						String followersCount = Integer.toString(userObject.get("followers_count").getAsInt());
						String friendsCount = Integer.toString(userObject.get("friends_count").getAsInt());
						String temporalString = "," + String.join(",", timestamp, followersCount, friendsCount);
						return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(id, createdAt),
								temporalString);
					}

					@Override
					public Iterator<Tuple2<Tuple2<String, String>, String>> call(String line) throws Exception {
						ArrayList<Tuple2<Tuple2<String, String>, String>> list = new ArrayList<>();
						JsonParser parser = new JsonParser();
						JsonObject twitterObject = parser.parse(line).getAsJsonObject();
						JsonObject mainUser = twitterObject.get("user").getAsJsonObject();
						String timestamp = twitterObject.get("created_at").getAsString();
						if (timestamp == null) {
							timestamp = "0";
						} else {
							timestamp = timestampToUTCEpoch(timestamp);
						}
						list.add(getTupleFromUserJsonObject(mainUser, timestamp));
						if (twitterObject.get("retweeted_status") != null) {
							JsonObject otherUser = twitterObject.get("retweeted_status").getAsJsonObject().get("user")
									.getAsJsonObject();
							list.add(getTupleFromUserJsonObject(otherUser, timestamp));
						}
						return list.iterator();
					}

				}).reduceByKey(new Function2<String, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(String a, String b) {
						return a + b;
					}

				}).map(new Function<Tuple2<Tuple2<String, String>, String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<Tuple2<String, String>, String> tuple) {
						return tuple._1._1 + "," + tuple._1._2 + tuple._2;
					}

				});
		JavaRDD<String> furtherFilteredLines = filteredLines.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) {
				try {
					JsonParser parser = new JsonParser();
					JsonObject twitterObject = parser.parse(line).getAsJsonObject();
					if (twitterObject.get("user") == null || twitterObject.get("retweeted_status") == null
							|| twitterObject.get("retweeted_status").getAsJsonObject().get("user") == null) {
						return false;
					}
					return true;
				} catch (JsonSyntaxException e) {
					return false;
				}
			}
		});
		System.out.println("O_NUM_LINES_AFTER_SECOND : " + furtherFilteredLines.count());
		JavaRDD<String> edges = furtherFilteredLines
				.mapToPair(new PairFunction<String, Tuple2<String, String>, String>() {

					private static final long serialVersionUID = 1L;

					private String timestampToUTCEpoch(String timestamp) throws ParseException {
						SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
						Date date = format.parse(timestamp);
						return Long.toString(date.getTime());
					}

					@Override
					public Tuple2<Tuple2<String, String>, String> call(String line) throws ParseException {
						JsonParser parser = new JsonParser();
						JsonObject twitterObject = parser.parse(line).getAsJsonObject();
						String srcId = twitterObject.get("user").getAsJsonObject().get("id_str").getAsString();
						String sinkID = twitterObject.get("retweeted_status").getAsJsonObject().get("user")
								.getAsJsonObject().get("id_str").getAsString();
						String timestampString = twitterObject.get("created_at").getAsString();
						if (timestampString == null) {
							timestampString = "0";
						} else {
							timestampString = timestampToUTCEpoch(timestampString);
						}
						String tweetId = twitterObject.get("retweeted_status").getAsJsonObject().get("id_str")
								.getAsString();
						String retweetId = twitterObject.get("id_str").getAsString();
						String temporalString = ";" + String.join(",", timestampString, tweetId, retweetId);
						for (Iterator<JsonElement> it = twitterObject.get("entities").getAsJsonObject().get("hashtags")
								.getAsJsonArray().iterator(); it.hasNext();) {
							temporalString = temporalString + ","
									+ it.next().getAsJsonObject().get("text").getAsString();
						}
						return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(srcId, sinkID),
								temporalString);
					}

				}).reduceByKey(new Function2<String, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(String a, String b) {
						return a + b;
					}

				}).map(new Function<Tuple2<Tuple2<String, String>, String>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<Tuple2<String, String>, String> tuple) {
						return tuple._1._1 + "," + tuple._1._2 + tuple._2;
					}

				});
		System.out.println("O_OUTPUT_NUM_VERTICES : " + vertices.count());
		System.out.println("O_OUTPUT_NUM_EDGES : " + edges.count());

		vertices.saveAsTextFile(vertexFile);
		edges.saveAsTextFile(edgeFile);

		sc.stop();
		sc.close();
	}

}