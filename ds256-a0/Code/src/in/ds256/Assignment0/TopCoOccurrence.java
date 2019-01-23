package in.ds256.Assignment0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for finding frequent co-occuring hash-tags
 */
public class TopCoOccurrence {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		// Nothing to write, output is small and can be read from the logs
		// String outputFile = args[1]; // Should be some file on HDFS

		SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		// Second command line argument, if given, is the fraction of read json lines
		// that is used.
		if (args.length > 1) {
			Double fraction = Double.valueOf(args[1]);
			lines = lines.sample(false, fraction);
		}
		System.out.println("O_NUM_LINES_BEFORE : " + lines.count());
		JavaRDD<String> filteredLines = lines.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) {
				try {
					JsonParser parser = new JsonParser();
					JsonObject twitterObject = parser.parse(line).getAsJsonObject();
					if (twitterObject.get("id_str") == null || twitterObject.get("entities") == null || twitterObject
							.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray().size() == 0) {
						return false;
					}
					return true;
				} catch (JsonSyntaxException e) {
					return false;
				}
			}
		});
		System.out.println("O_NUM_LINES_AFTER : " + filteredLines.count());
		JavaPairRDD<String, String> idsAndHashtags = filteredLines
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, String>> call(String line) {
						JsonParser parser = new JsonParser();
						JsonObject twitterObject = parser.parse(line).getAsJsonObject();
						String userId = twitterObject.get("id_str").getAsString();
						ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						for (Iterator<JsonElement> it = twitterObject.get("entities").getAsJsonObject().get("hashtags")
								.getAsJsonArray().iterator(); it.hasNext();) {
							JsonElement jsonElement = it.next();
							list.add(new Tuple2<String, String>(userId,
									jsonElement.getAsJsonObject().get("text").getAsString()));
						}
						return list.iterator();
					}
				});
		JavaPairRDD<Integer, Tuple2<String, String>> output = idsAndHashtags.join(idsAndHashtags)
				.filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<String, String>> tuple) {
						if (tuple._2._1.compareTo(tuple._2._2) >= 0) {
							return false;
						}
						return true;
					}
				})
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, Tuple2<String, String>, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<String, String>> tuple) {
						return new Tuple2<Tuple2<String, String>, Integer>(tuple._2, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}).mapToPair(
						new PairFunction<Tuple2<Tuple2<String, String>, Integer>, Integer, Tuple2<String, String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Integer, Tuple2<String, String>> call(
									Tuple2<Tuple2<String, String>, Integer> tuple) {
								return new Tuple2<Integer, Tuple2<String, String>>(tuple._2, tuple._1);
							}
						})
				.sortByKey(false);
		System.out.println("O_OUTPUT : " + output.take(100));

		sc.stop();
		sc.close();
	}

}