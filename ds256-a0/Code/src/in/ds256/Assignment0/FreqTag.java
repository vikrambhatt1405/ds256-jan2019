package in.ds256.Assignment0;

import java.io.IOException;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * DS-256 Assignment 0 Code for generating frequency distribution per hashtag
 */
public class FreqTag {

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		// Nothing to write, output is small and can be read from the logs
		// String outputFile = args[1]; // Should be some file on HDFS

		SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
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
					if (twitterObject.get("user") == null || twitterObject.get("entities") == null) {
						return false;
					}
					return true;
				} catch (JsonSyntaxException e) {
					return false;
				}
			}
		});
		System.out.println("O_NUM_LINES_AFTER : " + filteredLines.count());
		Map<Double, Long> output = filteredLines.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) {
				JsonParser parser = new JsonParser();
				JsonObject twitterObject = parser.parse(line).getAsJsonObject();
				String userId = twitterObject.get("user").getAsJsonObject().get("id_str").getAsString();
				Integer hashtagCount = twitterObject.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray()
						.size();
				return new Tuple2<String, Integer>(userId, hashtagCount);
			}
		}).mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer in) {
				return new Tuple2<Integer, Integer>(in, 1);
			}
		}).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) {
				return new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2);
			}
		}).mapValues(new Function<Tuple2<Integer, Integer>, Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Tuple2<Integer, Integer> val) {
				return new Double(val._1) / val._2;
			}
		}).map(new Function<Tuple2<String, Double>, Tuple2<String, Double>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Double> in) {
				return in;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Double, String> call(Tuple2<String, Double> in) {
				return new Tuple2<Double, String>(in._2, in._1);
			}
		}).countByKey();
		System.out.println("O_OUTPUT : " + output);

		sc.stop();
		sc.close();
	}

}