package in.ds256.Assignment0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.simple.JSONArray;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import scala.Tuple2;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

/**
 * DS-256 Assignment 0
 * Code for generating frequency distribution per hashtag
 */
public class FreqTag {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> twitterData = sc.textFile(inputFile);
        System.out.println("Program_Log: File Opened !");

        // Get Hash Count for each tweet
        JavaPairRDD<Long, Tuple2> hashCount = twitterData.flatMapToPair((PairFlatMapFunction<String, Long, Tuple2>) FreqTag::getHashCount);
        System.out.println("Program_Log: Obtained Count !");

        // Get average per user

        JavaRDD<Double> avgPerUser = hashCount.reduceByKey((x, y) -> new Tuple2<>((Integer) x._1 + (Integer) y._1, (Integer) x._2 + (Integer) y._2)).map(x -> ((Integer) x._2._1).doubleValue() / ((Integer) x._2._2).doubleValue());
        System.out.println("Program_Log: Obtained Avg. value !");

        // Get Histogram

        JavaDoubleRDD avgPerUserD = avgPerUser.mapToDouble(x -> x);
        Tuple2<double[], long[]> histogram = avgPerUserD.histogram(20);

        // Save file

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(outputFile), conf);
        FSDataOutputStream out = fs.create(new Path(outputFile));
        for(int i=0; i<20; i++) {
            out.write((histogram._1[i]+",").getBytes());
            out.write((histogram._2[i]+"").getBytes());
            out.write(("\n").getBytes());
        }
        out.close();

        System.out.println("Program_Log: Output written to file !");

        sc.stop();
        sc.close();
    }

    private static Iterator<Tuple2<Long, Tuple2>> getHashCount(String x) {
        try {
            JSONObject j = (JSONObject) new JSONParser().parse(x);
            return Collections.singletonList(new Tuple2<Long, Tuple2>(
                    (Long) ((JSONObject) j.get("user")).get("id"),
                    new Tuple2<>(((JSONArray) ((JSONObject) j.get("entities")).get("hashtags")).size(), 1))).iterator();
        } catch (Exception e) {
            return Collections.emptyIterator();
        }
    }

}