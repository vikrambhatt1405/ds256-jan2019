package in.ds256.Assignment0;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;

/**
 * DS-256 Assignment 0
 * Code for generating interaction graph
 */
public class InterGraph {

    public static void main(String[] args) {

        String inputFile = args[0]; // Should be some file on HDFS
        String vertexFile = args[1]; // Should be some file on HDFS
        String edgeFile = args[2]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> twitterData = sc.textFile(inputFile).persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("Program_Log: File Opened !");

        // Get vertex info
        JavaRDD<String> vertexInfo = twitterData.flatMapToPair((PairFlatMapFunction<String, Tuple2<Long, Long>, String>) InterGraph::getVertex).reduceByKey((x, y) -> x + "," + y).map(x -> x._1._1 + "," + x._1._2 + "," + x._2);
        System.out.println("Program_Log: Obtained Vertex Info !");

        // Get Edge info
        JavaRDD<String> edgeInfo = twitterData.flatMapToPair((PairFlatMapFunction<String, Tuple2<Long, Long>, String>) InterGraph::getEdge).reduceByKey((x, y) -> x + ";" + y).map(x -> x._1._1 + "," + x._1._2 + ";" + x._2);
        System.out.println("Program_Log: Obtained Vertex Info !");

        // Save File
        vertexInfo.coalesce(1,true).saveAsTextFile(vertexFile);
        edgeInfo.coalesce(1,true).saveAsTextFile(edgeFile);

        sc.stop();
        sc.close();
    }

    private static Iterator<Tuple2<Tuple2<Long, Long>, String>> getVertex(String x) {
        try {
            JSONObject j = (JSONObject) new JSONParser().parse(x);
            JSONObject k = (JSONObject) j.get("user");
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss xxxx yyyy");
            Long id = (Long) k.get("id");
            if (id == null)
                return Collections.emptyIterator();
            Long created_at = k.get("created_at") == null ? 0L: ZonedDateTime.parse((String) k.get("created_at"), dtf).toInstant().toEpochMilli();
            Long ts = ZonedDateTime.parse((String) j.get("created_at"), dtf).toInstant().toEpochMilli();
            Long foc = (Long) k.get("followers_count");
            Long frc = (Long) k.get("friends_count");
            return Collections.singletonList(new Tuple2<>(new Tuple2<>(id, created_at), ts + "," + foc + "," + frc)).iterator();
        } catch (Exception e) {
            return Collections.emptyIterator();
        }
    }

    private static Iterator<Tuple2<Tuple2<Long, Long>, String>> getEdge(String x) {
        try {
            JSONObject j = (JSONObject) new JSONParser().parse(x);
            JSONObject k = (JSONObject) j.get("user");
            JSONObject r = (JSONObject) j.get("retweeted_status");
            JSONObject rk = (JSONObject) r.get("user");
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss xxxx yyyy");
            Long src_id = (Long) k.get("id");
            Long sink_id = (Long) rk.get("id");
            if (src_id == null || sink_id == null)
                return Collections.emptyIterator();
            Long otid = (Long) r.get("id");
            Long rtid = (Long) j.get("id");
            Long ts = ZonedDateTime.parse((String) j.get("created_at"), dtf).toInstant().toEpochMilli();
            JSONArray a = (JSONArray) ((JSONObject) j.get("entities")).get("hashtags");
            String[] as = new String[a.size()];
            for(int i = 0; i < a.size(); i++)
                as[i] = (String) ((JSONObject) a.get(i)).get("text");
            if (a.size() == 0)
                return Collections.singletonList(new Tuple2<>(new Tuple2<>(src_id, sink_id), ts + "," + otid + "," + rtid )).iterator();
            return Collections.singletonList(new Tuple2<>(new Tuple2<>(src_id, sink_id), ts + "," + otid + "," + rtid + "," + String.join(",", as))).iterator();
        } catch (Exception e) {
            return Collections.emptyIterator();
        }
    }

}