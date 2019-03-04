import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.unix_timestamp

object InterGraph {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val vertexFile = args(1)
    val edgeFile = args(2)
    val spark = SparkSession
      .builder()
      .appName("InterGraph")
      .getOrCreate()
    import spark.implicits._

    val tweets = spark.read.option("multiline","true").json(inputFile)

    val vertexTable = tweets.select($"user.id_str".as("user_ID"),
                                    $"user.created_at".as("created_at"),
                                    $"created_at".as("timestamp"),
                                    $"user.followers_count",
                                    $"user.friends_count").
                              filter{$"created_at".isNotNull && $"user_ID".isNotNull}

    val reorderedColumnNames = Array[String]("user_ID","created_at_epochs","timestamp_epochs","followers_count","friends_count")

    val vertexTableEpochs = vertexTable.withColumn("created_at_epochs",unix_timestamp($"created_at","E MMM dd HH:mm:ss Z yyyy")).
                                        withColumn("timestamp_epochs",unix_timestamp($"timestamp","E MMM dd HH:mm:ss Z yyyy")).
                                        drop("created_at","timestamp").
                                        select(reorderedColumnNames.head,reorderedColumnNames.tail:_*).
                                        rdd

    vertexTableEpochs.map{x=> (Seq(x(0).toString,x(1).toString),Seq(x(2).toString,x(3).toString,x(4).toString))}.
      reduceByKey{(x:Seq[String],y:Seq[String]) => Seq.concat(x,y)}.
      map{case (x:Seq[String],y:Seq[String]) => (x(0),x(1),y(0),y(1),y(2))}.
      saveAsTextFile(vertexFile)

    val edgeSet=tweets.select($"user.id_str".as("src_user_ID"),
                              $"retweeted_status.user.id_str".as("sink_user_ID"),
                              $"created_at".as("timestamp"),
                              $"retweeted_status.id_str".as("tweet_id"),
                              $"id_str".as("retweet_id"),
                              $"entities.hashtags.text".as("retweet_hashtags"),
                              $"retweeted_status.entities.hashtags.text".as("tweet_hashtags"))
    val edgeSetEpochs=edgeSet.filter($"src_user_ID".isNotNull && $"sink_user_ID".isNotNull).
        withColumn("timestamp_epochs",unix_timestamp($"timestamp","E MMM dd HH:mm:ss Z yyyy")).
        drop("timestamp").rdd

    def ConcatHashTags(x:org.apache.spark.sql.Row):Tuple2[Tuple2[String,String],Seq[String]] = {
       val a = Tuple2(x(0).toString,x(1).toString)
       val b = Seq(x(6).toString,x(2).toString,x(3).toString)
       val c =b ++ Seq.concat(x(4).asInstanceOf[Seq[String]],x(5).asInstanceOf[Seq[String]])
       return Tuple2(a,c)
    }

    def ReduceFunction(x:Seq[String],y:Seq[String]):Seq[String] = {
      return x ++ y;
    }

    edgeSetEpochs.map(ConcatHashTags).reduceByKey(ReduceFunction).map{
       case (x:(String, String),y:Seq[String]) => List(x._1,x._2) ++ List(y:_*)}.map(x => x.mkString(",")).saveAsTextFile(edgeFile)



  }
}
