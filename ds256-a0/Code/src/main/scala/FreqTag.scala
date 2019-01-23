import org.apache.spark.sql.SparkSession
object FreqTag{
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)
    val spark = SparkSession
      .builder()
      .appName("FreqTag")
      .getOrCreate()
    val tweets = spark.read.option("multiline","true").json(inputFile)
    val hashTagsRdd = tweets.select("entities.hashtags.text").rdd
    hashTagsRdd.filter(x => x(0)!=null).map{case x:org.apache.spark.sql.Row=>(x(0).asInstanceOf[Seq[String]].size,1)}.reduceByKey((x,y)=>x+y).saveAsTextFile(outputFile)
  }
}