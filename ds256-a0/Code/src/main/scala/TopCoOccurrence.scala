import org.apache.spark.sql.SparkSession
object TopCoOccurrence {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)
    val spark = SparkSession
      .builder()
      .appName("TopCoOccurrence")
      .getOrCreate()
    val sc = spark.sparkContext
    val tweets = spark.read.option("multiline","true").json(inputFile)
    val hashTagsRdd = tweets.select("entities.hashtags.text").rdd
    val validTwoCombs = hashTagsRdd.filter(x => x(0)!=null).map{case x:org.apache.spark.sql.Row=>x(0).asInstanceOf[Seq[String]]}.filter(x=>x.size>1)
    val result = validTwoCombs.map(x=>x.toList.sorted).map(x=>x.combinations(2).toList).flatMap(x=>x).map(x=>(x,1)).reduceByKey(_+_).top(100)(ImplicitOrder)
    sc.parallelize(result).saveAsTextFile(outputFile)
  }
  object ImplicitOrder extends  Ordering[(List[String],Int)]{
    override def compare(x: (List[String], Int), y: (List[String], Int)): Int = x._2 compare y._2  }
}
