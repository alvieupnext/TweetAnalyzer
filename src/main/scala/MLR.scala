import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MLR extends App {
  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]")
  val sc = new SparkContext(conf)

  def parseTweet(tweet: String): Tweet = ???

  val tweets = sc.textFile("/data/twitter/tweetraw").map(parseTweet)
}
