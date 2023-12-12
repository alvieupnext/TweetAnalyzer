import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MLR extends App {
  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]")
  val sc = new SparkContext(conf)

  val tweets: RDD[Tweet] = sc.textFile("data/twitter/tweetsraw")
      //first parse the tweets, filter the ones that are not defined and then get all the tweets out of the option
                  .map(Tweet.parse).filter(_.isDefined).map(_.get).persist()

  //Get the number of tweets
  val numberOfTweets = tweets.count()

  //For each tweet, get the like amount
  val likes = tweets.map(_.likes)

  //Print the likes
  println("Likes: " + likes.collect().mkString(", "))

  //Print the amount of tweets
  println("Number of tweets: " + numberOfTweets)

  System.in.read() // Keep the application active.

}
