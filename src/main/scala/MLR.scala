import Feature.{Feature, FeatureTuple, arrayFeatureToString}
import Metrics.{Likes, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MLR extends App {
  val conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[4]")
  val sc = new SparkContext(conf)

  // Create a SparkSession which is required for working with Dataset
  val spark = SparkSession.builder().config(conf).getOrCreate()

  // Import the implicits, this is now done from the SparkSession (not from SparkContext)
  import spark.implicits._

  // Load the possible tweets
  val possibleTweets: Dataset[Option[Tweet]] = sc.textFile("data/twitter/tweetsraw")
      //first parse the tweets, transform RDD to a Dataset and then get all the tweets out of the option
      .map(Tweet.parse).toDS

  //Filter all the nones
  val tweets: Dataset[Tweet] = possibleTweets.filter(_.isDefined).map(_.get).persist()

  //first, collect all the hashtags
  val hashtags: Dataset[Metrics.Tag] = tweets.flatMap(_.hashTags).persist()

  //then we count the hashtags by the amount of times they occur (make all hashtags lowercase)
  val hashtagCounts = hashtags.groupByKey(_.toLowerCase).count()

  //We get the total amount of times any hashtag has appeared
  val totalHashtagCount = hashtags.count().toInt

//  //Sort the hashtagCount in descending order, in case of a tie, sort by the hashtag alphabetically
//  val sortedHashtagCounts = hashtagCounts.sort($"count(1)".desc, $"key".asc).persist()
//
//  //print the sortedHashtagCounts
//  println("Sorted hashtag counts: " + sortedHashtagCounts.collect().mkString(", "))

//  //Divide the hashtags into groups
//  val amountOfHashtagGroups = 2

  //Based on the totalHashtag

  //Then we divide the amount of times a hashtag has appeared by the total amount of hashtags to get a distribution
  val hashtagDistribution = hashtagCounts.map(h => (h._1, h._2.toDouble / totalHashtagCount)).persist()

  //Get the highest distribution
  val highestDistribution = hashtagDistribution.sort($"_2".desc).first()._2

  val amountOfHashtagGroups = 10

  //Normalize the distribution (10 should correspond to the highest distribution)
  //The double should become a natural number between 0 and amountOfHashtagGroups
  val normalizedHashtagDistribution: Dataset[(Tag, Int)] = hashtagDistribution.map(h => (h._1, (h._2 / highestDistribution * amountOfHashtagGroups).toInt)).persist()

  //Collect the normalized hashtag distribution
  val normalizedHashtagDistributionCollected = normalizedHashtagDistribution.collect()
  //Print the normalized distribution
  println("Normalized hashtag distribution: " + normalizedHashtagDistributionCollected.mkString(", "))

  //Make a function which given a hashtag, returns the distribution
  //Function should return zero if the hashtag is not found
  def getHashtagDistribution(hashtag: Tag, distributions: Array[(Tag, Int)] = normalizedHashtagDistributionCollected): Int = {
    val distribution = distributions.filter(_._1 == hashtag.toLowerCase)
    distribution match
    {
      case d if d.length == 0 => 0
      case d => d.head._2
    }
  }

  def extractFeatures(tweets: Dataset[Tweet]): Dataset[FeatureTuple] = {
    tweets.map(tweet => {
      //Get the features from the tweet
      val tweetLength = tweet.length.toFloat
      val user = tweet.user
      val userFeatures: Array[Float] = user.features
//      //Filter the normalized hashtag distribution by the hashtags in the tweet
//      val tweetHashtagDistribution = normalizedHashtagDistribution.filter(h => tweet.hashTags.contains(h._1))
      val hashtagDistributions = tweet.hashTags.map(getHashtagDistribution(_))
      //Generate an array from 0 to 10 (possible distribution values)
      val possibleDistributions = (0 to amountOfHashtagGroups).toArray
      //For each possible distribution, count the amount of times it occurs in the tweet
      val hashtagFeatures: Array[Float] = possibleDistributions.map(d => hashtagDistributions.count(_ == d))
      val dependent = tweet.likes
      //Return the features, remember to add x0 = 1
      (Array(1f, tweetLength) ++ userFeatures ++ hashtagFeatures, dependent)
    })
  }

  //Extract the features from the tweets
  val featureDataset: Dataset[FeatureTuple] = extractFeatures(tweets)

  //A procedure to scale the features by normalizing them
  def scaleFeatures(features: Dataset[FeatureTuple]): Dataset[FeatureTuple] = {
    // Extract the feature array from the feature tuples
    val featureArray = features.map(_._1)

    // Calculate the mean and standard deviation for each feature
    val mean = featureArray
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / features.count())

    //Print the mean
    println("Mean: " + arrayFeatureToString(mean))

    //Calculate the standard deviation
    val stddev = featureArray
      .map(f => f.zip(mean).map { case (x, y) => Math.pow(x - y, 2) })
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / features.count())
      .map(math.sqrt)
      .map(_.toFloat)

    //Print the standard deviation
    println("Stddev: " + arrayFeatureToString(stddev))

    // Normalize the features
    val normalizedFeatures = features.map { case (f, label) =>
      val normFeatures = f.zip(mean).zip(stddev).map {
        case ((feature, mean), stdDev) => if (stdDev != 0) (feature - mean) / stdDev else 0f
      }
      (normFeatures, label)
    }

    normalizedFeatures
  }

  //Scale the features
  val scaledFeatureDataset = scaleFeatures(featureDataset)

  //Print the features
  println("Features: " + scaledFeatureDataset.collect().map(tuple => (arrayFeatureToString(tuple._1), tuple._2)).mkString(", "))
  System.in.read() // Keep the application active.

}
