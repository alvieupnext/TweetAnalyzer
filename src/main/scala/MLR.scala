import Feature.{Feature, FeatureTuple, arrayFeatureToString}
import Metrics.{Likes, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
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
  val nonUniqueTweets: Dataset[Tweet] = possibleTweets.filter(_.isDefined).map(_.get)

  //Filter all the unique tweets (keep the tweet with the most likes)
  val tweets: Dataset[Tweet] = nonUniqueTweets
    .groupByKey(_.id).reduceGroups((t1, t2) => if (t1.likes > t2.likes) t1 else t2).map(_._2).persist()

  //first, collect all the hashtags (keep them grouped by tweet)
  val hashtags: Dataset[(Tweet, Metrics.Tag)] = tweets.flatMap(tweet => tweet.hashTags.map(hashtag => (tweet, hashtag))).persist()

  //then we count the hashtags by the amount of times they occur (make all hashtags lowercase)
  val hashtagGroups: KeyValueGroupedDataset[String, (Tweet, Tag)] = hashtags.groupByKey(_._2.toLowerCase)

  val hashtagCounts = hashtagGroups.count()

  //We get the total amount of times any hashtag has appeared
  //Make it a Float to make sure the division is a float division
  val totalHashtagCount = hashtags.count().toFloat

//  //Sort the hashtagCount in descending order, in case of a tie, sort by the hashtag alphabetically
//  val sortedHashtagCounts = hashtagCounts.sort($"count(1)".desc, $"key".asc).persist()
//
//  //print the sortedHashtagCounts
//  println("Sorted hashtag counts: " + sortedHashtagCounts.collect().mkString(", "))

//  //Divide the hashtags into groups
//  val amountOfHashtagGroups = 2

  //Based on the totalHashtag

  //Then we divide the amount of times a hashtag has appeared by the total amount of hashtags to get a distribution
  val hashtagDistribution = hashtagCounts.map(h => (h._1, h._2 / totalHashtagCount))

  //Get the highest distribution
  val highestDistribution = hashtagDistribution.sort($"_2".desc).first()._2

  val amountOfHashtagGroups = 10

  //Normalize the distribution (10 should correspond to the highest distribution)
  //The double should become a natural number between 0 and amountOfHashtagGroups
  val normalizedHashtagDistribution: Dataset[(Tag, Int)] = hashtagDistribution.map(h => (h._1, (h._2 / highestDistribution * amountOfHashtagGroups).toInt))

  //Join the hashtags with the normalized distribution
  val hashtagGroupsJoined: Dataset[((Tweet, Tag), (Tag, Int))] = hashtags.joinWith(normalizedHashtagDistribution, hashtags("_2") === normalizedHashtagDistribution("_1")).persist()

  //Remove the hashtag from both sides of the join
  val hashtagDistributionJoined: Dataset[(Tweet, Int)] = hashtagGroupsJoined.map(h => (h._1._1, h._2._2))

  //Group the hashtag scores by tweet
  val hashtagDistributionGrouped: KeyValueGroupedDataset[Tweet, (Tweet, Int)] = hashtagDistributionJoined.groupByKey(_._1)

  //Group the hashtag scores by tweet and collect the scores
  val tweetScoresGrouped: Dataset[(Tweet, Array[Int])] = hashtagDistributionGrouped.mapGroups { (tweet, scoresIterator) =>
    (tweet, scoresIterator.map(_._2).toArray)
  }.persist()

  //Procedure for transforming the hashtag scores to features
  def hashtagScoresToFeatures(scores: Array[Int]): Array[Feature] = {
    //Generate an array from 0 to 10 (possible distribution values)
    val possibleDistributions = (0 to amountOfHashtagGroups).toArray
    //For each possible distribution, count the amount of times it occurs in the tweet
    val hashtagFeatures: Array[Float] = possibleDistributions.map(d => scores.count(_ == d))
    hashtagFeatures
  }


  //Transform the tweet scores to features
  val tweetFeatures: Dataset[(Tweet, Array[Feature])] = tweetScoresGrouped.map { case (tweet, scores) =>
    (tweet, hashtagScoresToFeatures(scores))
  }

  //Only get the id from the tweet and then print tweetfeatures
  println("Tweet features: " + tweetFeatures.map(t => (t._1.id, arrayFeatureToString(t._2))).collect().mkString(", "))

  def extractFeatures(tweets: Dataset[(Tweet, Array[Float])]): Dataset[FeatureTuple] = {
    //Extract the features from the tweets
    tweets.map(tweetTuple => { case (tweet: Tweet, hashtagFeatures) =>
      //Get the features from the tweet
      val tweetLength = tweet.length.toFloat
      val user = tweet.user
      val userFeatures: Array[Float] = user.features
      //Return the features, remember to add x0 = 1
      (Array(1f, tweetLength) ++ userFeatures ++ hashtagFeatures, tweet.likes)
    })
  }

  //Extract the features from the tweets
  val featureDataset: Dataset[FeatureTuple] = extractFeatures(tweetFeatures)

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
  val scaledFeatureDataset = scaleFeatures(featureDataset).persist()

  //Print the features
  println("Features: " + scaledFeatureDataset.collect().map(tuple => (arrayFeatureToString(tuple._1), tuple._2)).mkString(", "))

  //Implement the gradient descent algorithm
  //First by implementing the cost function
  type Theta = Array[Float]

  def cost(features: Dataset[FeatureTuple], theta: Theta): Float = {
    //Get the amount of tweets (via counting the features)
    val m = features.count().toFloat
    //Calculate the cost
    val cost = features.map { case (f, label) =>
      val h = f.zip(theta).map { case (x, y) => x * y }.sum
      Math.pow(h - label, 2)
    }.reduce(_ + _)
    //Return the cost
    cost.toFloat / (2 * m)
  }

  //Gradient descent
//  def gradientDescent(features: Dataset[FeatureTuple], theta: Theta, alpha: Float, iterations: Int): Theta = {
//    //Get the amount of tweets (via counting the features)
//    val m = features.count().toFloat
//    //For each iteration, update theta
//    val updatedTheta = (0 until iterations).foldLeft(theta) { case (theta, _) =>
//      //Calculate the new theta
//      val newTheta = theta.zipWithIndex.map { case (theta, j) =>
//        val h = features.map { case (f, label) =>
//          val h = f.zip(theta).map { case (x, y) => x * y }.sum
//          (h - label) * f(j)
//        }.reduce(_ + _)
//        theta - alpha * h.toFloat / m
//      }
//      //Return the new theta
//      newTheta
//    }
//    //Return the updated theta
//    updatedTheta
//  }

  System.in.read() // Keep the application active.

}
