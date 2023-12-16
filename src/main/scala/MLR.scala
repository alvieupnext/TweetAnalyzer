import Feature.{Feature, FeatureTuple, arrayFeatureToString}
import Metrics.{Likes, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

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
  val nonUniqueTweets: Dataset[Tweet] = sc.textFile("data/twitter/tweetsraw")
    //first parse the tweets, transform RDD to a Dataset and then get all the tweets out of the option
    .flatMap(Tweet.parse).toDS

  //Filter all the unique tweets (keep the tweet with the most likes)
  val tweets: Dataset[Tweet] = nonUniqueTweets
    .groupByKey(_.id).reduceGroups((t1, t2) => if (t1.likes > t2.likes) t1 else t2).map(_._2).persist()

  //A random seed to make sure the random split is the same every time
  val randomSeed = 42

  //Split the tweets into a training set and a test set
  val Array(trainingTweets, testTweets) = tweets.randomSplit(Array(0.8, 0.2), randomSeed)

  def extractHashtagMetrics(tweets: Dataset[Tweet], amountOfHashtagGroups: Int = 10): Dataset[(Tweet, Array[Feature])] = {

    // Collect all the hashtags (keep them grouped by tweet)
    val hashtags = tweets.flatMap(tweet => tweet.hashTags.map(hashtag => (tweet, hashtag.toLowerCase))).persist()

    // Count the hashtags by the amount of times they occur (make all hashtags lowercase)
    val hashtagGroups = hashtags.groupByKey(_._2)

    val hashtagCounts = hashtagGroups.count()

    // Get the total amount of times any hashtag has appeared
    val totalHashtagCount = hashtags.count().toFloat

    // Divide the amount of times a hashtag has appeared by the total amount of hashtags to get a distribution
    val hashtagDistribution = hashtagCounts.map(h => (h._1, h._2 / totalHashtagCount))

    // Get the highest distribution
    val highestDistribution = hashtagDistribution.map(_._2).reduce((a, b) => if (a > b) a else b)

    // Normalize the distribution
    val normalizedHashtagDistribution = hashtagDistribution.map(h => (h._1, (h._2 / highestDistribution * amountOfHashtagGroups).toInt))

    // Join the hashtags with the normalized distribution
    val hashtagGroupsJoined: Dataset[((Tweet, Tag), (Tag, Int))] = hashtags.joinWith(normalizedHashtagDistribution, hashtags("_2") === normalizedHashtagDistribution("_1")).persist()

    // Remove the hashtag from both sides of the join
    val hashtagDistributionJoined = hashtagGroupsJoined.map(h => (h._1._1, h._2._2))

    // Group the hashtag scores by tweet
    val hashtagDistributionGrouped: KeyValueGroupedDataset[Tweet, (Tweet, Int)] = hashtagDistributionJoined.groupByKey(_._1)

    // and collect the scores
    val tweetScoresGrouped = hashtagDistributionGrouped.mapGroups { (tweet, scoresIterator) =>
      (tweet, scoresIterator.map(_._2).toArray)
    }

    // Transform the tweet scores to features
    tweetScoresGrouped.map { case (tweet, scores) =>
      val possibleDistributions = (0 to amountOfHashtagGroups).toArray
      val hashtagFeatures = possibleDistributions.map(d => scores.count(_ == d).toFloat)
      (tweet, hashtagFeatures)
    }
  }

  val amountOfHashTagGroups = 4

  val tweetsWithHashtags: Dataset[(Tweet, Array[Feature])] = extractHashtagMetrics(trainingTweets, amountOfHashTagGroups).persist()

  //Print the amount of tweet features
  println("Amount of tweets with hashtags: " + tweetsWithHashtags.count())

  def extractFeatures(tweets: Dataset[Tweet], tweetsWithHashTags: Dataset[(Tweet, Array[Float])]): Dataset[FeatureTuple] = {
    // Define the default array
    val defaultArray = Array.fill(amountOfHashTagGroups)(0f)

    //Perform a left outer join on the tweets and the tweets with hashtags
    val tweetsWithHashTagsFeatures = tweets.joinWith(tweetsWithHashTags, tweets("id") === tweetsWithHashTags("_1.id"), "left_outer")
      //Fill the missing values with the default array
      .map {
        case (tweet, (hashtagTweet, hashtags)) if hashtagTweet != null => (tweet, hashtags)
        case (tweet, _) => (tweet, defaultArray)
      }
    //Extract the features from the tweets
    tweetsWithHashTagsFeatures.map{ case (tweet: Tweet, hashtagFeatures) =>
      //Get the features from the tweet
      val tweetLength = tweet.length.toFloat
      val user = tweet.user
      val userFeatures: Array[Float] = user.features
      //Return the features, remember to add x0 = 1
      (Array(1f, tweetLength) ++ userFeatures ++ hashtagFeatures, tweet.likes)
    }
  }
//
  //Extract the features from the tweets
  val featureDataset: Dataset[FeatureTuple] = extractFeatures(tweets, tweetsWithHashtags).persist()

  //Print the amount of features
  println("Amount of features: " + featureDataset.count())

  //Get the amount of features (via counting the array length of the first feature tuple)
  val amountOfFeatures = featureDataset.first()._1.length

  //A procedure to scale the features by normalizing them
  def scaleFeatures(features: Dataset[FeatureTuple]): Dataset[FeatureTuple] = {
    // Extract the feature array from the feature tuples
    val featureArray = features.map(_._1)

    // Calculate the mean and standard deviation for each feature
    val mean: Array[Float] = featureArray
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / amountOfFeatures)

    //Print the mean
    println("Mean: " + arrayFeatureToString(mean))

    //Calculate the standard deviation
    val stddev: Array[Float] = featureArray
      .map(f => f.zip(mean).map { case (x, y) => Math.pow(x - y, 2) })
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / amountOfFeatures)
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

  //Implement the gradient descent algorithm
  //First by implementing the cost function
  type Theta = Array[Float]

  def cost(features: Dataset[FeatureTuple], theta: Theta): Float = {
    //Calculate the cost
    val cost = features.map { case (f, label) =>
      val h = f.zip(theta).map { case (x, y) => x * y }.sum
      Math.pow(h - label, 2)
    }.reduce(_ + _)
    //Return the cost
    cost.toFloat / (2 * amountOfFeatures)
  }

  def gradientDescent(features: Dataset[FeatureTuple], theta: Theta, alpha: Float, sigma: Float, maxIterations: Int): Theta = {
    var error = cost(features, theta)
    var newTheta = theta
    var iteration = 0

    while (iteration < maxIterations) {
      newTheta = newTheta.zipWithIndex.map { case (currentTheta, j) =>
        val h = features.map { case (f, label) =>
          val h = f.zip(newTheta).map { case (x, y) => x * y }.sum
          (h - label) * f(j)
        }.reduce(_ + _)
        currentTheta - alpha * h / amountOfFeatures
      }

      val newCost = cost(features, newTheta)
      val newDelta = error - newCost

      println(s"Iteration: $iteration")
      println("Previous error: " + error)
      println("New error: " + newCost)

      if (newDelta < sigma) {
        return newTheta
      } else {
        error = newCost
        iteration += 1
      }
    }

    newTheta
  }

  //Initialize the theta
  val theta = Array.fill(scaledFeatureDataset.first()._1.length)(0f)

  //Perform the gradient descent
  val newTheta = gradientDescent(scaledFeatureDataset, theta, 0.01f, 0.1f, 10)

  //Print the new theta
  println("New theta: " + arrayFeatureToString(newTheta))

  val testTweetsWithHashtags: Dataset[(Tweet, Array[Feature])] = extractHashtagMetrics(testTweets, amountOfHashTagGroups).persist()

  //Use the testTweets to test the model
  val testFeatures = extractFeatures(testTweets, testTweetsWithHashtags).persist()

  //Calculate the cost of the test features
  val testCost = cost(testFeatures, newTheta)

  //Print the cost
  println("Test cost: " + testCost)


  System.in.read() // Keep the application active.

}
