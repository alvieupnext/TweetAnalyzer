import Feature.{Feature, FeatureTuple, arrayFeatureToString}
import Metrics.{ID, Likes, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import java.io.{BufferedWriter, FileWriter}


import scala.annotation.tailrec
import scala.util.control.Breaks.break

object MLR extends App {

  val conf = new SparkConf()
  conf.setAppName("Multivariate Linear Regression")
  // Uncomment when running locally
//  conf.setMaster("local[4]")
  val sc = new SparkContext(conf)

  // Create a SparkSession which is required for working with Dataset
  val spark = SparkSession.builder().config(conf).getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

  // Import the implicits, this is now done from the SparkSession (not from SparkContext)
  import spark.implicits._

  val desiredPartitions = 100

  // Define a custom partitioner which groups tweets by their id
  class IDPartitioner(partitions: Int = desiredPartitions) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      // Assuming key is the tweet.id
      val id = key.toString
      Tweet.getPartitionNumber(id)
    }
  }

 // Load the possible tweets
  val nonUniqueTweets: RDD[Tweet] = sc.textFile("/data/twitter/tweetsraw")
    //first parse the tweets, transform RDD to a Dataset and then get all the tweets out of the option
    .flatMap(Tweet.parse)

  //Print the first ten non unique tweets
//  nonUniqueTweets.take(10).foreach(println)

  //Get the id per tweet
  val nonUniqueTweetsWithId: RDD[(ID, Tweet)] = nonUniqueTweets.map(tweet => (tweet.id, tweet))

  //Repartition the tweets by their id
  val repartitionedTweetsWithId: RDD[(ID, Tweet)] = nonUniqueTweetsWithId.partitionBy(new IDPartitioner(desiredPartitions))

  //Filter all the unique tweets (keep the tweet with the most likes) and turn into a Dataset
  //Keep tweets in the Disk only storage level to not burden the memory
  val tweets: Dataset[Tweet] = repartitionedTweetsWithId
    .reduceByKey((t1, t2) => if (t1.likes > t2.likes) t1 else t2).map(_._2).toDS().persist(StorageLevel.DISK_ONLY)

  //Print the amount of tweets
//  println("Amount of tweets: " + tweets.count())

  //A random seed to make sure the random split is the same every time
  val randomSeed = 42

  //Split the tweets into a training set and a test set
  val Array(trainingTweets, testTweets) = tweets.randomSplit(Array(0.8, 0.2), randomSeed)

  import org.apache.spark.sql.functions._

  def extractHashtagMetrics(tweets: Dataset[Tweet]): Dataset[(Tweet, Long)] = {

    // Collect all the hashtags (keep them grouped by tweet)
    val hashtags: Dataset[(Tweet, Tag)] = tweets
      .flatMap(tweet => tweet.hashTags.map(hashtag => (tweet, hashtag.toLowerCase))).repartition(desiredPartitions, col("_2"))

//    print("Amount of hashtags: " + hashtags.count())

    // Define a DataFrame with hashtags and their counts
    val hashtagCounts: Dataset[(Tag, Long)] = hashtags
      .groupByKey(p => p._2)// Group by hashtag and rename the column
      .count()
    // Join the original hashtags dataset with the counts
    // Here, we use a broadcast join if hashtagCounts is small to optimize the join
    val hashtagGroupsJoined: Dataset[(Tweet, Long)] = hashtags
      .joinWith(hashtagCounts, hashtags("_2") === hashtagCounts("key"))
      .map{case (p1, p2) => (p1._1, p2._2)}

    // Repartition the tweets with hashtag counts exactly like the regular tweets
    // Makes the subsequent groupBy tweet not shuffled and
    // makes the later join with the original tweets Dataset not shuffled
    val withPartitionNumber = hashtagGroupsJoined.map { case (tweet, count) =>
      val partitionNumber = Tweet.getPartitionNumber(tweet.id)
      (tweet, count, partitionNumber)
    }

    val repartitionedHashtagGroups = withPartitionNumber.repartition(desiredPartitions, $"_3") // 100 partitions


    val tweetScoresGrouped: Dataset[(Tweet, Long)] = repartitionedHashtagGroups
      // Remove the partition number
      .map{case (tweet, count, _) => (tweet, count)}
      // Group by tweet
      .groupByKey(_._1)
      // Only retain the count (transform to feature)
      .mapValues(_._2)
      // Reduce by summing the values
      .reduceGroups(_+_)
    tweetScoresGrouped
  }


  val trainingTweetsWithHashtags: Dataset[(Tweet, Long)] = extractHashtagMetrics(trainingTweets)

  //Print the amount of tweets with hashtags
//  println("Amount of training tweets with hashtags: " + trainingTweetsWithHashtags.count())

  def extractFeatures(tweets: Dataset[Tweet], tweetsWithHashTags: Dataset[(Tweet, Long)]): Dataset[FeatureTuple] = {

    //Perform a left outer join on the tweets and the tweets with hashtags
    val tweetsWithHashTagsFeatures: Dataset[(Tweet, Long)] = tweets.joinWith(tweetsWithHashTags, tweets("id") === tweetsWithHashTags("key.id"), "left_outer")
      //If the tweet doesn't have a hashtag, default to 0
      .map {
        case (tweet, (hashtagTweet, hashtagScore)) if hashtagTweet != null => (tweet, hashtagScore)
        case (tweet, _) => (tweet, 0)
      }
    //Extract the features from the tweets
    tweetsWithHashTagsFeatures.map{ case (tweet: Tweet, hashtagCount: Long) =>
      //Get the features from the tweet
      val tweetLength = tweet.textLength.toFloat
      val user = tweet.user
      val userFeatures: Array[Float] = user.features
      //Return the features, remember to add x0 = 1
      (Array(1f, tweetLength,
        hashtagCount.toFloat
      ) ++ userFeatures, tweet.likes)
    }
  }
//
  //Extract the features from the tweets
  val featureDataset: Dataset[FeatureTuple] = extractFeatures(trainingTweets, trainingTweetsWithHashtags).persist()

//  println("Printing regular features:")
//  //Print the first ten features
//  featureDataset.take(10).map{case (tuple: Array[Feature], dependent: Feature) => (arrayFeatureToString(tuple), dependent)}.foreach(println)

  //Get the amount of DataPoints (tweets). This should also persist the featureDataset
  val amountOfDataPoints = featureDataset.count()

  //A procedure to scale the features by normalizing them

  def scaleFeatures(features: Dataset[FeatureTuple], m: Long = amountOfDataPoints): Dataset[FeatureTuple] = {

    // Calculate the mean
    val mean: Array[Feature] = features.map{ case (f: Array[Feature], label: Feature) => f :+ label }
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / m)

    // Print the mean
    println("Mean: " + arrayFeatureToString(mean))

    // Calculate the standard deviation
    val stddev: Array[Feature] = features.map{ case (f: Array[Feature], label: Feature) => f :+ label }
      .map(f => f.zip(mean).map { case (x, y) => Math.pow(x - y, 2) })
      .reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
      .map(_ / m)
      .map(math.sqrt)
      .map(_.toFloat)

    // Print the standard deviation
    println("Stddev: " + arrayFeatureToString(stddev))
    println("Broadcasting")

    // Broadcast the mean and standard deviation
    val meanBroadcast: Broadcast[Array[Feature]] = features.sparkSession.sparkContext.broadcast(mean)
    val stddevBroadcast: Broadcast[Array[Feature]] = features.sparkSession.sparkContext.broadcast(stddev)

    // Normalize the features using the broadcast variables
    val normalizedFeatures = features.map { case (features, label) =>
      val f = features :+ label
      val normFeatures = f.zip(meanBroadcast.value).zip(stddevBroadcast.value).map {
        case ((feature, mean), stdDev) => if (stdDev != 0) (feature - mean) / stdDev else 0f
      }
      // Extract the final feature (the label) and remove it from the features
      val normLabel = normFeatures.last
      val normFeaturesWithoutLabel = normFeatures.dropRight(1)

      (normFeaturesWithoutLabel, normLabel)
    }

    normalizedFeatures
  }


  //Scale the features
  val scaledFeatureDataset = scaleFeatures(featureDataset).persist()

//  println("Printing scaled features:")
//
//  //Print the first ten scaled features
//  scaledFeatureDataset.take(10).map{case (tuple: Array[Feature], dependent: Feature) => (arrayFeatureToString(tuple), dependent)}.foreach(println)

  //Implement the gradient descent algorithm
  //First by implementing the cost function
  type Theta = Array[Float]

  def cost(features: Dataset[FeatureTuple], theta: Theta, m: Long = amountOfDataPoints): Float = {
    //Calculate the cost
    val cost = features.map { case (f, label) =>
      val h = f.zip(theta).map { case (x, y) => x * y }.sum
      Math.pow(h - label, 2)
    }.reduce(_ + _)
    //Return the cost
    cost.toFloat / (2 * m)
  }

  def gradientDescent(features: Dataset[FeatureTuple], initialTheta: Theta, alpha: Float, sigma: Float, file: BufferedWriter, initialError: Float, m: Long = amountOfDataPoints): Theta = {
    var error = initialError
    var theta = initialTheta
    var iteration = 0

    // Broadcast the initial theta
    var thetaBroadcast: Broadcast[Theta] = features.sparkSession.sparkContext.broadcast(theta)

    while (true) {
      // Update theta
      val newTheta = theta.zipWithIndex.map { case (currentTheta, j) =>
        val sum = features.map { case (f, label) =>
          val localTheta = thetaBroadcast.value
          val h = f.zip(localTheta).map { case (x, y) => x * y }.sum
          (h - label) * f(j)
        }.reduce(_ + _)
        currentTheta - alpha * sum / m
      }

      // Destroy the old broadcast variable and broadcast the new theta
      thetaBroadcast.destroy()
      thetaBroadcast = features.sparkSession.sparkContext.broadcast(newTheta)

      // Calculate new cost using the updated theta
      val newCost = cost(features, thetaBroadcast.value, m)
      val newDelta = error - newCost

      println(s"Iteration: $iteration")
      println("Previous error: " + error)
      println("New error: " + newCost)
      file.write(s"$iteration, $newCost\n")

      if (newDelta < sigma) {
        theta = thetaBroadcast.value
        // Release the broadcast variable resources
        thetaBroadcast.destroy()
        return theta
      }
      else {
        error = newCost
        theta = newTheta
        iteration += 1
      }
    }
    theta
  }



  //Get the amount of features (via counting the array length of the first feature tuple)
  val amountOfFeatures = scaledFeatureDataset.first()._1.length

  //Initialize the theta
  val theta = Array.fill(amountOfFeatures)(0f)

  //Get the initial cost
  val initialCost = cost(scaledFeatureDataset, theta)

  //The cost function now has used the scaled features, so we can free up the regular features
  featureDataset.unpersist()

  val alpha = 0.1f

  //Generate a filename based on alpha and the amount of features
  val filename = "error_log_" + alpha + "_" + theta.length + ".txt"

  // Initialize file writer
  val file = new BufferedWriter(new FileWriter(filename))

  //Perform the gradient descent (1 to the power of -12 is the sigma)
  val newTheta = gradientDescent(scaledFeatureDataset, theta, alpha, 1e-9f, file, initialCost)

  //Print the new theta
  println("New theta: " + arrayFeatureToString(newTheta))

  //Unpersist the scaled feature dataset
  scaledFeatureDataset.unpersist()


  val testTweetsWithHashtags: Dataset[(Tweet, Long)] = extractHashtagMetrics(testTweets)

  //Use the testTweets to test the model
  val testFeatures = extractFeatures(testTweets, testTweetsWithHashtags).persist()

  //Count the testFeatures
  val amountOfTestFeatures = testFeatures.count()

  //Scale the test features
  val scaledTestFeatures = scaleFeatures(testFeatures, amountOfTestFeatures)

  //Calculate the cost of the test features
  val testCost = cost(scaledTestFeatures, newTheta, amountOfTestFeatures)

  //Print the cost
  println("Test cost: " + testCost)

  //Write the test cost to the file
  file.write(s"$testCost\n")

  //Close the file
  file.close()
//
//
  System.in.read() // Keep the application active.

}
