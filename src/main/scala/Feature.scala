import org.apache.spark.rdd.RDD
import Metrics.Likes

object Feature {
  type Feature = Float
  //These are the features we will use to train our model
  //The features are text length,
  //From the user: followers count, friends count, listed count, favourites count, statuses count, verified,
  //The final dependent is the amount of likes

  //Create an Array[Feature] type which prints as a string
  def arrayFeatureToString(features: Array[Feature]): String = features.mkString("[", ",", "]")
  def arrayDoubleToString(features: Array[Double]): String = features.mkString("[", ",", "]")
  type FeatureTuple = (Array[Feature], Likes)

}
