//get Tag and Likes from Metrics
import Metrics.{Tag, Likes}
import upickle.default._


case class Tweet(text: String,
                 user: String,
                 hashTags: Array[Tag],
                 likes: Likes)

object Tweet {
  // parseTweet should have a signature that accepts a ujson.Value and returns a Tweet
  def parseTweet(json: ujson.Value): Tweet = {
    // Parse the JSON to create a Tweet object
    val text = json("text").str
    val user = json("user")("screen_name").str
    val hashTags = json("entities")("hashtags").arr.map(_ ("text").str).toArray
    val likes = json("favorite_count").num.toInt
    //Return the Tweet object
    Tweet(text, user, hashTags, likes)
  }

  // The parse method signature remains the same
  def parse(tweet: String): Option[Tweet] = {
    // The string is in a json format, so we can use the json library to parse it
    val jsonValue = ujson.read(tweet)
    // Check whether the tweet is a retweet or a quoted tweet
    (jsonValue.obj.get("retweeted_status"), jsonValue.obj.get("quoted_status")) match {
      case (Some(retweet), _) if retweet.obj.nonEmpty => Some(parseTweet(retweet))
      case (_, Some(quote)) if quote.obj.nonEmpty => Some(parseTweet(quote))
      // If it is not a retweet or a quoted tweet, return None
      case _ => None
    }
  }
}
