//get Tag and Likes from Metrics
import Metrics.{Tag, Likes, ID}
//Import ujson to parse the tweets


case class Tweet(id: ID, text: String,
                 user: User,
                 hashTags: Array[Tag] = Array(),
                 likes: Likes = 0) {

  //Get the number of hashtags
  def numberOfHashTags: Int = hashTags.length

  //Get the length of the tweet
  def length: Int = text.length

  //Overwrite the toString method to display all hashtags via mkString
  override def toString: String = {
    s"Tweet($id, $length, ${user.screenName}, ${hashTags.mkString("[", ",", "]")}, $likes)"
  }

  //Change equality to only check the id
  override def equals(obj: Any): Boolean = obj match {
    case tweet: Tweet => tweet.id == id
    case _ => false
  }
}

object Tweet {
  // parseTweet should have a signature that accepts a ujson.Value and returns a Tweet
  def parseTweet(json: ujson.Value): Tweet = {
    // Parse the JSON to create a Tweet object
    val id = json("id_str").str
    val text = json("text").str
    val user = User.parseUser(json("user"))
    val hashTags = json("entities")("hashtags").arr.map(_("text").str).toArray
    val likes = json("favorite_count").num.toInt
    //Return the Tweet object
    Tweet(id, text, user, hashTags, likes)
  }

  // The parse method signature remains the same
  def parse(tweet: String): Option[Tweet] = {
    //try catch
    try {
      // The string is in a json format, so we can use the json library to parse it
      val jsonValue = ujson.read(tweet)
      // Check whether the tweet is a retweet or a quoted tweet
      (jsonValue.obj.get("retweeted_status"), jsonValue.obj.get("quoted_status")) match {
        case (Some(retweet), _) => Some(parseTweet(retweet))
        case (_, Some(quote)) => Some(parseTweet(quote))
        // If it is not a retweet or a quoted tweet, return None
        case _ => None
      }
    } catch {
      // If the parsing fails, return None, but print the tweet
      case e: Exception =>
//        println(s"Failed to parse tweet: $tweet")
//        println(e)
        None
    }
  }

  def getPartitionNumber(id: String): Int = {
    // Assuming id is always at least 2 characters long and contains only hexadecimal characters
    // Extract the last 2 characters of the id
    val lastTwo = id.takeRight(2)
    // Convert the last three characters to a number using hexadecimal base
    val number = Integer.parseInt(lastTwo, 10)
    // Use modulo to map the number to a range from 0 to 99
    number
  }

}
