import Feature.Feature

case class User(screenName: String, followersCount: Int,
//                friendsCount: Int,
//                listedCount: Int,
                favoritesCount: Int,
                statusesCount: Int,
//                verified : Boolean
               ) {
  //Return the features of the User
  def features: Array[Feature] = Array(
    followersCount,
//    friendsCount,
//        listedCount,
//        statusesCount,
    favoritesCount,
//    if (verified) 1f else 0f
  )
}


object User {
  def parseUser(json: ujson.Value): User = {
    val screenName = json("screen_name").str
    val followersCount = json("followers_count").num.toInt
//    val friendsCount = json("friends_count").num.toInt
//    val listedCount = json("listed_count").num.toInt
    val favoritesCount = json("favourites_count").num.toInt
    val statusesCount = json("statuses_count").num.toInt
//    val verified = json("verified").bool
    User(screenName, followersCount,
//      friendsCount, listedCount,
      favoritesCount, statusesCount
      //      verified
    )
  }
}

