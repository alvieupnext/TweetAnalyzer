case class User(screenName: String, followersCount: Int, friendsCount: Int,
                listedCount: Int, favoritesCount: Int, statusesCount: Int, verified : Boolean)


object User {
  def parseUser(json: ujson.Value): User = {
    val screenName = json("screen_name").str
    val followersCount = json("followers_count").num.toInt
    val friendsCount = json("friends_count").num.toInt
    val listedCount = json("listed_count").num.toInt
    val favoritesCount = json("favourites_count").num.toInt
    val statusesCount = json("statuses_count").num.toInt
    val verified = json("verified").bool
    User(screenName, followersCount, friendsCount, listedCount, favoritesCount, statusesCount, verified)
  }
}

