package XMLParse

case class Post(Id: Int,
                PostTypeId: Int,
                ParentId: Option[Int],
                AcceptedAnswerId: Option[Int],
                CreationDate: Long,
                Score: Int,
                ViewCount: Option[Int],
                Body: String,
                OwnerUserId: Int,
                ClosedDate: Option[Long],
                CommentCount: Option[Int],
                FavoriteCount: Option[Int])

// Object to hold post information
object Posts extends BaseFile {

  val filePath = FilePath("Posts")

  private[XMLParse] def Parse(post: String): Option[Post] = {
    try {
      val xmlNode = scala.xml.XML.loadString(post)
      Some(Post(
        (xmlNode \ "@Id").text.toInt,
        (xmlNode \ "@PostTypeId").text.toInt,
        parseOptionInt(xmlNode \ "@ParentId"),
        parseOptionInt(xmlNode \ "@AcceptedAnswerId"),
        parseDate(xmlNode \ "@CreationDate"),
        (xmlNode \ "@Score").text.toInt,
        parseOptionInt(xmlNode \ "@ViewCount"),
        (xmlNode \ "@Body").text,
        (xmlNode \ "@OwnerUserId").text.toInt,
        parseOptionDate(xmlNode \ "@ClosedDate"),
        parseOptionInt(xmlNode \ "@CommentCount"),
        parseOptionInt(xmlNode \ "@FavoriteCount")))
    } catch {
      case _: Exception => None
    }
  }
}
