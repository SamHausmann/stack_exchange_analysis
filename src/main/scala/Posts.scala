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

object Posts extends BaseFile {

  val filePath = FilePath("FinalProject/Posts.xml")

  private[XMLParse] def Parse(post: String): Post = {
    val xmlNode = scala.xml.XML.loadString(post)
    Post(
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
    parseOptionInt(xmlNode \ "@FavoriteCount"))
  }
}