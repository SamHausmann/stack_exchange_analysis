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
                LastEditorUserId: Option[Int],
                LastEditorDisplayName: String,
                LastEditDate: Option[Long],
                LastActivityDate: Long,
                CommunityOwnedDate: Option[Long],
                ClosedDate: Option[Long],
                Title: String,
                Tags: String,
                AnswerCount: Option[Int],
                CommentCount: Option[Int],
                FavoriteCount: Option[Int])

// Class to store information related to a specific User
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
    parseOptionInt(xmlNode \ "@LastEditorUserId"),
    (xmlNode \ "@LastEditorDisplayName").text,
    parseOptionDate(xmlNode \ "@LastEditDate"),
    parseDate(xmlNode \ "@LastActivityDate"),
    parseOptionDate(xmlNode \ "@CommunityOwnedDate"),
    parseOptionDate(xmlNode \ "@ClosedDate"),
    (xmlNode \ "@Title").text,
    (xmlNode \ "@Tags").text,
    parseOptionInt(xmlNode \ "@AnswerCount"),
    parseOptionInt(xmlNode \ "@CommentCount"),
    parseOptionInt(xmlNode \ "@FavoriteCount"))
  }
}