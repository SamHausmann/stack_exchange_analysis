package XMLParse

case class Comment(CommentPostId: Int,
                   CommentUserId: Int,
                   Score: Int)

// File to hold comment information
object Comments extends BaseFile {

  val filePath = FilePath("Comments")

  private[XMLParse] def Parse(comment: String): Option[Comment] = {
    try {
      val xmlNode = scala.xml.XML.loadString(comment)
      Some(Comment(
        (xmlNode \ "@PostId").text.toInt,
        (xmlNode \ "@UserId").text.toInt,
        (xmlNode \ "@Score").text.toInt))
    } catch {
      case _: Exception => None
    }
  }
}