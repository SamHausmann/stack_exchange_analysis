package XMLParse

case class Comment(CommentPostId: Int,
                   CommentUserId: Option[Int],
                   Score: Int)

object Comments extends BaseFile {

  //val filePath = FilePath("Comments")

  private[XMLParse] def filePath(exchange: String, bucketName: String): String = {
    val fp = FilePath(exchange + "_Comments.xml", bucketName)
    fp
  }

  private[XMLParse] def Parse(comment: String): Comment = {
    val xmlNode = scala.xml.XML.loadString(comment)
    Comment(
      (xmlNode \ "@PostId").text.toInt,
      parseOptionInt(xmlNode \ "@UserId"),
      (xmlNode \ "@Score").text.toInt)
  }
}