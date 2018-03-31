package XMLParse

case class Comment(CommentPostId: Int,
                   Score: Int)

object Comments extends BaseFile {

  val filePath = FilePath("FinalProject/Comments.xml")

  private[XMLParse] def Parse(comment: String): Comment = {
    val xmlNode = scala.xml.XML.loadString(comment)
    Comment(
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@Score").text.toInt)
  }
}