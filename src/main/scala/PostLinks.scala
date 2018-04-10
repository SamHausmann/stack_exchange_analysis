package XMLParse

case class PostLink(LinkPostId: Int)

// Object to hold post link information
object PostLinks extends BaseFile {

  val filePath = FilePath("PostLinks")

  private[XMLParse] def Parse(postLink: String): Option[PostLink] = {
    try {
      val xmlNode = scala.xml.XML.loadString(postLink)
      Some(PostLink((xmlNode \ "@PostId").text.toInt))
    } catch {
      case _: Exception => None
    }
  }
}