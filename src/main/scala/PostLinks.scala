package XMLParse

case class PostLink(LinkPostId: Int)

object PostLinks extends BaseFile {

  val filePath = FilePath("FinalProject/PostLinks.xml")

  private[XMLParse] def Parse(postLink: String): PostLink = {
    val xmlNode = scala.xml.XML.loadString(postLink)
    PostLink((xmlNode \ "@PostId").text.toInt)
  }
}