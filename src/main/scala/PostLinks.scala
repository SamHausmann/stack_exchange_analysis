package XMLParse

case class PostLink(LinkPostId: Int, IsValid: Boolean)

object PostLinks extends BaseFile {

  //val filePath = FilePath("PostLinks")

  private[XMLParse] def filePath(exchange: String, bucketName: String): String = {
  	val fp = FilePath(exchange + "_PostLinks.xml", bucketName)
  	fp
  }

  private[XMLParse] def Parse(postLink: String): PostLink = {
    try {
      val xmlNode = scala.xml.XML.loadString(postLink)
      PostLink((xmlNode \ "@PostId").text.toInt, true)
    } catch {
      case _: Exception => PostLink(0, false)
    }
  }
}