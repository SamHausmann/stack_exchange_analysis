package XMLParse

case class PostLink(LinkPostId: Int)

object PostLinks extends BaseFile {

  //val filePath = FilePath("PostLinks")

  private[XMLParse] def filePath(exchange: String, bucketName: String): String = {
  	val fp = FilePath(exchange + "_PostLinks.xml", bucketName)
  	fp
  }

  private[XMLParse] def Parse(postLink: String): PostLink = {
    val xmlNode = scala.xml.XML.loadString(postLink)
    PostLink((xmlNode \ "@PostId").text.toInt)
  }
}