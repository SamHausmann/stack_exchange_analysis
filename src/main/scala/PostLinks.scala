package XMLParse

case class PostLink(Id: Int,
                CreationDate: Long,
                PostId: Int,
                RelatedPostId: Int,
                PostLinkTypeId: Option[Int])

// Class to store information related to a specific User
object PostLinks extends BaseFile {

  val filePath = FilePath("FinalProject/PostLinks.xml")

  private[XMLParse] def Parse(postLink: String): PostLink = {
    val xmlNode = scala.xml.XML.loadString(postLink)
    PostLink(
      (xmlNode \ "@Id").text.toInt,
      parseDate(xmlNode \ "@CreationDate"),
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@RelatedPostId").text.toInt,
      parseOptionInt(xmlNode \ "@PostLinkTypeId"))
  }
}