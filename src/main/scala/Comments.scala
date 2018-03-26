package XMLParse

case class Comment(Id: Int,
                   PostId: Int,
                   Score: Int,
                   Text: String,
                   CreationDate: Long,
                   UserId: Option[Int])
// I'm not sure how this UserId is an option ID, we should filter those without ID's

// Class to store information related to a specific User
object Comments extends BaseFile {

  val filePath = FilePath("FinalProject/Comments.xml")


  private[XMLParse] def Parse(comment: String): Comment = {
    val xmlNode = scala.xml.XML.loadString(comment)
    Comment(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@Score").text.toInt,
      (xmlNode \ "@Text").text,
      parseDate(xmlNode \ "@CreationDate"),
      parseOptionInt(xmlNode \ "@UserId"))
  }
}