package XMLParse

case class PostHistory(Id: Int,
                PostHistoryTypeId: Int,
                PostId: Int,
                RevisionGUID: String,
                CreationDate: Long,
                UserId: Option[Int],
                UserDisplayName: String,
                Comment: String,
                Text: String,
                CloseReasonId: Option[Int])
// I'm not sure how UserId is an option, we should filter that out

// Class to store information related to a specific User
object PostHistories extends BaseFile {

  val filePath = FilePath("FinalProject/PostHistory.xml")

  private[XMLParse] def Parse(postHistory: String): PostHistory = {
    val xmlNode = scala.xml.XML.loadString(postHistory)

    PostHistory(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@PostHistoryTypeId").text.toInt,
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@RevisionGUID").text,
      parseDate(xmlNode \ "@CreationDate"),
      parseOptionInt(xmlNode \ "@UserId"),
      (xmlNode \ "@UserDisplayName").text,
      (xmlNode \ "@Comment").text,
      (xmlNode \ "@Text").text,
      parseOptionInt(xmlNode \ "@CloseReasonId"))
  }
}