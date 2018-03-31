package XMLParse

case class PostHistory(HistoryPostId: Int)
// I'm not sure how UserId is an option, we should filter that out

// Class to store information related to a specific User
object PostHistories extends BaseFile {

  val filePath = FilePath("FinalProject/PostHistory.xml")

  private[XMLParse] def Parse(postHistory: String): PostHistory = {
    val xmlNode = scala.xml.XML.loadString(postHistory)
    PostHistory(
      (xmlNode \ "@PostId").text.toInt)
  }
}