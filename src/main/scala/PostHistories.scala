package XMLParse

case class PostHistory(HistoryPostId: Int)

object PostHistories extends BaseFile {

  val filePath = FilePath("FinalProject/PostHistory.xml")

  private[XMLParse] def Parse(postHistory: String): PostHistory = {
    val xmlNode = scala.xml.XML.loadString(postHistory)
    PostHistory(
      (xmlNode \ "@PostId").text.toInt)
  }
}