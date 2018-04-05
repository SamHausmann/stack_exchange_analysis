package XMLParse

case class PostHistory(HistoryPostId: Int,
                       HistoryUserId: Option[Int])

object PostHistories extends BaseFile {

  //val filePath = FilePath("PostHistory")

  private[XMLParse] def filePath(exchange: String, bucketName: String): String = {
    val fp = FilePath(exchange + "_PostHistory.xml", bucketName)
    fp
  }

  private[XMLParse] def Parse(postHistory: String): PostHistory = {
    val xmlNode = scala.xml.XML.loadString(postHistory)
    PostHistory(
      (xmlNode \ "@PostId").text.toInt,
      parseOptionInt(xmlNode \ "@UserId"))
  }
}