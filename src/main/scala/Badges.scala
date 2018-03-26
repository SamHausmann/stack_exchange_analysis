package XMLParse

case class Badge(Id: Int,
                 UserId: Int,
                 Name: String,
                 Date: Long)

// Class to store information related to a specific Badge
object Badges extends BaseFile {

  val filePath = FilePath("FinalProject/Badges.xml")

  private[XMLParse] def Parse(badge: String): Badge = {
    val xmlNode = scala.xml.XML.loadString(badge)
    Badge(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@UserId").text.toInt,
      (xmlNode \ "@Name").text,
      parseDate(xmlNode \ "@Date"))
  }
}