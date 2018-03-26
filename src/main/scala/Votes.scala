package XMLParse

case class Vote(Id: Int,
                PostId: Int,
                VoteTypeId: Int,
                CreationDate: Long,
                UserId: Option[Int],
                BountyAmount: Option[Int])

// Class to store information related to a specific User
object Votes extends BaseFile {

  val filePath = FilePath("FinalProject/Votes.xml")

  private[XMLParse] def Parse(vote: String): Vote = {
    val xmlNode = scala.xml.XML.loadString(vote)
    Vote(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@VoteTypeId").text.toInt,
      parseDate(xmlNode \ "@CreationDate"),
      parseOptionInt(xmlNode \ "@UserId"),
      parseOptionInt(xmlNode \ "@BountyAmount"))
  }
}