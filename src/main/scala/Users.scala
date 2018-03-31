package XMLParse

case class User(UserId: Int,
                Reputation: Int,
                UserCreationDate: Long,
                Age: Option[Int],
                AboutMeLength: Int,
                Views: Int,
                UpVotes: Int,
                DownVotes: Int)

object Users extends BaseFile {

  val filePath = FilePath("FinalProject/Users.xml")


  private[XMLParse] def Parse(user: String): User = {
    val xmlNode = scala.xml.XML.loadString(user)
    User(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@Reputation").text.toInt,
      parseDate(xmlNode \ "@CreationDate"),
      parseOptionInt(xmlNode \ "@Age"),
      (xmlNode \ "@AboutMe").text.split(" ").length,
      (xmlNode \ "@Views").text.toInt,
      (xmlNode \ "@UpVotes").text.toInt,
      (xmlNode \ "@DownVotes").text.toInt)
  }
}