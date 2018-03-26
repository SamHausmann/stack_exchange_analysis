package XMLParse

case class User(Id: Int,
                Reputation: Int,
                CreationDate: Long,
                EmailHash: String,
                LastAccessDate: Long,
                WebsiteUrl: String,
                Location: String,
                Age: Option[Int],
                AboutMe: String,
                Views: Int,
                UpVotes: Int,
                DownVotes: Int)

// Class to store information related to a specific User
object Users extends BaseFile {

  val filePath = FilePath("FinalProject/Users.xml")


  private[XMLParse] def Parse(user: String): User = {
    val xmlNode = scala.xml.XML.loadString(user)
    User(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@Reputation").text.toInt,
      parseDate(xmlNode \ "@CreationDate"),
      (xmlNode \ "@EmailHash").text,
      parseDate(xmlNode \ "@LastAccessDate"),
      (xmlNode \ "@WebsiteUrl").text,
      (xmlNode \ "@Location").text,
      parseOptionInt(xmlNode \ "@Age"),
      (xmlNode \ "@AboutMe").text,
      (xmlNode \ "@Views").text.toInt,
      (xmlNode \ "@UpVotes").text.toInt,
      (xmlNode \ "@DownVotes").text.toInt)
  }
}