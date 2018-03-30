package XMLParse

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Badge(Id: Int,
                 UserId: Int,
                 Name: String,
                 Date: Long)


// Class to store information related to a specific Badge
object Badges extends BaseFile {

  val filePath = FilePath("FinalProject/Badges.xml")

  val importantBadges : List[String] = List("Suffrage", "Electorate", "Civic Duty", "Explainer", "Refiner", "Nice Question")

  val badgesSchema = StructType(StructField("UserId", IntegerType, true) :: importantBadges.map(fieldName => StructField(fieldName, IntegerType, true)))

  private[XMLParse] def MapListOfBadgesToCounts(badges: List[String]): List[Int] = {
    importantBadges.map(badge => badges.count(_ == badge))
  }

  private[XMLParse] def Parse(badge: String): Badge = {
    val xmlNode = scala.xml.XML.loadString(badge)
    Badge(
      (xmlNode \ "@Id").text.toInt,
      (xmlNode \ "@UserId").text.toInt,
      (xmlNode \ "@Name").text,
      parseDate(xmlNode \ "@Date"))
  }
}