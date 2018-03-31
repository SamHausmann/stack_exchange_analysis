package XMLParse

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Badge(BadgeUserId: Int,
                 Name: String)

object Badges extends BaseFile {

  val filePath = FilePath("FinalProject/Badges.xml")

  val importantBadges : List[String] = List("Suffrage", "Electorate", "Civic Duty", "Explainer", "Refiner", "Nice Question")

  val badgesDFSchema = StructType(StructField("BadgeUserId", IntegerType, true) :: importantBadges.map(fieldName => StructField(fieldName, IntegerType, true)))

  private[XMLParse] def Parse(badge: String): Badge = {
    val xmlNode = scala.xml.XML.loadString(badge)
    Badge(
      (xmlNode \ "@UserId").text.toInt,
      (xmlNode \ "@Name").text)
  }
}