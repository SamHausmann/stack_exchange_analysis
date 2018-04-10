package XMLParse

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Badge(BadgeUserId: Int,
                 Name: String)

// Object to store information about Badges
object Badges extends BaseFile {

  val filePath = FilePath("Badges")

  // List of badges that we deemed potentially important
  val importantBadges : List[String] = List("Suffrage", "Electorate", "Civic Duty", "Explainer", "Refiner", "Nice Question")
  // Schema created from above badges and UserId
  val badgesDFSchema = StructType(StructField("BadgeUserId", IntegerType, true) ::
    importantBadges.map(fieldName => StructField(fieldName, IntegerType, true)))

  private[XMLParse] def Parse(badge: String): Option[Badge] = {
    try {
      val xmlNode = scala.xml.XML.loadString(badge)
      Some(Badge(
        (xmlNode \ "@UserId").text.toInt,
        (xmlNode \ "@Name").text))
    } catch {
      case _: Exception => None
    }
  }
}