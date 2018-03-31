package XMLParse

//import XMLParse.Badges.importantBadges
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Comment(CommentPostId: Int,
                   Score: Int)

// Class to store information related to a specific User
object Comments extends BaseFile {

  val filePath = FilePath("FinalProject/Comments.xml")

//  val commentsDFSchema = StructType(
//    StructField("PostId", IntegerType, true) ::
//      StructField("ScoreCount", IntegerType, true) :: Nil)


  private[XMLParse] def Parse(comment: String): Comment = {
    val xmlNode = scala.xml.XML.loadString(comment)
    Comment(
      (xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@Score").text.toInt)
  }
}