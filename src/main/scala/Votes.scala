package XMLParse

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Vote(VotePostId: Int, VoteTypeId: Int)

// Object to parse and hold vote information
object Votes extends BaseFile {

  // Types of votes that we deemed important
  val importantVoteTypes: Map[Int, String] = Map(1 -> "AcceptedByOriginator", 4 -> "Offensive", 5 -> "Favorite")
  val votesDFSchema = StructType(StructField("VotePostId", IntegerType, true) ::
    importantVoteTypes.values.map(field => StructField(field, IntegerType, true)).toList)

  val filePath = FilePath("Votes")

  private[XMLParse] def Parse(vote: String): Option[Vote] = {
    try {
      val xmlNode = scala.xml.XML.loadString(vote)
      Some(Vote((xmlNode \ "@PostId").text.toInt,
        (xmlNode \ "@VoteTypeId").text.toInt))
    } catch {
      case _: Exception => None
    }
  }
}