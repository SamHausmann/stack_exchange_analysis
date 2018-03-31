package XMLParse

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class Vote(VotePostId: Int, VoteTypeId: Int)

// Class to store information related to a specific User
object Votes extends BaseFile {

  val importantVoteTypes: Map[Int, String] = Map(1 -> "AcceptedByOriginator", 4 -> "Offensive", 5 -> "Favorite")

  val votesDFSchema = StructType(StructField("VotePostId", IntegerType, true) :: importantVoteTypes.values.map(field => StructField(field, IntegerType, true)).toList)

  val filePath = FilePath("FinalProject/Votes.xml")

  private[XMLParse] def Parse(vote: String): Vote = {
    val xmlNode = scala.xml.XML.loadString(vote)
    Vote((xmlNode \ "@PostId").text.toInt,
      (xmlNode \ "@VoteTypeId").text.toInt)
  }
}