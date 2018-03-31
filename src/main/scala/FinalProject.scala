package XMLParse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD


import scala.collection.mutable.ListBuffer

object FinalProject {

  val conf: SparkConf = new SparkConf().setAppName("SimpleSpark").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
//  sc.hadoopConfiguration.set("fs.s3a.access.key", "")
//  sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  val dataPointRow : String = "  <row "

  def main(args: Array[String]) {

//    val test = sc.textFile("s3a://stack.exchange.analysis/testfile.xml")
//    test.collect().map(println)


    val badgesRdd: RDD[Badge] = sc.textFile(Badges.filePath).filter(s => s.startsWith(dataPointRow)).map(Badges.Parse).cache()
//    badgesRdd.collect().map(println)
    val commentsRdd : RDD[Comment] = sc.textFile(Comments.filePath).filter(s => s.startsWith(dataPointRow)).map(Comments.Parse).cache()
//    commentsRdd.collect().map(println)
    val postsRdd : RDD[Post] = sc.textFile(Posts.filePath).filter(s => s.startsWith(dataPointRow)).map(Posts.Parse).cache()
//    postsRdd.collect().map(println)
    val postHistoryRdd : RDD[PostHistory] = sc.textFile(PostHistories.filePath).filter(s => s.startsWith(dataPointRow)).map(PostHistories.Parse).cache()
//    postHistoryRdd.collect().map(println)
    val postLinksRdd : RDD[PostLink] = sc.textFile(PostLinks.filePath).filter(s => s.startsWith(dataPointRow)).map(PostLinks.Parse).cache()
//    postLinksRdd.collect().map(println)
    val usersRdd : RDD[User] = sc.textFile(Users.filePath).filter(s => s.startsWith(dataPointRow)).map(Users.Parse).cache()
//    usersRdd.collect().map(println)
    val votesRdd : RDD[Vote] = sc.textFile(Votes.filePath).filter(s => s.startsWith(dataPointRow)).map(Votes.Parse).cache()
//    votesRdd.collect().map(println)

    // Get a DF of UserId, B1, B2, B3  where B's represent badges we care about
    val badgeCountsRdd: RDD[Row] = badgesRdd
      .filter(badge => Badges.importantBadges.contains(badge.Name))
      .map(badge => (badge.BadgeUserId, badge.Name))
      // Avoid GroupByKey
      .aggregateByKey(ListBuffer.empty[String])(Utility.AddToListBuffer[String],
        Utility.CombineBuffers[String])
        .mapValues(_.toList)
        .mapValues(values => Utility.MapListOfItemsToCounts[String](values, Badges.importantBadges))
      .map({case (id, badges) => Row(id :: badges:_*)})

    val badgesDF = spark.createDataFrame(badgeCountsRdd, Badges.badgesDFSchema)
//    badgesDF.show()

    val commentsDF = commentsRdd.filter(comments => comments.CommentUserId.isDefined).toDF

    // Get a DF of postId and sum of comment score counts on a post
    val commentScoresDF = commentsDF
      .groupBy(commentsDF("CommentPostId"))
        .agg(sum("Score")).toDF("CommentScorePostId", "SumCommentScore")
//    commentScoresDF.show()

    val commentPostUserDF = commentsDF
      .groupBy(commentsDF("CommentPostId"), commentsDF("CommentUserId"))
      .agg(count(commentsDF("*")).as("CommentsByAuthor"))
//      commentPostUserDF.show()

    // Create a DF of Questions
    val questionsDF = postsRdd
      .filter(post => post.PostTypeId == 1)
        .filter(post => post.ClosedDate.isEmpty) // Only unclosed questions
        .map(post => Questions.Extract(post)).toDF()
//    questionsDF.show()

    // Create a DF of Answers
    val answersDF = postsRdd
      .filter(post => post.PostTypeId == 2)
      .map(post => Answers.Extract(post)).toDF()
//    answersDF.show()

    val answerFeaturesDF = questionsDF
      .join(answersDF, questionsDF("QuestionId") === answersDF("ParentId"), "left_outer")
      .withColumn("TimeSinceCreation", answersDF("AnswerCreationDate") - questionsDF("QuestionCreationDate"))
      .drop("AnswerCreationDate").drop("QuestionCreationDate").drop("ParentId").drop("QuestionId")
//    answerFeaturesDF.show()  // Prints a little weird because the arabic goes from right to left

    val postHistoriesDF = postHistoryRdd
      .filter(post => post.HistoryUserId.isDefined).toDF

    val postHistoriesUserDF = postHistoriesDF
      .groupBy(postHistoriesDF("HistoryPostId"), postHistoriesDF("HistoryUserId"))
      .agg(count(postHistoriesDF("*")).as("PosterEditCount"))
      .orderBy(postHistoriesDF("HistoryPostId"), postHistoriesDF("HistoryUserId"))
//    postHistoriesUserDF.show()

    val postLinksDF = postLinksRdd
      .map(postLink => (postLink.LinkPostId, 1))
      .reduceByKey(_ + _)
      .toDF("LinkPostId", "LinksCount")
//    postLinksDF.show()

    val usersDF = usersRdd.toDF()
//    usersDF.show()

    val voteCountsRdd: RDD[Row] = votesRdd
      .map(vote => (vote.VotePostId, vote.VoteTypeId))
      .aggregateByKey(ListBuffer.empty[Int])(Utility.AddToListBuffer[Int],
      Utility.CombineBuffers[Int])
      .mapValues(_.toList)
      .mapValues(values => Utility.MapListOfItemsToCounts(values, Votes.importantVoteTypes.keys.toList))
      .map({case (id, votes) => Row(id :: votes:_*)})

    val votesDF = spark.createDataFrame(voteCountsRdd, Votes.votesDFSchema)
//    votesDF.show()

//      .join(badgesDF, "UserID")  /// Could do this using common field
    val userData = usersDF.join(badgesDF, usersDF("UserId") === badgesDF("BadgeUserId"), "left_outer")
      .drop("BadgeUserId").na.fill(0)
//    userData.show()

    val totalAnswer = answerFeaturesDF
      .join(commentPostUserDF, $"AnswerId" === commentPostUserDF("CommentPostId") && $"OwnerUserId" === commentPostUserDF("CommentUserId"), "left_outer")
      .join(commentScoresDF, $"AnswerId" === commentScoresDF("CommentScorePostId"), "left_outer")
      .join(postHistoriesUserDF, $"AnswerId" === postHistoriesUserDF("HistoryPostId") && $"OwnerUserId" === postHistoriesUserDF("HistoryUserId"), "left_outer")
      .join(postLinksDF, $"AnswerId" === postLinksDF("LinkPostId"), "left_outer")
      .join(votesDF, $"AnswerId" === votesDF("VotePostId"), "left_outer")
      .drop("VotePostId").drop("LinkPostId").drop("HistoryPostId")
      .drop("CommentPostId").drop("CreationDate").drop("CommentUserId")
        .drop("CommentScorePostId").drop("HistoryUserId").na.fill(0)

    val finalAnswerJoin = totalAnswer.join(userData, totalAnswer("OwnerUserId") === userData("UserId"), "left_outer").drop("OwnerUserId")
    finalAnswerJoin.show()

    sc.stop()
  }
}

////// Old work trying to get the spark-xml library to function properly
//    val test: String = sc.textFile(Users.FilePath).map(str => str.replaceAll(" />", "></row>")).reduce(_ + _)

//     val df: DataFrame =
//       sparkSession.read.format("xml")
//       .option("rootTag", "tags")
//       .option("excludeAttribute", "false")
//       .option("rowTag", "row")
//      .load(Users.FilePath)
//      .collect()

//    df = sqlContext.read
//      .format("com.databricks.spark.xml")
//      .option("rowTag", "row")
//      .load(Users.FilePath)

////// Old work on badgesDF

//    val badgesDF = badgesRdd.toDF()
//    val resultBadges = badgesDF
//      .select(badgesDF("UserId"), badgesDF("Name"))
//      .groupBy(badgesDF("UserId"))
//      .count()
//      .orderBy(badgesDF("UserId"))
//      .where(badgesDF("Name").isin(importantBadges:_*))

//    resultBadges.show()
