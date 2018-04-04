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

  def getBadgesDF(answerUserIDs: List[Int]): DataFrame = {
    val badgesRdd: RDD[Badge] = sc.textFile(Badges.filePath)
      .filter(s => s.startsWith(dataPointRow)).map(Badges.Parse)
      .filter(badge => answerUserIDs.contains(badge.BadgeUserId))

    val badgeCountsRdd: RDD[Row] = badgesRdd
      .filter(badge => Badges.importantBadges.contains(badge.Name))
      .map(badge => (badge.BadgeUserId, badge.Name))

      // Avoid GroupByKey
      .aggregateByKey(ListBuffer.empty[String])(Utility.AddToListBuffer[String],
      Utility.CombineBuffers[String])
      .mapValues(_.toList)
      .mapValues(values => Utility.MapListOfItemsToCounts[String](values, Badges.importantBadges))
      .map({case (id, badges) => Row(id :: badges:_*)})

    spark.createDataFrame(badgeCountsRdd, Badges.badgesDFSchema)
  }

  def getCommentsDf(answerUserIDs: List[Int]): DataFrame = {
    sc.textFile(Comments.filePath)
      .filter(s => s.startsWith(dataPointRow))
      .map(Comments.Parse)
      .filter(comments => comments.CommentUserId.isDefined)
      .filter(comment => answerUserIDs.contains(comment.CommentUserId.get))
      .toDF
  }

  // Get a DF of postId and sum of comment score counts on a post
  def getCommentScoresDF(commentsDF: DataFrame): DataFrame = {
    commentsDF
      .groupBy(commentsDF("CommentPostId"))
      .agg(sum("Score")).toDF("CommentScorePostId", "SumCommentScore")
  }

  def getPostsRDD(): RDD[Post] = {
    sc.textFile(Posts.filePath).filter(s => s.startsWith(dataPointRow))
      .map(Posts.Parse)
      .filter(post => post.OwnerUserId.isDefined)
  }

  def getAnswersDF(postsRdd: RDD[Post]): DataFrame = {
    postsRdd
      .filter(post => post.PostTypeId == 2)
      .map(post => Answers.Extract(post)).toDF()
  }

  def getQuestionsDF(postsRDD: RDD[Post], answerUserIDs: List[Int]): DataFrame = {
    postsRDD
      .filter(post => answerUserIDs.contains(post.OwnerUserId.get))
      .filter(post => post.PostTypeId == 1)
      .filter(post => post.ClosedDate.isEmpty) // Only unclosed questions
      .map(post => Questions.Extract(post))
      .toDF()
  }

  def getAnswerFeaturesDF(answersDF: DataFrame, questionsDF: DataFrame): DataFrame = {
    questionsDF
      .join(answersDF, questionsDF("QuestionId") === answersDF("ParentId"), "left_outer")
      .withColumn("TimeSinceCreation", answersDF("AnswerCreationDate") - questionsDF("QuestionCreationDate"))
      .drop("AnswerCreationDate").drop("QuestionCreationDate")
      .drop("ParentId").drop("QuestionId")
  }

  def getPostLinksDF(answerPostIDs: List[Int]): DataFrame = {
    sc.textFile(PostLinks.filePath)
      .filter(s => s.startsWith(dataPointRow))
      .map(PostLinks.Parse)
      .filter(postLink => answerPostIDs.contains(postLink.LinkPostId))
      .map(postLink => (postLink.LinkPostId, 1))
      .reduceByKey(_ + _)
      .toDF("LinkPostId", "LinksCount")
  }

  def getUsersDF(answerUserIDs: List[Int]): DataFrame = {
    sc.textFile(Users.filePath)
      .filter(s => s.startsWith(dataPointRow))
      .map(Users.Parse)
      .filter(user => answerUserIDs.contains(user.UserId))
      .toDF()
  }

  def getVotesDF(answerPostIDs: List[Int]): DataFrame = {
    val votesRdd: RDD[Vote] = sc.textFile(Votes.filePath)
      .filter(s => s.startsWith(dataPointRow))
      .map(Votes.Parse)
      .filter(vote => answerPostIDs.contains(vote.VotePostId))

    val voteCountsRdd: RDD[Row] = votesRdd
      .map(vote => (vote.VotePostId, vote.VoteTypeId))
      .aggregateByKey(ListBuffer.empty[Int])(Utility.AddToListBuffer[Int],
        Utility.CombineBuffers[Int])
      .mapValues(_.toList)
      .mapValues(values => Utility.MapListOfItemsToCounts(values, Votes.importantVoteTypes.keys.toList))
      .map({case (id, votes) => Row(id :: votes:_*)})

    spark.createDataFrame(voteCountsRdd, Votes.votesDFSchema)
  }

  def main(args: Array[String]) {

//    val test = sc.textFile("s3a://stack.exchange.analysis/testfile.xml")
//    test.collect().map(println)

    val postsRDD: RDD[Post] = getPostsRDD()
    val numPartitions: Int = postsRDD.getNumPartitions

    val answersDF: DataFrame = getAnswersDF(postsRDD).repartition(numPartitions, $"ParentId")

    val answerUserIDs: List[Int] = answersDF.select("OwnerUserId").collect().map(_(0).asInstanceOf[Int]).toList
    val answerPostIDs: List[Int] = answersDF.select("AnswerId").collect().map(_(0).asInstanceOf[Int]).toList

    val questionsDF: DataFrame = getQuestionsDF(postsRDD, answerUserIDs).repartition(numPartitions, $"QuestionId")
    val answerFeaturesDF: DataFrame = getAnswerFeaturesDF(answersDF, questionsDF).repartition(numPartitions, $"AnswerId")

    val badgesDF: DataFrame = getBadgesDF(answerUserIDs).repartition(numPartitions, $"BadgeUserId")

    val usersDF = getUsersDF(answerUserIDs).repartition(numPartitions, $"UserId")

    val commentsDF: DataFrame = getCommentsDf(answerUserIDs).repartition(numPartitions, $"CommentPostId")
    val commentScoresDF: DataFrame = getCommentScoresDF(commentsDF).repartition(numPartitions, $"CommentScorePostId")

    val postLinksDF: DataFrame = getPostLinksDF(answerPostIDs).repartition(numPartitions, $"LinkPostId")

    val votesDF = getVotesDF(answerPostIDs).repartition(numPartitions, $"VotePostId")

    val userData: DataFrame = usersDF.join(badgesDF, usersDF("UserId") === badgesDF("BadgeUserId"), "left_outer")
      .drop("BadgeUserId")
      .na.fill(0)
      .repartition(numPartitions, $"UserId")

    val postData: DataFrame = answerFeaturesDF
      .join(commentScoresDF, $"AnswerId" === commentScoresDF("CommentScorePostId"), "left_outer")
        .repartition(numPartitions, $"AnswerId")
      .join(postLinksDF, $"AnswerId" === postLinksDF("LinkPostId"), "left_outer")
      .repartition(numPartitions, $"AnswerId")
      .join(votesDF, $"AnswerId" === votesDF("VotePostId"), "left_outer")
      .drop("VotePostId").drop("LinkPostId")
      .drop("CreationDate").drop("CommentScorePostId")
      .drop("HistoryUserId").drop("HistoryPostId")
      .drop("CommentPostId").drop("CommentUserId")
      .na.fill(0).repartition(numPartitions, $"OwnerUserId")

    val finalAnswerJoin = postData.join(userData, postData("OwnerUserId") === userData("UserId"), "left_outer")
      .drop("OwnerUserId")
    finalAnswerJoin.show()

    sc.stop()
  }
}
