package XMLParse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object FinalProject {

  def main(args: Array[String]) {

    // Each actual datapoint begins with a row tag
    val dataPointRow: String = "  <row "

    // Initialize spark inside of main method
    val conf: SparkConf = new SparkConf().setAppName("FinalProject").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // Allow for toDF() function, implicit types for dataframes
    import spark.implicits._

    // Create a dataframe from the badges file
    def getBadgesDF(answerUserIDs: List[Int]): DataFrame = {
      val badgesRdd: RDD[Badge] = sc.textFile(Badges.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(Badges.Parse)
        .filter(_.isDefined).map(_.get)
        .filter(badge => answerUserIDs.contains(badge.BadgeUserId))

      // Generate RDD (userID, badge-name)
      val badgeCountsRdd: RDD[Row] = badgesRdd
        .filter(badge => Badges.importantBadges.contains(badge.Name))
        .map(badge => (badge.BadgeUserId, badge.Name))

        // Avoid GroupByKey
        // Create a list of badges badges and then use to utility method to map to different columns
        .aggregateByKey(ListBuffer.empty[String])(Utility.AddToListBuffer[String],
        Utility.CombineBuffers[String])
        .mapValues(_.toList)
        .mapValues(values => Utility.MapListOfItemsToCounts[String](values, Badges.importantBadges))
        // Create an RDD of Rows
        .map({case (id, badges) => Row(id :: badges:_*)})

      // Create dataframe explicitly from schema
      spark.createDataFrame(badgeCountsRdd, Badges.badgesDFSchema)
    }

    // Get comments dataframe
    def getCommentsDf(answerPostIds: List[Int]): DataFrame = {
      sc.textFile(Comments.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(Comments.Parse)
        .filter(_.isDefined).map(_.get)
        .filter(comment => answerPostIds.contains(comment.CommentPostId))
        .toDF
    }

    // Get a DF of postId and sum of comment score counts on a post
    def getCommentScoresDF(commentsDF: DataFrame): DataFrame = {
      commentsDF
        .groupBy(commentsDF("CommentPostId"))
        .agg(sum("Score")).toDF("CommentScorePostId", "SumCommentScore")
    }

    // Get RDD of post objects
    def getPostsRDD(): RDD[Post] = {
      sc.textFile(Posts.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(Posts.Parse)
        .filter(_.isDefined).map(_.get)
    }

    // Get DF with answer data
    def getAnswersDF(postsRdd: RDD[Post]): DataFrame = {
      postsRdd
        .filter(post => post.PostTypeId == 2)
        .map(post => Answers.Extract(post)).toDF()
    }

    // Get DF with question data
    def getQuestionsDF(postsRDD: RDD[Post]): DataFrame = {
      postsRDD
        .filter(post => post.PostTypeId == 1)
        .filter(post => post.ClosedDate.isEmpty) // Only unclosed questions
        .map(post => Questions.Extract(post))
        .toDF()
    }

    // Combine answer and question DataFrames
    def getAnswerFeaturesDF(answersDF: DataFrame, questionsDF: DataFrame): DataFrame = {
      answersDF
        .join(questionsDF, answersDF("ParentId") === questionsDF("QuestionId"), "left_outer")
        .withColumn("TimeSinceCreation", answersDF("AnswerCreationDate") - questionsDF("QuestionCreationDate"))
        .drop("AnswerCreationDate").drop("QuestionCreationDate")
        .drop("ParentId").drop("QuestionId")
    }

    // Get a count for postlinks in each post
    def getPostLinksDF(answerPostIds: List[Int]): DataFrame = {
      sc.textFile(PostLinks.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(PostLinks.Parse)
        .filter(_.isDefined).map(_.get)
        .filter(link => answerPostIds.contains(link.LinkPostId))
        .map(postLink => (postLink.LinkPostId, 1))
        .reduceByKey(_ + _)
        .toDF("LinkPostId", "LinksCount")
    }

    // Get users dataframe
    def getUsersDF(answerUserIds: List[Int]): DataFrame = {
      sc.textFile(Users.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(Users.Parse)
        .filter(_.isDefined).map(_.get)
        .filter(user => answerUserIds.contains(user.UserId))
        .toDF()
    }

    // Get votes DataFrame
    def getVotesDF(answerPostIds: List[Int]): DataFrame = {
      val votesRdd: RDD[Vote] = sc.textFile(Votes.filePath)
        .filter(s => s.startsWith(dataPointRow))
        .map(Votes.Parse)
        .filter(_.isDefined).map(_.get)
        .filter(vote => answerPostIds.contains(vote.VotePostId))

      // Create column for each important type of vote
      val voteCountsRdd: RDD[Row] = votesRdd
        .map(vote => (vote.VotePostId, vote.VoteTypeId))
        .aggregateByKey(ListBuffer.empty[Int])(Utility.AddToListBuffer[Int],
          Utility.CombineBuffers[Int])
        .mapValues(_.toList)
        .mapValues(values => Utility.MapListOfItemsToCounts(values, Votes.importantVoteTypes.keys.toList))
        .map({case (id, votes) => Row(id :: votes:_*)})

      spark.createDataFrame(voteCountsRdd, Votes.votesDFSchema)
    }

    // Read in posts RDD and cache it, filter other RDD's on features of the PostsRDD
    val postsRDD: RDD[Post] = getPostsRDD().cache()
    val numPartitions: Int = postsRDD.getNumPartitions
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", numPartitions.toString)

    val answersDF: DataFrame = getAnswersDF(postsRDD).repartition($"ParentId")
    val answerUserIDs: List[Int] = answersDF.select("OwnerUserId").collect().map(_(0).asInstanceOf[Int]).toList
    val answerPostIDs: List[Int] = answersDF.select("AnswerId").collect().map(_(0).asInstanceOf[Int]).toList
    val questionsDF: DataFrame = getQuestionsDF(postsRDD).repartition($"QuestionId")

    postsRDD.unpersist()

    val answerFeaturesDF: DataFrame = getAnswerFeaturesDF(answersDF, questionsDF).repartition($"AnswerId")

    val badgesDF: DataFrame = getBadgesDF(answerUserIDs).repartition($"BadgeUserId")

    val usersDF = getUsersDF(answerUserIDs).repartition($"UserId")

    val commentsDF: DataFrame = getCommentsDf(answerPostIDs).repartition($"CommentPostId")
    val commentScoresDF: DataFrame = getCommentScoresDF(commentsDF).repartition($"CommentScorePostId")

    val postLinksDF: DataFrame = getPostLinksDF(answerPostIDs).repartition($"LinkPostId")

    val votesDF = getVotesDF(answerPostIDs).repartition($"VotePostId")

    // Combine user data (users and badges)
    val userData: DataFrame = usersDF.join(badgesDF, usersDF("UserId") === badgesDF("BadgeUserId"), "left_outer")
      .drop("BadgeUserId")
      .na.fill(0)
      .repartition($"UserId")

    // Combine post data (answer, commentScores, postLinks, votes)
    val postData: DataFrame = answerFeaturesDF
      .join(commentScoresDF, $"AnswerId" === commentScoresDF("CommentScorePostId"), "left_outer")
        .repartition($"AnswerId")
      .join(postLinksDF, $"AnswerId" === postLinksDF("LinkPostId"), "left_outer")
      .repartition($"AnswerId")
      .join(votesDF, $"AnswerId" === votesDF("VotePostId"), "left_outer")
      .drop("VotePostId").drop("LinkPostId")
      .drop("CreationDate").drop("CommentScorePostId")
      .drop("HistoryUserId").drop("HistoryPostId")
      .drop("CommentPostId").drop("CommentUserId")
      .na.fill(0).repartition($"OwnerUserId")

    // Combine to final data warehouse
    val finalAnswerJoin = postData.join(userData, postData("OwnerUserId") === userData("UserId"), "left_outer")
      .drop("OwnerUserId")

    // Write to file
    finalAnswerJoin.repartition(1).write.format("csv").option("header", "true").save("cleanData.csv")

    finalAnswerJoin.show()

    sc.stop()
  }
}