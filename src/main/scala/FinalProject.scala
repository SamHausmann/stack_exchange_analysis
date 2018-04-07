package XMLParse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object FinalProject {

  val dataPointRow : String = "  <row "

  def main(args: Array[String]) {

  val conf: SparkConf = new SparkConf().setAppName("FinalProject")
  val sc: SparkContext = new SparkContext(conf)
//  sc.hadoopConfiguration.set("fs.s3a.access.key", "")
//  sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  if (args.length < 2) {
    println("CLI arg 0: <Exchange>, CLI arg 1: <S3_Bucket>")
  }
  val exchange: String = args(0)
  val bucket: String = args(1)

  def getBadgesDF(exchange: String, bucket: String): DataFrame = {
    val badgesRdd: RDD[Badge] = sc.textFile(Badges.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(Badges.Parse)
      .filter(badge => badge.IsValid)

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

  def getCommentsDf(exchange: String, bucket: String): DataFrame = {
    sc.textFile(Comments.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(Comments.Parse)
      .filter(comment => comment.IsValid)
      .filter(comments => comments.CommentUserId.isDefined)
      .toDF.drop("IsValid")
  }

  // Get a DF of postId and sum of comment score counts on a post
  def getCommentScoresDF(commentsDF: DataFrame): DataFrame = {
    commentsDF
      .groupBy(commentsDF("CommentPostId"))
      .agg(sum("Score")).toDF("CommentScorePostId", "SumCommentScore")
  }

  def getPostsRDD(exchange: String, bucket: String): RDD[Post] = {
    sc.textFile(Posts.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(Posts.Parse)
      .filter(post => post.IsValid)
      .filter(post => post.OwnerUserId.isDefined)
  }

  def getAnswersDF(postsRdd: RDD[Post]): DataFrame = {
    postsRdd
      .filter(post => post.PostTypeId == 2)
      .map(post => Answers.Extract(post)).toDF().drop("IsValid")
  }

  def getQuestionsDF(postsRDD: RDD[Post]): DataFrame = {
    postsRDD
      .filter(post => post.PostTypeId == 1)
      .filter(post => post.ClosedDate.isEmpty) // Only unclosed questions
      .map(post => Questions.Extract(post))
      .toDF().drop("IsValid")
  }

  def getAnswerFeaturesDF(answersDF: DataFrame, questionsDF: DataFrame): DataFrame = {
    answersDF
      .join(questionsDF, answersDF("ParentId") === questionsDF("QuestionId"), "left_outer")
      .withColumn("TimeSinceCreation", answersDF("AnswerCreationDate") - questionsDF("QuestionCreationDate"))
      .drop("AnswerCreationDate").drop("QuestionCreationDate")
      .drop("ParentId").drop("QuestionId")
  }

  def getPostLinksDF(exchange: String, bucket: String): DataFrame = {
    sc.textFile(PostLinks.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(PostLinks.Parse)
      .filter(Link => Link.IsValid)
      .map(postLink => (postLink.LinkPostId, 1))
      .reduceByKey(_ + _)
      .toDF("LinkPostId", "LinksCount")
  }

  def getUsersDF(exchange: String, bucket: String): DataFrame = {
    sc.textFile(Users.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(Users.Parse)
      .filter(user => user.IsValid)
      .toDF()
      .drop("IsValid")
  }

  def getVotesDF(exchange: String, bucket: String): DataFrame = {
    val votesRdd: RDD[Vote] = sc.textFile(Votes.filePath(exchange, bucket))
      .filter(s => s.startsWith(dataPointRow))
      .map(Votes.Parse)
      .filter(vote => vote.IsValid)

    val voteCountsRdd: RDD[Row] = votesRdd
      .map(vote => (vote.VotePostId, vote.VoteTypeId))
      .aggregateByKey(ListBuffer.empty[Int])(Utility.AddToListBuffer[Int],
        Utility.CombineBuffers[Int])
      .mapValues(_.toList)
      .mapValues(values => Utility.MapListOfItemsToCounts(values, Votes.importantVoteTypes.keys.toList))
      .map({case (id, votes) => Row(id :: votes:_*)})

    spark.createDataFrame(voteCountsRdd, Votes.votesDFSchema)
  }

//    val test = sc.textFile("s3a://stack.exchange.analysis/testfile.xml")
//    test.collect().map(println)
//    sc..setConf("spark.sql.shuffle.partitions", "300")

    val postsRDD: RDD[Post] = getPostsRDD(exchange, bucket)//.cache()
//    val numPartitions: Int = postsRDD.getNumPartitions

//    spark.sqlContext.setConf("spark.sql.shuffle.partitions", numPartitions.toString)

    val answersDF: DataFrame = getAnswersDF(postsRDD).repartition($"ParentId")

    //val answerUserIDs: List[Int] = answersDF.select("OwnerUserId").collect().map(_(0).asInstanceOf[Int]).toList
    //val answerPostIDs: List[Int] = answersDF.select("AnswerId").collect().map(_(0).asInstanceOf[Int]).toList

    val questionsDF: DataFrame = getQuestionsDF(postsRDD).repartition($"QuestionId")

//    postsRDD.unpersist()

    val answerFeaturesDF: DataFrame = getAnswerFeaturesDF(answersDF, questionsDF).repartition($"AnswerId")

    val badgesDF: DataFrame = getBadgesDF(exchange, bucket).repartition($"BadgeUserId")

    val usersDF = getUsersDF(exchange, bucket).repartition($"UserId")

    val commentsDF: DataFrame = getCommentsDf(exchange, bucket).repartition($"CommentPostId")
    val commentScoresDF: DataFrame = getCommentScoresDF(commentsDF).repartition($"CommentScorePostId")

    val postLinksDF: DataFrame = getPostLinksDF(exchange, bucket).repartition($"LinkPostId")

    val votesDF = getVotesDF(exchange, bucket).repartition($"VotePostId")

    val userData: DataFrame = usersDF.join(badgesDF, usersDF("UserId") === badgesDF("BadgeUserId"), "left_outer")
      .drop("BadgeUserId")
      .na.fill(0)
      .repartition($"UserId")

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

    val finalAnswerJoin = postData.join(userData, postData("OwnerUserId") === userData("UserId"), "left_outer")
      .drop("OwnerUserId")

    val test = finalAnswerJoin.show(5)
    finalAnswerJoin.repartition(1).write.format("csv").option("header", "true").save("s3a://hausmanbucket/" +  exchange + "2.csv")
//    finalAnswerJoin.repartition(1).write.format("csv").option("header", "true").save("s3a://stack.exchange.analysis/" +  exchange + ".csv")
//    finalAnswerJoin.coalesce(1).write.format("csv").option("header", "true").save("/home/ec2-user/test.csv")

    sc.stop()
  }
}

