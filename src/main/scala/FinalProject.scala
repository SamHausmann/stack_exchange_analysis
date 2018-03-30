package XMLParse

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object FinalProject {

  val conf: SparkConf = new SparkConf().setAppName("SimpleSpark").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
//  sc.hadoopConfiguration.set("fs.s3a.access.key", "")
//  sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  val dataPointRow : String = "  <row "

  def main(args: Array[String]) {

//    val test = sc.textFile("s3a://stack.exchange.analysis/testfile.xml")
//    test.collect().map(println)


    val badgesRdd: RDD[Badge] = sc.textFile(Badges.filePath).filter(s => s.startsWith(dataPointRow)).map(Badges.Parse)
//    badgesRdd.collect().map(println)
    val usersRdd : RDD[User] = sc.textFile(Users.filePath).filter(s => s.startsWith(dataPointRow)).map(Users.Parse)
//    usersRdd.collect().map(println)
    val postsRdd : RDD[Post] = sc.textFile(Posts.filePath).filter(s => s.startsWith(dataPointRow)).map(Posts.Parse)
//    postsRdd.collect().map(println)
    val commentsRdd : RDD[Comment] = sc.textFile(Comments.filePath).filter(s => s.startsWith(dataPointRow)).map(Comments.Parse)
//    commentsRdd.collect().map(println)
    val postHistoryRdd : RDD[PostHistory] = sc.textFile(PostHistories.filePath).filter(s => s.startsWith(dataPointRow)).map(PostHistories.Parse)
//    postHistoryRdd.collect().map(println)
    val postLinksRdd : RDD[PostLink] = sc.textFile(PostLinks.filePath).filter(s => s.startsWith(dataPointRow)).map(PostLinks.Parse)
//    postLinksRdd.collect().map(println)
    val votesRdd : RDD[Vote] = sc.textFile(Votes.filePath).filter(s => s.startsWith(dataPointRow)).map(Votes.Parse)
//    votesRdd.collect().map(println)

    val test: RDD[Row] = badgesRdd
      .filter(badge => Badges.importantBadges.contains(badge.Name))
      .map(badge => (badge.UserId, badge.Name))
      .aggregateByKey(ListBuffer.empty[String])(Utility.AddStringToListBuffer,
        Utility.CombineStringBuffers)
        .mapValues(_.toList)
        .mapValues(Badges.MapListOfBadgesToCounts)
        .map({case (id, badges) => Row(id :: badges:_*)})

    val testDF = spark.createDataFrame(test, Badges.badgesSchema)
    testDF.show()

//    val df = spark.createDataFrame(test, schema)
//    df.show()
    //    badgesRdd.toDS().map(badge => (badge.UserId, (badge.Name, 1))).reduceByKey((name1, count1), (name2, count2) => (name, count + sum))
//    val badgesDF = badgesRdd.toDF()
//    val resultBadges = badgesDF
//      .groupBy(badgesDF("UserId"))
//      .agg(sum(badgesDF("Name").as[String]).when(badgesDF("Name").as[String] === "Nice Question", 1).otherwise(0))
//      .orderBy(badgesDF("UserId"))
//      .where(badgesDF("Name").isin(importantBadges:_*))

//    def projectedColumns(columns: List[String]): List[Column] = {
//      columns.map(column => {
//        badgesDF.select(badgesDF("count"))
//          .where(badgesDF("Name") === column)
//          .as(column)
//      })
//    }

//    badgesDF.select(projectedColumns())
//  }

//    val badgesDF = badgesRdd.toDF()
//    val resultBadges = badgesDF
//      .select(badgesDF("UserId"), badgesDF("Name"))
//      .groupBy(badgesDF("UserId"))
//      .count()
//      .orderBy(badgesDF("UserId"))
//      .where(badgesDF("Name").isin(importantBadges:_*))

//    resultBadges.show()

//    val postsDF = postsRdd.toDF()
//    val resultsPosts = postsDF
//      .filter(postsDF("PostTypeId") === 1)
//      .select(postsDF("OwnerUserId"),
//      postsDF("AcceptedAnswerId"),
//      postsDF("CreationDate"),
//      postsDF("Score"),
//      postsDF("ViewCount"),
//      postsDF("Body"),
//      postsDF("OwnerUserId"),
//      postsDF("CommentCount"),
//      postsDF("FavoriteCount")).toDF()

//    val joined = resultsPosts.join(resultBadges, resultsPosts("OwnerUserId") === badgesDF("UserId"), "left_outer")
//    joined.printSchema()
//    joined.show()

    sc.stop()
  }
}

// Old work trying to get the spark-xml library to function properly
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
