package LogisticReg

import java.io.{File, PrintWriter}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FinalProjectPhase3 {

  val conf: SparkConf = new SparkConf().setAppName("SimpleSpark").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
  sc.hadoopConfiguration.set("fs.s3a.access.key", "")
  sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  def main(args: Array[String]) {

    val reducedData: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("s3a://stack.exchange.analysis/cleanData.csv.csv")

    val featureCols =
      Array("Score", "UpVotes", "BodyLength", "CommentCount", "FavoriteCount")
//      Array("Score", "ViewCount", "BodyLength", "CommentCount", "FavoriteCount", "TimeSinceCreation",
//      "SumCommentScore", "LinksCount", "Offensive", "Favorite", "Reputation", "UserCreationDate",
//      "Age", "AboutMeLength", "Views", "UpVotes", "DownVotes", "Suffrage", "Electorate", "Civic Duty",
//      "Explainer", "Refiner", "Nice Question")

//    val labelIndexer = new StringIndexer().setInputCol("AcceptedByOriginator").setOutputCol("label")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val splitSeed = 5043
    val Array(trainingData: DataFrame, testData: DataFrame) = reducedData.randomSplit(Array(0.7, 0.3), splitSeed)

    trainingData.cache()
    testData.cache()

    val lr = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("AcceptedByOriginator")

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))

    val model = pipeline.fit(trainingData)
    val result = model.transform(testData)

    val modelTrained = model.stages(2).asInstanceOf[LogisticRegressionModel]

    println(s"Coefficients: ${modelTrained.coefficients} Intercept: ${modelTrained.intercept}")

    result.show()

    val Row(corrCoef: Matrix) = Correlation.corr(result, "scaledFeatures").head
    println("Pearson correlation matrix:\n" + corrCoef.toString(featureCols.length, Int.MaxValue))

    val pw = new PrintWriter(new File("test.txt"))
    pw.write("Features: " + featureCols.mkString(",") + "\n")
    pw.write(corrCoef.toString(featureCols.length, Int.MaxValue) + "\n")
    pw.write("Coefficients: " + modelTrained.coefficients + "\n")
    pw.write("Intercept: " + modelTrained.intercept + "\n")
    pw.close()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("AcceptedByOriginator")
      .setRawPredictionCol("rawPrediction")
    val accuracy = evaluator.evaluate(result)

    val lp = result.select(result("AcceptedByOriginator").alias("label"), result("prediction"))
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()

    println("Accuracy" + accuracy)
    println("TrueN" + trueN)
    println("TrueP" + trueP)
    println("FalseN" + falseN)
    println("FalseP" + falseP)
    sc.stop()
  }
}
