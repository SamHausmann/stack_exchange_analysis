scalaVersion := "2.11.8"

name:= "FinalProject"

// Deprecation errors were being thrown with explicit constructing of a SQLContext
//scalacOptions := Seq("-unchecked", "-deprecation")
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" %   "1.7.4"
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll(
//  ExclusionRule("com.amazonaws", "aws-java-sdk"),
//  ExclusionRule("commons-beanutils")
//)

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0"
)