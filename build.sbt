scalaVersion := "2.11.8"

name:= "FinalProject"

// Deprecation errors were being thrown with explicit constructing of a SQLContext
//scalacOptions := Seq("-unchecked", "-deprecation")

val sparkVersion = "2.2.0"

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0"
)
