scalaVersion := "2.11.8"

name:= "FinalProject"

// Deprecation errors were being thrown with explicit constructing of a SQLContext
//scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)