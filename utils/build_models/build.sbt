name := "regressor"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.7"
)

