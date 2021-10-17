name := "builder"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
	"org.scala-lang" % "scala-library" % "2.10.4",
	"org.apache.spark" %% "spark-sql" % "2.2.1"
)	
