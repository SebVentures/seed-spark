
ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / name             := "seed-spark"
ThisBuild / organization     := "com.dataintoresults"

lazy val seedSpark = (project in file("."))
  .settings(
    name := "seed-spark",
    libraryDependencies := Seq(
	"org.apache.spark" %% "spark-sql" % "2.4.0",
	"org.apache.spark" %% "spark-mllib" % "2.4.0",
	"org.apache.spark" %% "spark-hive-thriftserver" % "2.4.0",
	"org.apache.spark" %% "spark-hive" % "2.4.0",
	"org.apache.derby" % "derby" % "10.14.2.0",
	"com.typesafe" % "config" % "1.2.1",
	"org.apache.poi" % "poi-ooxml" % "4.0.1",
	"org.jfree" % "jfreechart" % "1.5.0"
	) 
  )
