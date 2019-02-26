package app


import com.dataintoresults.spark.SparkHelper._

object MyApp extends SparkEnabledApp("local") {
  
  val nbRows = 100
    
  val df = spark.range(nbRows)
    .withColumn("x1", lit(1))
    .withColumn("x2", lit(2))
    .withColumn("c1", expr("id*3"))  
    .withColumn("c2", expr("id*3.14")) 
    .withColumn("c3", expr("id*1.2")) 
    .withColumn("c4", expr("id*3.33"))
  
  df.toTable("toto")
    
  spark.select("toto").show
}
