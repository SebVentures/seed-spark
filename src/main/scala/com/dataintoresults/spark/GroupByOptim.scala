package com.dataintoresults.spark

import scala.util.Try

import java.io.{File, IOException, PrintWriter}

import org.apache.log4j.Logger

import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import com.typesafe.config.Config

import com.dataintoresults.Config.config
import com.dataintoresults.spark.SparkHelper._

import org.apache.spark.ml.{PipelineModel, Pipeline}

object GroupByOptim   {
  
  implicit final val logger = Logger.getLogger(this.getClass)
  
  def optimizeGroupBy(df: DataFrame, keys: String, aggregs: Array[String], batchSize: Int = 100): DataFrame = {
    // batch aggregs 100 by 100
    
    val spark = df.sparkSession
   
    val initialDfName = "optimizeGroupBy"
    df.createOrReplaceTempView("optimizeGroupBy")
    
    // Applique les group by sur chaque batch séparément
    val subDfNames = aggregs.grouped(batchSize).zipWithIndex.map { case (aggregBatch, i) => 
      val sql = s"select $keys, \n" +
        aggregBatch.mkString("\n", ",\n", "\n") +
        s"from $initialDfName\n" +
        s"group by $keys"
      //sql.info
      val batchName = s"${initialDfName}_$i"
      spark.sql(sql).createOrReplaceTempView(batchName)
      batchName
      
    }
    
    // Fusionne le tout avec des natural join
    val sql = s"select * \n" +
        subDfNames.mkString("from ", "\n natural join ", "")
    sql.info
    
    spark.sql(sql)
  }
}