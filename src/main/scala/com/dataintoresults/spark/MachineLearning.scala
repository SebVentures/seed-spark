package com.dataintoresults.spark

import org.apache.log4j.Logger

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.ml.Predictor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, StringIndexerModel, VectorAssembler, OneHotEncoderEstimator}
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor

object MachineLearning {
  final val logger = Logger.getLogger(this.getClass)
  
  final val suffixNum = "_num"
  final val suffixPredicted = "_predicted"
  final val suffixPredictedNum = suffixPredicted + suffixNum 
  final val featuresName = "features"
  
  def randomForest(df: DataFrame, target: String, features: Array[String],
      numTrees: Int = 10, minInstancesPerNode: Int = -1, maxDepth: Int = -1): Pipeline = {
    val targetNum = target + suffixNum    
    val targetPredictedNum = target + suffixPredicted + suffixNum

    var randomForest = new org.apache.spark.ml.classification.RandomForestClassifier()
      .setLabelCol(targetNum)
      .setFeaturesCol(featuresName)
      .setPredictionCol(targetPredictedNum)
      .setNumTrees(numTrees)
      .setSeed(1)
      
    randomForest = if(minInstancesPerNode > 0) randomForest.setMinInstancesPerNode(minInstancesPerNode)
      else randomForest
      
    randomForest = if(maxDepth > 0) randomForest.setMaxDepth(maxDepth)
      else randomForest
      
    createPredictionPipeline(df, target, features, randomForest)
  }
  
  
  def gradientBoostedTree(df: DataFrame, target: String, features: Array[String],
      maxIter: Int = 10): Pipeline = {
    val targetNum = target + suffixNum    
    val targetPredictedNum = target + suffixPredicted + suffixNum

    val gbt = new org.apache.spark.ml.classification.GBTClassifier()
      .setLabelCol(targetNum)
      .setFeaturesCol(featuresName)
      .setPredictionCol(targetPredictedNum)
      .setMaxIter(maxIter)
      
    createPredictionPipeline(df, target, features, gbt)
  }
  
  def decisionTreeRegressor(df: DataFrame, target: String, features: Array[String]): Pipeline = { 
    val targetPredicted = target + suffixPredicted   

    val treeRegressor = new DecisionTreeRegressor()
      .setLabelCol(target)
      .setFeaturesCol(featuresName)
      .setPredictionCol(targetPredicted)
      
    createPredictionPipeline(df, target, features, treeRegressor)
  }
  
  private def createPredictionPipeline[FT, L, M](df: DataFrame, target: String, 
      features: Array[String], pred: PipelineStage): Pipeline  = {
    val targetNum = target + suffixNum    
    val targetPredictedNum = target + suffixPredicted + suffixNum
    val targetPredicted = target + suffixPredicted   

    val schema = df.schema
    
    // Conversion de la colonne cible en Double
    val (targetCompressor, targetDecompressor)  =      
      schema(target).dataType match {
        case t: StringType => {
          logger.info(s"Convertion of target $target into double")
          val compressor = new StringIndexer()
            .setInputCol(target)
            .setOutputCol(targetNum)
          val compressorModel = compressor.fit(df)
          val decompressor = new IndexToString()
            .setInputCol(targetPredictedNum)
            .setOutputCol(targetPredicted)
            .setLabels(compressorModel.labels)
          (Seq(compressorModel), Seq(decompressor))
        }
        case t: DoubleType => {          
          (Seq(), Seq())
        }
        case t: IntegerType => {          
          throw new RuntimeException("Target attribute is of type Integer, it shoud be Double for regression or String for classification")
        }
      }
    
    
    val categorialFeatures = features
      .filter( { col => df.schema(col).dataType.isInstanceOf[StringType] } )
    val nonCategorialFeatures = features
      .filter( { col => ! df.schema(col).dataType.isInstanceOf[StringType] } )
      
    if(categorialFeatures.size > 0 )
      throw new RuntimeException("Can't use categorial features on Spark 2.2")
    
        
    val finalFeatures = nonCategorialFeatures
    
    // On combine les prédicteurs à prendre dans une nouvelle colonne fusionnées.
    logger.info("Features assembling")
    val assembler = new VectorAssembler()
     .setInputCols(finalFeatures)
     .setOutputCol(featuresName)
          
    val stages = 
        Seq(targetCompressor, Seq(assembler), Seq(pred), targetDecompressor).flatten.toArray

    val pipeline = new Pipeline().setStages(stages)

    pipeline
  }
  
  
  
  def evaluateAccuracy(df: DataFrame, target: String): Double = {
    evaluate(df, target, "accuracy")
  }
  
  def evaluateRecall(df: DataFrame, target: String): Double = {
    evaluate(df, target, "recall")
  }
  
  def evaluatePrecision(df: DataFrame, target: String): Double = {
    evaluate(df, target, "precision")
  }
  
  def evaluateRmse(df: DataFrame, target: String): Double = {
    evaluate(df, target, "rmse")
  }
  
  /*
   * Retourne un dataframe avec la probabilite de chaque classe en colonnes
   */
  def probabilitiesArray(df: DataFrame): DataFrame = {
    import org.apache.spark.ml.linalg.DenseVector
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrUdf = udf(toArr)
    
    val converted = df.select("probability").withColumn("probability", toArrUdf(col("probability")))
    
    val nClasses = converted.first().getList(0).size
    converted.select((Range(0, nClasses).map(idx => col("probability")(idx) as "idx"):_*))
  }
  
  
  def evaluate(df: DataFrame, target: String, metric: String): Double = {
         
    val evaluator = df.schema(target).dataType match {
        case t: StringType => new MulticlassClassificationEvaluator()
          .setLabelCol(target + suffixNum)
          .setPredictionCol(target + suffixPredicted + suffixNum)
          .setMetricName(metric)
        case t: DoubleType => new RegressionEvaluator()
          .setLabelCol(target)
          .setPredictionCol(target + suffixPredicted )
          .setMetricName("rmse")
    }
    
    //We compute the error using the evaluator
    val error = evaluator.evaluate(df)
        
    error
  }
  
}