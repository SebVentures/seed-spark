package com.dataintoresults.spark

import scala.util.Try

import java.io.{BufferedOutputStream, FileOutputStream, File, IOException, PrintWriter}

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

import org.apache.poi.xssf.usermodel._
import org.apache.poi.xssf.streaming._


import com.typesafe.config.Config
import com.dataintoresults.Config.config

import org.apache.spark.ml.{PipelineModel, Pipeline, Model}

object SparkHelper   {
      
  implicit final val logger = Logger.getLogger(this.getClass)
  
  private val sparkConfigMap = scala.collection.mutable.Map[SparkSession, Config]()
  
  
  def flag(filtre: String): Column = expr("case when "+filtre+" then 1 else 0 end")
  
  private def getSparkConfig(spark : SparkSession) = 
    sparkConfigMap.get(spark).getOrElse( { throw new RuntimeException("Instance de Spark non trouvée.") } )
  
  def createSpark(environnement: String = config.getString("cm.spark.defaultEnvironment")) = {
    System.setSecurityManager(null)
    val cfg = config.getConfig("cm.spark."+environnement)
      .withFallback(config.getConfig("cm.spark.default"))
    
    val master = cfg.getString("master")
    val dataDir = cfg.getString("dataPath")
    val hiveDir = cfg.getString("hivePath")
    val tmpDir = cfg.getString("tmpPath")
    val defaultParallelism = cfg.getInt("defaultParallelism")
    val appName = cfg.getString("appName")
    
    var builder = SparkSession.builder
      .master(master)      
      .config("spark.sql.warehouse.dir", dataDir)
      .config("spark.local.dir", tmpDir)
      .config("spark.default.parallelism", defaultParallelism)
      .appName(appName)
      
    if(cfg.getBoolean("enableHive")) {
      builder = builder
        .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveDir;create=true")
        .enableHiveSupport()
    }
    
    
    if(cfg.getBoolean("enableThrift")) {
      builder = builder.config("spark.ui.enabled", true)
        .config("hive.server2.thrift.port", cfg.getInt("thriftPort"))
        .config("spark.sql.hive.thriftServer.singleSession", true)
        .config("spark.sql.thriftServer.incrementalCollect", true)
    }

    val sparkSession = builder.getOrCreate()
      
    if(cfg.getBoolean("enableThrift")) {
      s"Enable Thrift Server on port ${cfg.getInt("thriftPort")}".info
      org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(sparkSession.sqlContext)
    }
    
    sparkConfigMap.put(sparkSession, cfg)
      
    registerFunctions(sparkSession)
    
    sparkSession
  }
  
  private def registerFunctions(spark : SparkSession) = {
    
    /*
     * Conversion d'une date type entier 20180102 en string '2018-01-02'
     */
    val int_to_date = (dtInt: Int) => {
      val year = dtInt / 10000
      val month = (dtInt / 100) % 100
      val day = dtInt %100
      s"$year-$month-$day" 
    }    
    spark.udf.register("int_to_date", int_to_date)
    
    
    /*
     * Conversion d'une date type entier 20180102 en string '2018-01-02'
     */
    val date_to_int = (dtStr: String) => {
      val parts = dtStr.split("-")
      val year = Integer.valueOf(parts(0))
      val month = Integer.valueOf(parts(1))
      val day = Integer.valueOf(parts(2))
      year*10000 + month*100 + day
    }    
    spark.udf.register("date_to_int", date_to_int)
    
    
	  // Fonction UDF pour accéder à un élement d'un vecteur   
    val vectorIdx = udf((v: org.apache.spark.ml.linalg.Vector, idx: Int) => v(idx))
    spark.udf.register("vector_idx", vectorIdx)
    
    
  }
  
  abstract class SparkEnabledApp(environnement: String = config.getString("cm.spark.defaultEnvironment")) extends App  {
    
    implicit val logger = Logger.getLogger(this.getClass)
    implicit val spark = createSpark(environnement)
    
    def col(colName: String) = org.apache.spark.sql.functions.col(colName)    
    def lit(a: Any) = org.apache.spark.sql.functions.lit(a)
    def expr(sql: String) = org.apache.spark.sql.functions.expr(sql)
    
  }
  
  implicit class DataFrameExtension(df : DataFrame) {
    def toArray[C1]() = df.collect().map {r => (r.getAs[C1](0))}
    def toArray[C1, C2]() = df.collect().map {r => (r.getAs[C1](0), r.getAs[C2](1))}
    def toArray[C1, C2, C3]() = df.collect().map {r => (r.getAs[C1](0), r.getAs[C2](1), r.getAs[C3](2))}
    def toArray[C1, C2, C3, C4]() = 
      df.collect().map {r => (r.getAs[C1](0), r.getAs[C2](1), r.getAs[C3](2), r.getAs[C4](3))}
    def toArray[C1, C2, C3, C4, C5]() = 
      df.collect().map {r => (r.getAs[C1](0), r.getAs[C2](1), r.getAs[C3](2), r.getAs[C4](3), r.getAs[C5](4))}
    
    def toSingle[C1]() = df.collect().head.getAs[C1](0)
       
    def applyTransformation[A](params: Seq[A], f: (DataFrame, A) => DataFrame): DataFrame = {
      params.foldLeft(df)(f)
    }
    
    
    // Transforme des nombre codé en français vers le type decimal
    def parseDecimalColumns(columns: String*): DataFrame = {
      val parser = (df: DataFrame, col : String) =>  
        df.withColumn(col, expr("cast(trim(replace(replace("+col+",' ',''), ',', '.')) as decimal(15,2))"))
        
      applyTransformation(columns, parser)
    }
        
    // Transforme des nombre codé en français vers le type decimal sur les colones du motif colPattern
    def parseDecimalColumnsPattern(colPattern: String): DataFrame = {
      parseDecimalColumns(
          df.columns.filter(c => c.matches(colPattern)):_*)
    }
    
    def saveAsTable(tableName: String, mode: SaveMode = SaveMode.Overwrite): DataFrame = toTable(tableName, mode)
    
    def toTable(tableName: String, mode: SaveMode = SaveMode.Overwrite): DataFrame = {
      // Si l'on lit un CSV (import), il n'y aura qu'une partition
      // Si ensuite on fait une opération de cette table, on n'aura 
      // possiblement toujours qu'une seule partition  
      // Du coup on force au besoin
      val defaultParallelism = df.sparkSession.defaultParallelism
      val df2 = 
        if(df.rdd.getNumPartitions < 16) {
          logger.info(s"Parallelism for table $tableName was ${df.rdd.getNumPartitions}, pushing to ${df.sparkSession.defaultParallelism}")
          df.repartition(df.sparkSession.defaultParallelism)
        }
        else {
          df
        }
      
      df2.write.format("parquet").mode(mode).saveAsTable(tableName)
      
      df2
    }
    
    /**
     * Retourne un DataFrame identique mais où le nom des colonnes est en minuscule)
     */
    def lowerCaseColumns(): DataFrame = {
      df.columns.foldLeft(df)((df, col) => {
        df.withColumnRenamed(col, col.toLowerCase())
      })
    }
    
    /**
     * Retourne un DataFrame identique mais où le nom des colonnes est en minuscule,
     * sans espace sur les côtés et avec des underscores en place des espaces 
     */
    def niceColumns(): DataFrame = {
      df.columns.foldLeft(df)((df, col) => {
        df.withColumnRenamed(col, col.trim().toLowerCase().replaceAll(" ", "_"))
      })
    }
        
    /**
     * Cree ou remplace une temps view et renvoie le dataframe
     */
    def toTempView(viewName: String): DataFrame = {
      df.createOrReplaceTempView(viewName)
      df
    }
    
    def toExcel(fileName: String): DataFrame = {
      
      if(df.count() >= 1000000) {
        "Too many line in the DataFrame to export to Excel".info()
        return df
      }
      
      val wb = new SXSSFWorkbook();
      val sheet = wb.createSheet("Export")
      val csHeader = wb.createCellStyle()
      val fHeader = wb.createFont()
      fHeader.setBold(true)
      csHeader.setFont(fHeader)
      
      // Export de l'en-tête
      val rowHeader = sheet.createRow(0)
      df.columns.zipWithIndex.foreach { case (colName, i) => 
             val cell = rowHeader.createCell(i)
             cell.setCellValue(colName)
             cell.setCellStyle(csHeader)
      }
      
      // Expor des lignes
      df.collect.zipWithIndex.foreach { case (row, i) =>
        if(i % 1000 == 0) s"Exporting to Excel line : ${i}".info
        val rowData = sheet.createRow(i+1)
        for(j <- 0 until row.length) {
          if(row.isNullAt(j))
            rowData.createCell(j).setCellValue("")
          else 
            row.get(j) match {
              case v : String => rowData.createCell(j).setCellValue(v)
              case v : Int => rowData.createCell(j).setCellValue(v)
              case v : Long => rowData.createCell(j).setCellValue(v)
              case v : Double => rowData.createCell(j).setCellValue(v)
              case v : java.math.BigDecimal => rowData.createCell(j).setCellValue(v.doubleValue())
              case v : TimestampType => rowData.createCell(j).setCellValue(v.formatted("yyyy-MM-dd HH:mm:ss"))
              case x : Any =>
                s"Unrecognized type to convert to Excel file : ${x.getClass().getName}".info
                rowData.createCell(j).setCellValue(x.toString)
            } 
        }
      }
      
      /*
       * Autosize ne marche pas avec SXSSF
       */
      /*for(i <- 0 to df.columns.length) {  
        sheet.autoSizeColumn(i)
      } 
        
      s"Exporting to Excel Autosize finish".info
      */
      
      val fileOut = new BufferedOutputStream(new FileOutputStream(df.sparkSession.exportPath + fileName + ".xlsx"));
      wb.write(fileOut);
      fileOut.close();

      // remove some temp files
      wb.dispose()
      // Closing the workbook
      wb.close();
      
      df
    }
    
    
    /**
     * Converti une colonne dans un type donné (int, string, ...)
     */
    def castColumn(colName: String, dtype: String): DataFrame = {
      df.withColumn(colName, col(colName).cast(dtype))
    }
    
    /**
     * Converti des colonnes dans un type donné (int, string, ...)
     */
    def castColumns(colsName: Array[String], dtype: String): DataFrame = {
      colsName.foldLeft(df)( { case (df, colName) => df.castColumn(colName, dtype) } )
    }
        
    /**
     * Supprime les colonnes qui rentrent dans le motif indiqué
     */
    def dropPatten(pattern: String): DataFrame = {
      val p = pattern.r
      df.columns
        .filter(p.findFirstIn(_).isDefined)
        .foldLeft(df)( (df, col) => df.drop(col))
    }
    
    def appendToTable(tableName: String): DataFrame = {
      saveAsTable(tableName, SaveMode.Append)
    }
    
    def toCsv(file: String, delimiter: String = "\t", header: Boolean = true) = {
      toCsvPath(df.sparkSession.exportPath + "/" + file + ".csv", delimiter, header)
      df
    }
    
    def toCsvPath(path: String, delimiter: String = "\t", header: Boolean = true) = {
      val tmpDirectory = path + "_tmp"
      
      // S'il y a un header à sortir, il faut une seule partition, 
      // sinon on va avoir plusieurs fois les en-têtes dans le fichier final 
      val df2 = if(header) df.coalesce(1) else df     
      
      // Ecriture HDFS (plusieurs fichiers dans un répertoire)
      df2.write
        .option("delimiter", delimiter) 
        .option("header", header)
        .mode("overwrite")
        .csv(tmpDirectory)
        
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      // On supprime cible le fichier s'il existe déjà
      hdfs.delete(new Path(path), true)
      
      // fusion des fichiers du répertoire HDFS en un seul fichier
      // le paramêtre true indique de supprimer le répertoire temporaire à la fin
      copyMerge(hdfs, new Path(tmpDirectory), hdfs, new Path(path), false, hadoopConfig)
      
      df
    }
    
    def setNullableStateOfColumn(cn: String, nullable: Boolean) : DataFrame = {
      // get schema
      val schema = df.schema
      // modify [[StructField] with name `cn`
      val newSchema = StructType(schema.map {
        case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
        case y: StructField => y
      })
      // apply new schema
      df.sqlContext.createDataFrame( df.rdd, newSchema )
    }
    
    def trainRandomForest(target: String, features: Array[String], 
        numTrees: Int = 10, minInstancesPerNode: Int = -1, maxDepth: Int = -1): PipelineModel = {
      val pipeline = MachineLearning.randomForest(df, target, features, numTrees, minInstancesPerNode, maxDepth)

      //We fit our DataFrame into the pipeline to generate a model
      val model = pipeline.fit(df.coalesce(4))
      
      model
    }
       
    /*
     * Si l'on ne donne pas les features, c'est toutes les autres colonnes
     */
    def trainRandomForest(target: String): PipelineModel = {
      val features = df.columns.filter(_ != target)
      
      trainRandomForest(target, features)
    }
    
    def trainGradientBoostedTree(target: String, features: Array[String]): PipelineModel = {
      val pipeline = MachineLearning.gradientBoostedTree(df, target, features)

      //We fit our DataFrame into the pipeline to generate a model
      val model = pipeline.fit(df)
      
      model
    }
    
    def trainGradientBoostedTree(target: String): PipelineModel = {
      val features = df.columns.filter(_ != target)
      
      trainGradientBoostedTree(target, features)
    }
    
    
    def trainDecisionTreeRegressor(target: String, features: Array[String]): PipelineModel = {
      val pipeline = MachineLearning.decisionTreeRegressor(df, target, features)

      //We fit our DataFrame into the pipeline to generate a model
      val model = pipeline.fit(df)
      
      model
    }
    
    def trainDecisionTreeRegressor(target: String): PipelineModel = {
      val features = df.columns.filter(_ != target)
      
      trainDecisionTreeRegressor(target, features)
    }
    
    
    def evaluateAccuracy(target: String) : Double = {
      MachineLearning.evaluateAccuracy(df, target)
    }
    
    def evaluatePrecision(target: String) : Double = {
      MachineLearning.evaluatePrecision(df, target)
    }
    
    def evaluateRecall(target: String) : Double = {
      MachineLearning.evaluateRecall(df, target)
    }
        
    def evaluateRmse(target: String) : Double = {
      MachineLearning.evaluateRmse(df, target)
    }
        
    def probabilitiesArray(): DataFrame = {
      MachineLearning.probabilitiesArray(df)
    }
    
    /*
     * Spark permet de calculer des matrices de confusion via 
     * les RDD, mais ce n'est ni pratique ni simple.
     */
    def confusionMatrix(target: String) : DataFrame = {
      df.groupBy(target).pivot(target + MachineLearning.suffixPredicted).count()
    }
    
    def toXYChart(xColumn: String, yColumns: Seq[String], fileName: String): DataFrame = {
      JFreeChartHelper.createXYChartPNG(df, xColumn, yColumns, df.sparkSession.exportPath + fileName + ".png")
      df
    }
    
    private def copyMerge(
      srcFS: FileSystem, srcDir: Path,
      dstFS: FileSystem, dstFile: Path,
      deleteSource: Boolean, conf: Configuration): Boolean = {
    
      if (dstFS.exists(dstFile))
        throw new IOException(s"Target $dstFile already exists")
    
      // Source path is expected to be a directory:
      if (srcFS.getFileStatus(srcDir).isDirectory()) {
    
        val outputFile = dstFS.create(dstFile)
        Try {
          srcFS
            .listStatus(srcDir)
            .sortBy(_.getPath.getName)
            .collect {
              case status if status.isFile() =>
                val inputFile = srcFS.open(status.getPath())
                Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                inputFile.close()
            }
        }
        outputFile.close()
    
        if (deleteSource) srcFS.delete(srcDir, true) else true
      }
      else false
    }
  }
  
  implicit class StringExtension(str : String) {
    def toDF()(implicit spark: SparkSession): DataFrame = {
      spark.sql(str)
    }
    
    def println(): String = {
      System.out.println(str)
      str
    }
    
    def info()(implicit log: Logger): String = {
      log.info(str)
      str
    }
    
    def toFile(path: String): String = {
      val pw = new PrintWriter(new File(path))
      pw.write(str)
      pw.close
      str
    }
  }
  
  
      
  implicit class PipelineExtension(pipeline : Pipeline) {
    def save(file: String)(implicit spark: SparkSession): Pipeline = {
      pipeline.write.overwrite().save(spark.modelPath + file)
      pipeline 
    }
  }
      
      
  implicit class SparkSesionExtension(spark : SparkSession) {
    def exportPath = getSparkConfig(spark).getString("exportPath") + "/" 
    def importPath = getSparkConfig(spark).getString("importPath") + "/" 
    def dataPath = getSparkConfig(spark).getString("dataPath") + "/" 
    def modelPath = getSparkConfig(spark).getString("modelPath") + "/" 
    def defaultParallelism = getSparkConfig(spark).getInt("defaultParallelism")
    
    def repairDatabase(): SparkSession = addLocalDatabase(dataPath)
    
    def loadModel(file: String): Pipeline = {
      Pipeline.read.load(modelPath + file)
    }
    
    
    def select(table: String): DataFrame = {
      spark.sqlContext.table(table)
    }
    
    def addLocalDatabase(path: String): SparkSession = {      
      val directory = new File(path)
      
      if(!directory.exists)
        throw new RuntimeException(s"$path n'existe pas")
      
      if(!directory.isDirectory)
        throw new RuntimeException(s"$path doit être un répertoire")
      
      
      directory.listFiles.filter(_.isDirectory) map { table => 
        try {  
          
          logger.info(s"Adding table ${table.getName} from ${table.getAbsolutePath}")
          
          val name = table.getName
          val tempName = table.getName  +"_tmp"
          val tablePath = "file:///" + table.getAbsolutePath.replaceAll("\\\\", "/")
          
          spark.read.parquet(tablePath).createOrReplaceTempView(tempName)
          import spark.implicits._
          
          val df = spark.sql(s"describe $tempName")
          // "colname (space) data-type"
          val columns = df.map(row => row(0) + " " + row(1)).collect()
           
          spark.sql(s"DROP TABLE IF EXISTS $name")
          
          // Print the Hive create table statement:
          val hiveQuery = s"""CREATE EXTERNAL TABLE $name
              (${columns.mkString(", ")})
            STORED AS PARQUET 
            LOCATION '$tablePath'"""
            
          logger.trace(hiveQuery)
          
          spark.sql(hiveQuery)
        }
        catch {
          case e: org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException => 
              logger.warn(s"${table.getName} existe déjà")
          case e: org.apache.spark.sql.AnalysisException => 
              logger.warn(s"${table.getAbsolutePath} n'est pas une table parquet")
        }
      }
     
      
      spark
    }
    
    def fromCsv(fileName: String, delimiter: String =  "\t", inferSchema: Boolean = true, 
        header: Boolean = true, decimal: String = ",", nice: Boolean = true): DataFrame = {
       fromCsvPath(spark.importPath + fileName, delimiter, inferSchema, header, decimal, nice)
    }
    
    def fromCsvPath(filePath: String, delimiter: String =  "\t", inferSchema: Boolean = true, 
        header: Boolean = true, decimal: String = ",", nice: Boolean = true): DataFrame = {
       val df = spark.read
        .option("delimiter", delimiter)
        .option("dec", decimal)
        .option("inferSchema", inferSchema)
        .option("header", header) 
        .csv(filePath)
       if(nice)
         df.niceColumns()
       else
         df
    }
  }
}


  