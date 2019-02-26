package com.dataintoresults.spark


import org.apache.log4j.Logger


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.dataintoresults.spark.SparkHelper._


import org.jfree.chart.JFreeChart; 
import org.jfree.chart.ChartFactory; 
import org.jfree.data.xy._; 
import org.jfree.chart.ChartUtils; 
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.{Color, BasicStroke}

object JFreeChartHelper {
	implicit final val logger = Logger.getLogger(this.getClass);
  
	def createXYChartPNG(df: DataFrame, xColumn: String, yColumns: Seq[String],
	    filePath: String,
	    chartTitle : String = "",
	    xAxisLabel: String = "",
	    yAxisLabel: String = ""): Unit = {
	  val dataset = new XYSeriesCollection()
	  
	  yColumns map { yCol =>
	    val serie = new XYSeries(yCol)
	    df.select(col(xColumn).cast("double"), col(yCol).cast("double"))
	      .toArray[Double, Double]()
	      .foreach { case (x, y) => serie.add(x, y)} 
	    serie
	  } foreach { serie =>
	    dataset.addSeries(serie)
	  }
	  
	  val chart = ChartFactory.createXYLineChart(
      chartTitle, xColumn, yAxisLabel, dataset, PlotOrientation.VERTICAL,
      true, true, false)
      
    // Changement du fond du graphique
    chart.setBackgroundPaint(Color.WHITE);
	  
	  // Changement de la taille des lignes 
	  val plot = chart.getXYPlot()
	  plot.setBackgroundPaint(Color.WHITE)
	  plot.setRangeGridlinePaint(Color.GRAY)
	  plot.setDomainGridlinePaint(Color.LIGHT_GRAY)
	  plot.setOutlineVisible(false)
    // here we change the line size
    val seriesCount = plot.getSeriesCount();
    for (i <- 0 until seriesCount) {
      plot.getRenderer().setSeriesStroke(i, new BasicStroke(2));
    }
    
    val width = 1200
    val height = 800
    val file = new java.io.File(filePath)
	  ChartUtils.saveChartAsPNG(file, chart, width, height)
	}
}