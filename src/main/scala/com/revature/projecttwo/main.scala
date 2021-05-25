package com.revature.projecttwo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("projecttwo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")



    val genCovidDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("covid_19_data.csv")
    val timeSereiesDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_confirmed.csv")
    val timeSereiesUsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_confirmed_US.csv")
    val timeSeriesDeathsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths.csv")
    val timeSeriesDeathsUsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths_US.csv")
    val timeSeriesRecoveredDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_recovered.csv")
    
    genCovidDF.show(20)
    timeSereiesDF.show(20)
    timeSereiesUsDF.show(20)
    timeSeriesDeathsDF.show(20)
    timeSeriesDeathsUsDF.show(20)
    timeSeriesRecoveredDF.show(20)
  }
}