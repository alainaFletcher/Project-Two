package com.revature.projecttwo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
// added _ to be able to use col on data frames and create aliases
import org.apache.spark.sql.functions._
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
    
    //genCovidDF.show(20)
    // timeSereiesDF.show(20)
    // timeSereiesUsDF.show(20)
    // timeSeriesDeathsDF.show(20)
    // timeSeriesDeathsUsDF.show(20)
    // timeSeriesRecoveredDF.show(20)
    deathConfRatio(spark)
    
  }
  // method to get death/confirmed case ration from 10 different countries
  def deathConfRatio(spark: SparkSession) = {
    import spark.implicits._
    // loading our tables once again
    val timeSeriesConfirmedDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_confirmed.csv")
    val timeSeriesDeathsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths.csv")
    // reducing time series to country/region and latest date as confirmed/deaths respectively
    val timeSeriesConfirmedDF1 = timeSeriesConfirmedDF.select(col("Country/Region"),col("5/2/21").as("confirmed"))
    val timeSeriesDeathsDF1 = timeSeriesDeathsDF.select(col("Country/Region"),col("5/2/21").as("deaths")) 
    // summing duplicate country/regions and removing the duplicates
    val timeConfReduced = timeSeriesConfirmedDF1.groupBy("Country/Region").agg(sum("confirmed")).orderBy("Country/Region")
    val timeDeathReduced = timeSeriesDeathsDF1.groupBy("Country/Region").agg(sum("deaths")).orderBy("Country/Region")
    // creating table with country, confirmed, deaths
    val timeConfDeath = timeDeathReduced.join(timeConfReduced, timeDeathReduced("Country/Region").as("dup") === timeConfReduced("Country/Region"))
       .select(timeDeathReduced("Country/Region"), col("sum(confirmed)").as("confirmed"),col("sum(deaths)").as("deaths")).orderBy(timeDeathReduced("Country/Region"))
    // adding our ratio of deaths/case
    val timeConfDeath1 = timeConfDeath.withColumn("Deaths/Confirmed", round(col("deaths")/col("confirmed"), 6))
    // showing top ten countries with highes population
    timeConfDeath1.filter($"Country/Region" === "China" || $"Country/Region" === "India" ||
       $"Country/Region" === "US" || $"Country/Region" === "Indonesia" ||
       $"Country/Region" === "Brazil" || $"Country/Region" === "Pakistan" ||
       $"Country/Region" === "Nigeria" || $"Country/Region" === "Bangladesh" ||
       $"Country/Region" === "Russia" || $"Country/Region" === "Mexico")
       .orderBy($"Deaths/Confirmed".desc)
       .show()
    
  }
}