package com.revature.projecttwo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameReader

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("projecttwo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("project two")
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
    
        
  deathConfRatio(spark)
  usDeathConfirmedRatio(spark)
  festiveDeaths(spark)
  monthlyDRratio(spark)
    
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
    val timeconfDeath1Table = timeConfDeath1.filter($"Country/Region" === "China" || $"Country/Region" === "India" ||
       $"Country/Region" === "US" || $"Country/Region" === "Indonesia" ||
       $"Country/Region" === "Brazil" || $"Country/Region" === "Pakistan" ||
       $"Country/Region" === "Nigeria" || $"Country/Region" === "Bangladesh" ||
       $"Country/Region" === "Russia" || $"Country/Region" === "Mexico")
       .orderBy($"Deaths/Confirmed".desc)
       
    timeconfDeath1Table.coalesce(1).write.csv("deathConfRatio.csv")
    timeconfDeath1Table.show()




  }

  def usDeathConfirmedRatio(spark: SparkSession) = {
    import spark.implicits._
     // loading our tables once again
    val timeSeriesConfirmedDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_confirmed_US.csv")
    val timeSeriesDeathsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths_US.csv")
    // reducing time series to country/region and latest date as confirmed/deaths respectively
    val timeSeriesConfirmedDF1 = timeSeriesConfirmedDF.select(col("Province_State"),col("5/2/21").as("confirmed"))
    val timeSeriesDeathsDF1 = timeSeriesDeathsDF.select(col("Province_State"),col("5/2/21").as("deaths"))
    // summing duplicate country/regions and removing the duplicates
    val timeConfReduced = timeSeriesConfirmedDF1.groupBy("Province_State").agg(sum("confirmed")).orderBy("Province_State")
    val timeDeathReduced = timeSeriesDeathsDF1.groupBy("Province_State").agg(sum("deaths")).orderBy("Province_State")
    // creating table with country, confirmed, deaths
    val timeConfDeath = timeDeathReduced.join(timeConfReduced, timeDeathReduced("Province_State").as("dup") === timeConfReduced("Province_State"))
      .select(timeDeathReduced("Province_State"), col("sum(confirmed)").as("confirmed"),col("sum(deaths)").as("deaths")).orderBy(timeDeathReduced("Province_State"))
    // adding our ratio of deaths/case
    val timeConfDeath1 = timeConfDeath.withColumn("Deaths/Confirmed", round(col("deaths")/col("confirmed"), 6))
    // showing top 5 states with highest amount of deaths
    //timeConfDeath1.orderBy($"Deaths/Confirmed".desc).show()
    timeConfDeath1.orderBy($"deaths".desc).show(5)

  }


  def festiveDeaths(spark: SparkSession) = {
    import spark.implicits._

    val timeSeriesDeathsUsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths_US.csv")
    timeSeriesDeathsUsDF.createOrReplaceTempView("festivedeaths")
    val fd = spark.sql("""Select Province_State, 
                  sum(cast(`11/28/20` as int)) - sum(cast(`11/27/20` as int)) as `nov 28`, 
                  sum(cast(`11/29/20` as int)) - sum(cast(`11/28/20` as int)) as `nov 29`, 
                  sum(cast(`11/30/20` as int)) - sum(cast(`11/29/20` as int)) as `nov 30`,
                  sum(cast(`12/1/20` as int)) - sum(cast(`11/30/20` as int)) as `dec 1`, 
                  sum(cast(`12/2/20` as int)) - sum(cast(`12/1/20` as int)) as `dec 2`,
                  sum(cast(`12/3/20` as int)) - sum(cast(`12/2/20` as int)) as `dec 3`, 
                  sum(cast(`12/4/20` as int)) - sum(cast(`12/3/20` as int)) as `dec 4`, 
                  sum(cast(`12/5/20` as int)) - sum(cast(`12/4/20` as int)) as `dec 5`,
                  sum(cast(`12/6/20` as int)) - sum(cast(`12/5/20` as int)) as `dec 6`, 
                  sum(cast(`12/7/20` as int)) - sum(cast(`12/6/20` as int)) as `dec 7`,
                  sum(cast(`12/8/20` as int)) - sum(cast(`12/7/20` as int)) as `dec 8`, 
                  sum(cast(`12/9/20` as int)) - sum(cast(`12/8/20` as int)) as `dec 9`, 
                  sum(cast(`12/10/20` as int)) - sum(cast(`12/9/20` as int)) as `dec 10`,
                  sum(cast(`12/11/20` as int)) - sum(cast(`12/10/20` as int)) as `dec 11`, 
                  sum(cast(`12/12/20` as int)) - sum(cast(`12/11/20` as int))  as `dec 12`,
                  sum(cast(`12/13/20` as int)) - sum(cast(`12/12/20` as int)) as `dec 13`, 
                  sum(cast(`12/14/20` as int)) - sum(cast(`12/13/20` as int)) as `dec 14`, 
                  sum(cast(`12/15/20` as int)) - sum(cast(`12/14/20` as int)) as `dec 15`,
                  sum(cast(`12/16/20` as int)) - sum(cast(`12/15/20` as int)) as `dec 16`, 
                  sum(cast(`12/17/20` as int)) - sum(cast(`12/16/20` as int)) as `dec 17`,
                  sum(cast(`12/18/20` as int)) - sum(cast(`12/17/20` as int)) as `dec 18`, 
                  sum(cast(`12/19/20` as int)) - sum(cast(`12/18/20` as int)) as `dec 19`, 
                  sum(cast(`12/20/20` as int)) - sum(cast(`12/19/20` as int)) as `dec 20`,
                  sum(cast(`12/21/20` as int)) - sum(cast(`12/20/20` as int)) as `dec 21`, 
                  sum(cast(`12/22/20` as int)) - sum(cast(`12/21/20` as int)) as `dec 22`,
                  sum(cast(`12/23/20` as int)) - sum(cast(`12/22/20` as int)) as `dec 23`,
                  sum(cast(`12/24/20` as int)) - sum(cast(`12/23/20` as int)) as `dec 24`, 
                  sum(cast(`12/25/20` as int)) - sum(cast(`12/24/20` as int)) as `dec 25`,
                  sum(cast(`12/26/20` as int)) - sum(cast(`12/25/20` as int)) as `dec 26`,
                  sum(cast(`12/27/20` as int)) - sum(cast(`12/26/20` as int)) as `dec 27`, 
                  sum(cast(`12/28/20` as int)) - sum(cast(`12/27/20` as int)) as `dec 28`,
                  sum(cast(`12/29/20` as int)) - sum(cast(`12/28/20` as int)) as `dec 29`,
                  sum(cast(`12/30/20` as int)) - sum(cast(`12/29/20` as int)) as `dec 30`, 
                  sum(cast(`12/31/20` as int)) - sum(cast(`12/30/20` as int)) as `dec 31`,
                  sum(cast(`1/1/21` as int)) - sum(cast(`12/31/20` as int)) as `jan 1` from festivedeaths group by Province_State order by Province_State""")
    fd.coalesce(1).write.csv("festivedeaths.csv")
    fd.show()
  }

  
  def monthlyDRratio(spark: SparkSession) = {
    import spark.implicits._
    val timeSeriesDeathsDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_deaths.csv")
    val timeSeriesRecoveredDF = spark.read.format("csv").option("header", "true") .option("mode", "DROPMALFORMED").load("time_series_covid_19_recovered.csv")
    timeSeriesDeathsDF.createOrReplaceTempView("deathrate")
    timeSeriesRecoveredDF.createOrReplaceTempView("recoveryrate")

    val deathTable = spark.sql("""Select `Country/Region` as `Country`, 
                  sum(cast(`2/1/20` as int)) - sum(cast(`1/22/20` as int)) as `djan`,
                  sum(cast(`3/1/20` as int)) - sum(cast(`2/1/20` as int)) as `dfeb`,
                  sum(cast(`4/1/20` as int)) - sum(cast(`3/1/20` as int)) as `dmar`,
                  sum(cast(`5/1/20` as int)) - sum(cast(`4/1/20` as int)) as `dapr`,
                  sum(cast(`6/1/20` as int)) - sum(cast(`5/1/20` as int)) as `dmay`,
                  sum(cast(`7/1/20` as int)) - sum(cast(`6/1/20` as int)) as `djun`,
                  sum(cast(`8/1/20` as int)) - sum(cast(`7/1/20` as int)) as `djul`,
                  sum(cast(`9/1/20` as int)) - sum(cast(`8/1/20` as int)) as `daug`,
                  sum(cast(`10/1/20` as int)) - sum(cast(`9/1/20` as int)) as `dsep`,
                  sum(cast(`11/1/20` as int)) - sum(cast(`10/1/20` as int)) as `doct`,
                  sum(cast(`12/1/20` as int)) - sum(cast(`11/1/20` as int)) as `dnov`,
                  sum(cast(`1/1/21` as int)) - sum(cast(`12/1/20` as int)) as `ddec`,
                  sum(cast(`2/1/21` as int)) - sum(cast(`1/1/21` as int)) as `djanII`,
                  sum(cast(`3/1/21` as int)) - sum(cast(`2/1/21` as int)) as `dfebII`,
                  sum(cast(`4/1/21` as int)) - sum(cast(`3/1/21` as int)) as `dmarII`,
                  sum(cast(`5/1/21` as int)) - sum(cast(`4/1/21` as int)) as `daprII` from deathrate group by `Country/Region` order by `Country/Region`""")

    val recoveryTable = spark.sql("""Select `Country/Region` as `Region`, 
                  sum(cast(`2/1/20` as int)) - sum(cast(`1/22/20` as int)) as `rjan`,
                  sum(cast(`3/1/20` as int)) - sum(cast(`2/1/20` as int)) as `rfeb`,
                  sum(cast(`4/1/20` as int)) - sum(cast(`3/1/20` as int)) as `rmar`,
                  sum(cast(`5/1/20` as int)) - sum(cast(`4/1/20` as int)) as `rapr`,
                  sum(cast(`6/1/20` as int)) - sum(cast(`5/1/20` as int)) as `rmay`,
                  sum(cast(`7/1/20` as int)) - sum(cast(`6/1/20` as int)) as `rjun`,
                  sum(cast(`8/1/20` as int)) - sum(cast(`7/1/20` as int)) as `rjul`,
                  sum(cast(`9/1/20` as int)) - sum(cast(`8/1/20` as int)) as `raug`,
                  sum(cast(`10/1/20` as int)) - sum(cast(`9/1/20` as int)) as `rsep`,
                  sum(cast(`11/1/20` as int)) - sum(cast(`10/1/20` as int)) as `roct`,
                  sum(cast(`12/1/20` as int)) - sum(cast(`11/1/20` as int)) as `rnov`,
                  sum(cast(`1/1/21` as int)) - sum(cast(`12/1/20` as int)) as `rdec`,
                  sum(cast(`2/1/21` as int)) - sum(cast(`1/1/21` as int)) as `rjanII`,
                  sum(cast(`3/1/21` as int)) - sum(cast(`2/1/21` as int)) as `rfebII`,
                  sum(cast(`4/1/21` as int)) - sum(cast(`3/1/21` as int)) as `rmarII`,
                  sum(cast(`5/1/21` as int)) - sum(cast(`4/1/21` as int)) as `raprII` from recoveryrate group by `Country/Region` order by `Country/Region`""")
    
    deathTable.createOrReplaceTempView("dT")
    recoveryTable.createOrReplaceTempView("rT")

    val joinedTable = spark.sql("select * from dT join rT on dT.Country = rT.Region order by Country")

    joinedTable.createOrReplaceTempView("jT")

    val deathrecTable = spark.sql("""select Country, "...",
                                   dapr as `apr '20 deaths`, rapr as `apr '20 recovery `, dapr/rapr as `apr '20 ratio`, "...",
                                  daprII as `apr '21 deaths`, raprII as `apr '21 recovery `, daprII/raprII as `apr '21 ratio` from jT""").show()
 

  }

}