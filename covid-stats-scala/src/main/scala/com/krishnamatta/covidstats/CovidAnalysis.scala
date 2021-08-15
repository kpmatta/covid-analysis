package main.scala.com.krishnamatta.covidstats

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object CovidAnalysis {
  lazy val spark = SparkSession.builder()
    .master("local[*]")
    .appName("covid")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val srcConfirmedFile = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    val localConfirmedFile = "data/time_series_covid19_confirmed_global.csv"
    Download.downloadFile(srcConfirmedFile, localConfirmedFile)
    analyseData(localConfirmedFile)

    val deathsFile = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
    val localDeaths = "time_series_covid19_deaths_global.csv"
    Download.downloadFile(deathsFile, localDeaths)
    analyseData(localDeaths)
  }

  def analyseData(filePath: String): Unit = {
    val inDf = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(filePath)
      .withColumnRenamed("Country/Region", Constants.COUNTRY)

    // get latest 5 dates from columns list
    val dateColumns = inDf.columns.takeRight(5)

    // prepare select columns
    val selColumns =  Array("Country") ++ dateColumns.map(c => s"`${c}`")

    // wrap date columns with sum function
    val sumDateColumns = dateColumns.map(col => sum(s"`$col`").alias(col))

    inDf.selectExpr(selColumns:_*)
      .groupBy(Constants.COUNTRY)
      .agg(sumDateColumns.head, sumDateColumns.tail:_*)
      .orderBy(desc(dateColumns.last))
      .show()
  }
}
