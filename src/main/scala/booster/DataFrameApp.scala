package booster

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object DataFrameApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val sc = spark.conf
    val uvPath = sc.get("spark.booster.uv.path")
    val geoipBlocksPath = sc.get("spark.booster.geoip.blocks.path")
    val geoipLocationPath = sc.get("spark.booster.geoip.location.path")
    val outputPath = sc.get("spark.booster.output.path")
    val hc = spark.sparkContext.hadoopConfiguration
    hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    //    io.File(outputPath).deleteRecursively()

    def userVisitsCsv = {
      StructType(Array(
        StructField("ip", StringType, nullable = true),
        StructField("url", StringType, nullable = true),
        StructField("date", StringType, nullable = true),
        StructField("revenue", FloatType, nullable = true),
        StructField("ua", StringType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("lang", StringType, nullable = true),
        StructField("search", StringType, nullable = true),
        StructField("duration", IntegerType, nullable = true)))
    }

    def blocksCsv = {
      StructType(Array(
        StructField("startIpNum", LongType, nullable = true),
        StructField("endIpNum", LongType, nullable = true),
        StructField("locId", IntegerType, nullable = true)))
    }

    def locationCsv = {
      StructType(Array(
        StructField("locId", IntegerType, nullable = true),
        StructField("l_country", StringType, nullable = true),
        StructField("l_region", StringType, nullable = true),
        StructField("city", StringType, nullable = true),
        StructField("l_postalCode", StringType, nullable = true),
        StructField("l_latitude", FloatType, nullable = true),
        StructField("l_longitude", FloatType, nullable = true),
        StructField("l_metroCode", StringType, nullable = true),
        StructField("l_areaCode", StringType, nullable = true)))
    }

    val coder: (String => Option[Long]) = ipv4ToLong
    val sqlIpv4ToLong = udf(coder)

    val yearTaker: (String => String) = getYear
    val sqlDateToYear = udf(yearTaker)

    val userVisits = spark.read
      .format("csv")
      .schema(userVisitsCsv)
      .option("header", value = true)
      .load(uvPath)
      .withColumn("ip", sqlIpv4ToLong(col("ip")))

    val geoipBlocks = spark.read
      .format("csv")
      .schema(blocksCsv)
      .option("header", value = true)
      .load(geoipBlocksPath)

    val geoipLocation = spark.read
      .format("csv")
      .schema(locationCsv)
      .option("header", value = true)
      .load(geoipLocationPath)

    val locations = geoipBlocks.join(geoipLocation, "locId")
    //      .filter(col("city").isNotNull)

    val w = Window.partitionBy("year", "city")
      .orderBy(col("sum(revenue)").desc)
//    val w2 = Window
//      .partitionBy("year", "country")
//      .orderBy(col("sum(revenue)").desc)
//
//    val refined = userVisits
//      .withColumn("date", sqlDateToYear(col("date")))
//      .withColumnRenamed("date", "year")
//      .withColumn("rn", row_number.over(w2)).where(col("rn") <= 3)
//      .orderBy(col("year").desc, col("country"), col("rn"))

    val uvl: DataFrame = userVisits.as("uv")
      .join(locations.as("loc"), col("uv.ip").between(col("loc.startIpNum"), col("loc.endIpNum")))
      .withColumn("date", sqlDateToYear(col("date")))
      .withColumnRenamed("date", "year")
      .filter(col("city").isNotNull)
      .filter(col("year") > 2012)
      .select("year", "country", "city", "revenue")
      .groupBy("year", "country", "city")
      .sum("revenue")
      .withColumn("rn", row_number.over(w)).where(col("rn") <= 3)
      .orderBy(col("sum(revenue)").desc, col("rn").desc)

    //    val uvl2 = uvl.select("date", "country", "city", "sum(adRevenue)")
    //      .orderBy("sum(adRevenue)")

    val namedFrames = List(
      //      (userVisits, "/userVisits"),
      //      (geoipBlocks, "/geoipBlocks"),
      //      (geoipLocation, "/geoipLocation"),
      //      (locations, "/locations"),
//      (refined, "/refined"),
      (uvl, "/uvl")
    )
    printFrames(namedFrames.map(_._1))
    writeFrames(namedFrames, outputPath)
  }

  def ipv4ToLong(ip: String): Option[Long] = Try(
    ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0, 8, 16, 24)).map(xi => xi._1 << xi._2).sum
  ).toOption

  def getYear(date: String): String = {
    date.take(4)
  }

  def writeFrames(dfTuplesList: List[(DataFrame, String)], outputPath: String): Unit = {
    dfTuplesList.foreach { df =>
      df._1.write
        .mode(SaveMode.Overwrite)
        .option("header", value = true)
        .csv(outputPath + df._2)
    }
  }

  def printFrames(dfList: List[DataFrame]): Unit = {
    dfList.foreach { df =>
      df.show
      df.printSchema
    }
  }
}
