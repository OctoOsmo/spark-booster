package booster

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, row_number, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object DataFrameApp {
  def userVisitsCsv: StructType = {
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

  def blocksCsv: StructType = {
    StructType(Array(
      StructField("startIpNum", LongType, nullable = true),
      StructField("endIpNum", LongType, nullable = true),
      StructField("locId", IntegerType, nullable = true)))
  }

  def locationCsv: StructType = {
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

  val sqlIpv4ToLong: UserDefinedFunction = udf((ip: String) =>
    Try(ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0, 8, 16, 24)).map(xi => xi._1 << xi._2).sum)
      .toOption)
  val sqlDateToYear: UserDefinedFunction = udf((date: String) => date.take(4))

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

    val userVisits = spark.read.format("csv")
      .schema(userVisitsCsv)
      .option("header", value = false)
      .load(uvPath)
      .withColumn("ip", sqlIpv4ToLong(col("ip")))

    val geoipBlocks = spark.read.format("csv")
      .schema(blocksCsv)
      .option("header", value = false)
      .load(geoipBlocksPath)

    val geoipLocation = spark.read.format("csv")
      .schema(locationCsv)
      .option("header", value = true)
      .load(geoipLocationPath)

    val locations: Broadcast[DataFrame] = spark.sparkContext.broadcast(geoipBlocks.join(geoipLocation, "locId"))

    val yearlyRevenueWindow = Window.partitionBy("year")
      .orderBy(col("sum(revenue)").desc)

    userVisits.as("uv")
      .join(locations.value.as("loc"), col("uv.ip").between(col("loc.startIpNum"), col("loc.endIpNum")))
      .withColumn("date", sqlDateToYear(col("date")))
      .withColumnRenamed("date", "year")
      .filter(col("city").isNotNull)
      .withColumn("year", col("year").cast(IntegerType))
//      .filter(col("year") > 2010)
      .select("year", "country", "city", "revenue")
      .groupBy("year", "country", "city")
      .sum("revenue")
      .withColumn("rank", row_number.over(yearlyRevenueWindow)).where(col("rank") <= 3)
      .orderBy(col("year"), col("rank"))
      .withColumnRenamed("sum(revenue)", "sum(adRevenue)")
      .withColumnRenamed("country", "countryCode")
      .write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv(outputPath + "/uvl")

    spark.read
      .format("csv")
      .option("header", value = true)
      .load(outputPath + "/uvl")
      .orderBy(col("year"), col("rank"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv(outputPath + "/merged")
  }
}
