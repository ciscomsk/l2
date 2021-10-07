import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, count, pow, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.net.{URL, URLDecoder}
import scala.util.Try

object Lab1 {
  def main(args: Array[String]): Unit = {
    val log: Logger = Logger.getLogger("org")
    log.setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab1")
      .master("local[*]")
//      .config("spark.submit.deployMode","client")
      .getOrCreate()

    log.error("Spark session started.")

    val logRdd: RDD[(String, String)] = spark
      .sparkContext
//      .textFile("/labs/laba02/logs/")
      .textFile("src/main/resources/labs/laba02/logs/")
      .map(_.split("\t").toList)
      .filter(list => list.length == 3 && list(2).contains("http"))
      .map { case List(uid, _, url) =>
        val replacedUTF8: String =
          Try(new URL(URLDecoder.decode(url, "UTF-8")).getHost).getOrElse("")

        val removedStartWww: String = replacedUTF8.replaceAll("^www\\.", "")

        List(uid, removedStartWww)
      }
      .collect { case list if list(1).nonEmpty => (list.head, list(1)) }
//      .filter(list => list(1).nonEmpty)
//      .map(list => (list.head, list(1)))

//    println(logRdd.count())
//    logRdd.take(10).foreach(println)
    log.error("logRdd processed.")

    val logDf: DataFrame = spark
      .createDataFrame(logRdd)
      .toDF("uid", "domain")

//    logDf.show()
    log.error("logRdd converted to logDf.")

    val autousersDf: DataFrame = spark
      .read
      .json("src/main/resources/labs/laba02/autousers.json")
//      .json("/labs/laba02/autousers.json")

    log.error("autousersDf processed.")

    val logsWithAutousersDf: DataFrame =  logDf.crossJoin(autousersDf)

//    logsWithAutousersDf.show()
    log.error("logsWithAutousersDf processed.")

    import spark.implicits._

    val flagDf: DataFrame = logsWithAutousersDf
      .withColumn("auto_flag", array_contains($"autousers", $"uid").cast(IntegerType))
      .drop("uid", "autousers")

//    flagDf.show()
    log.error("flagDf processed.")

    flagDf.cache()
    log.error("flagDf cached.")

//    println(flagDf.filter($"auto_flag" === true).count())
//    println(flagDf.filter($"auto_flag" === false).count())

    val totalAggDf: DataFrame = flagDf
      .agg(
        count("*").as("total_count"),
        sum("auto_flag").as("one_total_count")
      )

//    totalAggDf.show()
    log.error("totalAggDf processed.")

    val aggByDomainDf: DataFrame = flagDf
      .groupBy("domain")
      .agg(
        count("auto_flag").as("domain_total_count"),
        sum("auto_flag").as("domain_one_count")
      )
      .withColumn("domain_zero_count", $"domain_total_count" - $"domain_one_count")

//    aggByDomainDf.show()
    log.error("aggByDomainDf processed.")

    flagDf.unpersist()
    log.error("flagDf uncached.")

    val composedAggDf: DataFrame = aggByDomainDf.crossJoin(totalAggDf)

//    composedAggDf.show()
    log.error("composedAggDf processed.")

    val resultDf: Dataset[Row] = composedAggDf
      .withColumn("relevance", pow($"domain_one_count" / $"total_count", 2)/(($"domain_total_count" / $"total_count") * ($"one_total_count" / $"total_count")))
      .drop("domain_total_count", "domain_one_count", "domain_zero_count", "total_count", "one_total_count")
      .withColumn("relevance", $"relevance".cast(DecimalType(16, 15)))
      .orderBy($"relevance".desc, $"domain")
      .limit(200)

//    resultDf.show(truncate = false)
//    println(resultDf.schema)
    log.error("resultDf processed.")

    resultDf
      .repartition(1)
      .rdd
      .map(_.mkString("\t"))
      .saveAsTextFile("src/main/resources/rdd")
//      .saveAsTextFile("/user/mikhail.kuznetzov/lab1")

    log.error("resultRdd saved.")
  }
}
