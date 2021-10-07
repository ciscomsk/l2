import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, count, pow, sum}
import org.apache.spark.sql.types.IntegerType

import java.net.URLDecoder

object Lab1 {
  def main(args: Array[String]): Unit = {
    val log: Logger = Logger.getLogger("org")
    log.setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab1")
      .master("local[*]")
//      .master("spark://spark-master-6:7077")
      .config("spark.submit.deployMode","client")
      .getOrCreate()

    log.error("Spark session started.")

    val logRdd: RDD[(String, String)] = spark
      .sparkContext
//      .textFile("hdfs://spark-de-master-1.newprolab.com:8020/labs/laba02/logs/")
      .textFile("/labs/laba02/logs/")
//      .textFile("src/main/resources/labs/laba02/logs/")
      .map(_.split("\t").toList)
      .filter(_.length == 3)
      .map { case List(uid, _, url) =>
        val replacedUTF8: String = URLDecoder.decode(url, "UTF-8")
        val removedWWW: String = replacedUTF8.replace("www.", "")

        List(uid, removedWWW)
      }
      .filter(list => list(1).contains("http://") || list(1).contains("https://"))
      .map { case List(uid, url) =>
        val domain: String = url
          .replace("http://", "")
          .replace("https://", "")
          .split("/").head

        List(uid, domain)
      }
      .map(list => (list.head, list(1)))

    log.error("logRdd processed.")
//    println(logRdd.count())
//    logRdd.take(10).foreach(println)

    val logDf: DataFrame = spark
      .createDataFrame(logRdd)
      .toDF("uid", "domain")

    log.error("logRdd converted to logDf.")
//    logDf.show()

    val autousersDf: DataFrame = spark
      .read
//      .json("src/main/resources/labs/laba02/autousers.json")
      .json("/labs/laba02/autousers.json")

    log.error("autousersDf processed.")

    val logsWithAutousersDf: DataFrame =  logDf.crossJoin(autousersDf)
    log.error("logsWithAutousersDf processed.")
//    logsWithAutousersDf.show()


    import spark.implicits._

    val flagDf: DataFrame = logsWithAutousersDf
      .withColumn("auto_flag", array_contains($"autousers", $"uid").cast(IntegerType))
      .drop("uid", "autousers")

    log.error("flagDf processed.")
//    flagDf.show()

    flagDf.cache()
    log.error("flagDf cached.")

//    println(flagDf.filter($"auto_flag" === true).count())
//    println(flagDf.filter($"auto_flag" === false).count())

    val totalAggDf: DataFrame = flagDf
      .agg(
        count("*").as("total_count"),
        sum("auto_flag").as("one_total_count")
      )

    log.error("totalAggDf processed.")
//    totalAggDf.show()

    val aggByDomainDf: DataFrame = flagDf
      .groupBy("domain")
      .agg(
        count("auto_flag").as("domain_total_count"),
        sum("auto_flag").as("domain_one_count")
      )
      .withColumn("domain_zero_count", $"domain_total_count" - $"domain_one_count")

    log.error("aggByDomainDf processed.")
//    aggByDomainDf.show()

    flagDf.unpersist()
    log.error("flagDf uncached.")

    val composedAggDf: DataFrame = aggByDomainDf.crossJoin(totalAggDf)
    log.error("composedAggDf processed.")
//    composedAggDf.show()

    val resultDf: Dataset[Row] = composedAggDf
      .withColumn("relevance", pow($"domain_one_count" / $"total_count", 2)/(($"domain_total_count" / $"total_count") * ($"one_total_count" / $"total_count")))
      .drop("domain_total_count", "domain_one_count", "domain_zero_count", "total_count", "one_total_count")
      .orderBy($"relevance".desc, $"domain")
      .limit(200)

    log.error("resultDf processed.")
//    resultDf.show(truncate = false)
//    println(resultDf.schema)

    resultDf
      .repartition(1)
      .rdd
      .map(_.mkString("\t"))
//      .saveAsTextFile("src/main/resources/rdd")
      .saveAsTextFile("/user/mikhail.kuznetzov/lab1")

    log.error("resultRdd saved.")
  }
}
