package bdh_mimic.main

import com.google.cloud.spark.bigquery._
import bdh_mimic.model.{queryResult_test, dop_dur}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//Plan create jar files from project and load into gcp dataproc to run job and write table out to bigquery
//gcp https://www.youtube.com/watch?v=XriOHFKrzLM
//create jar with intellij https://sqlrelease.com/create-jar-in-intellij-idea-for-maven-based-scala-spark-project

object Main {

  def main(args: Array[String]) {

    //Bigquery: https://sidshome.wordpress.com/2022/06/19/query-google-biqquery-from-a-scala-console-app/

    //Set app for .jar
    val spark = SparkSession.builder.appName("simp").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    //  lets query and convert out bigQuery table to an RDD
    //    val df = spark.read.option("bdh6250-380417", "264814785831").bigquery("bdh6250-380417.mimic_test.query_result")
    ////    df.show()
    //
    //    val resultsRDD: RDD[queryResult] = df.as[queryResult].rdd
    //    resultsRDD.take(1).foreach(println)
    //
    ////    Some Fun
    //    val sum_stats = resultsRDD.map(x => (x.SUBJECT_ID,1.0)).reduceByKey(_+_)
    //    sum_stats.take(1).foreach(println)

    //Inspect a table
    //    val dop = spark.read.option("bdh6250-380417", "264814785831").bigquery("bdh6250-380417.MIMIC_Extract.icu_dopamine_dur")

    //    dop.show()
    //
    //    //Inspect Schema
    //    dop.printSchema()

    //RDD //ERROR
    //    val dopRDD: RDD[dop_dur] = dop.as[dop_dur].rdd
    //    dopRDD.take(1).foreach(println)

    //load dataset as RDD
    val test_load = (spark.read.format("bigquery")
      .option("table", "bdh6250-380417.MIMIC_Extract.icu_dopamine_dur")
      .load()
      .cache())

    //TO_DO
    //Will load in Row format need to figure out how to convert prior to sql
    //Need to import rest of the tables into RDD and get dosage time maybe easier in bigQuery

    test_load.createOrReplaceTempView("dop_dur")

    //Find Schema in Console: bq show --format=prettyjson bdh6250-380417:MIMIC_Extract.icu_dopamine_dur
    val test_loaddf = spark.sql(
      "SELECT * FROM dop_dur LIMIT 10")

    test_loaddf.show()

    //if you pack in object faster to debug
    //cmd >
    //sbt
    //cmd >
    //compile
    //cmd >
    //runMain --figure out

  }

}