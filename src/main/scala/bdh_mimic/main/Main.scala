package bdh_mimic.main

import com.google.cloud.spark.bigquery._
import bdh_mimic.model.{PatientStatic, Events, Items, queryResult_test}
import bdh_mimic.tutorial_sparkbq.tutorial_sparkbq //import package and object for fnc
import bdh_mimic.dataload.dataload
import bdh_mimic.utils.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_timestamp

//Plan create jar files from project and load into gcp dataproc to run job and write table out to bigquery
//gcp https://www.youtube.com/watch?v=XriOHFKrzLM
//create jar with intellij https://sqlrelease.com/create-jar-in-intellij-idea-for-maven-based-scala-spark-project

object Main {

  def main(args: Array[String]) {

    //Set app for .jar
    val spark = SparkSession.builder.appName("simp").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    //Tutorial Run
//    val result_tut: RDD[queryResult_test] = tutorial_sparkbq.bq_test(spark)
//    result_tut.take(1).foreach(println)

    //Get Static Patient Data
    val patients_static: RDD[PatientStatic] = dataload.get_patients_static_variables(spark)
//
//    patients_static.take(1).foreach(println)
//
//    patients_static.cache()
//
//    Get Chart Events Data
    val (icu_chart, icu_lab) = dataload.get_icu_events(spark)
//
//    icu_chart.take(1).foreach(println)
//
//    icu_lab.take(1).foreach(println)

    //Remove Outliers
    val items = dataload.get_items(spark)

    val icu_chart_impute = utils.outlier_removal(spark, items, icu_chart)

    //So far have chart data loaded (item and lab dataloaded as well just not returned) need to perform hourly agg on this

  }

}