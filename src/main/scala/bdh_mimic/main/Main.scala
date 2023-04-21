package bdh_mimic.main

import com.google.cloud.spark.bigquery._
import bdh_mimic.model.{PatientStatic, Events, Items, queryResult_test}
import bdh_mimic.tutorial_sparkbq.tutorial_sparkbq //import package and object for fnc
import bdh_mimic.dataload.dataload
import bdh_mimic.utils.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_timestamp

object Main {

  def main(args: Array[String]) {

    //Set app for .jar
    val spark = SparkSession.builder
      .appName("simp")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    //Tutorial Run
//    val result_tut: RDD[queryResult_test] = tutorial_sparkbq.bq_test(spark)
//    result_tut.take(1).foreach(println)

    //Get Static Patient Data
    val patients_static: RDD[PatientStatic] = dataload.get_patients_static_variables(age = 15)
//
//    patients_static.take(1).foreach(println)
//
//    patients_static.cache()
//
//    Get Chart Events Data
    val (icu_chart, icu_lab) = dataload.get_icu_events()
//
//    icu_chart.take(1).foreach(println)
//
//    icu_lab.take(1).foreach(println)

    icu_chart.cache()

    //Remove Outliers
    val items = dataload.get_items()

    items.cache()

    val icu_chart_impute = utils.outlier_removal(items, icu_chart)

//    val testdf = icu_chart_impute.toDF().filter("ITEMID = 226707")

//    testdf.show()

    //So far have chart data loaded (item and lab dataloaded as well just not returned) need to perform hourly agg on this
    icu_chart_impute.cache()
    val hourly_agg = utils.hourly_agg(icu_chart_impute)
    hourly_agg.cache()

    // println("icu_chart_impute count: " + icu_chart_impute.count())
    // Expecting "icu_chart_impute count: 145199077"

    // println("hourly_agg count: " + hourly_agg.count())
    // Expecting "hourly_agg count: 99373881"

//    hourly_agg.take(20).foreach(println)

    val int_test = utils.interventions(("icu_vasopressor_dur"))

    int_test.take(1).foreach(println)

    utils.to_csv(int_test.toDF(),"icu_vasopressor_dur.csv")

  }

}