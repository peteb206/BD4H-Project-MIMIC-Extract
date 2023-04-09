package bdh_mimic.tutorial_sparkbq

import bdh_mimic.model.{queryResult_test}
import com.google.cloud.spark.bigquery._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object tutorial_sparkbq {
  //set obeject

  def bq_test (spark: SparkSession): RDD[queryResult_test] = {
    //def function in object

    //gcloud auth application-default login
    //gcloud config set project <Project-Name>

    //Spark bigQuery Notes/ Examples
    //  lets query and convert out bigQuery table to an RDD
        val df = spark.read.option("bdh6250-380417", "264814785831").bigquery("bdh6250-380417.MIMIC_Extract.codes")
    ////    df.show()
        import spark.implicits._

        val resultsRDD: RDD[queryResult_test] = df.as[queryResult_test].rdd
//        resultsRDD.take(1).foreach(println)

    //    Some Fun
//        val sum_stats = resultsRDD.map(x => (x.SUBJECT_ID,1.0)).reduceByKey(_+_)
//        sum_stats.take(1).foreach(println)

    //Inspect a table
//        val dop = spark.read.option("bdh6250-380417", "264814785831").bigquery("bdh6250-380417.MIMIC_Extract.icu_dopamine_dur")

//        dop.show()

        //Inspect Schema
//        dop.printSchema()

    //RDD //ERROR
    //    val dopRDD: RDD[dop_dur] = dop.as[dop_dur].rdd
    //    dopRDD.take(1).foreach(println)

    //load dataset as RDD
    //    val test_load = (spark.read.format("bigquery")
    //      .option("table", "bdh6250-380417.MIMIC_Extract.icu_dopamine_dur")
    //      .load()
    //      .cache())
    //
    //    //TO_DO
    //    //Will load in Row format need to figure out how to convert prior to sql
    //    //Need to import rest of the tables into RDD and get dosage time maybe easier in bigQuery
    //
    //    test_load.createOrReplaceTempView("dop_dur")
    //
    //    //Find Schema in Console: bq show --format=prettyjson bdh6250-380417:MIMIC_Extract.icu_dopamine_dur
    //    val test_loaddf = spark.sql(
    //      "SELECT * FROM dop_dur LIMIT 10")
    //
    //    test_loaddf.show()

    //if you pack in object faster to debug
    //cmd >
    //sbt
    //cmd >
    //compile
    //cmd >
    //runMain --figure out

    //OLD CODE

    //    val admission: RDD[Admission] = spark.read.format("bigquery")
    //      .option("table", "bdh6250-380417.MIMIC_Extract.icu_admission_sorted1")
    //      .load()
    //      .filter(s"(ICUDur BETWEEN $icuDurMin and $icuDurMax) AND (visit_id = $visit)")
    //      .as[Admission].rdd
    //
    //    val patient: RDD[Patient] = spark.read.format("bigquery")
    //      .option("table", "bdh6250-380417.MIMIC_Extract.patient_age")
    //      .load()
    //      .filter(s"age >= $age")
    //      .as[Patient].rdd

    //    val patient: RDD[Patient] = p1.as[Patient].rdd

    //https://towardsdatascience.com/best-practices-for-caching-in-spark-sql-b22fb0f02d34

    //    admission.cache()
    //    patient.cache()

    //    val admin_filter = patient.map(x => x.HADM_ID).collect.toSeq
    //
    //    val patient_filter = patient.filter(x => admin_filter.contains(x.HADM_ID))

    //    patient_filter.take(1).foreach(println)

    resultsRDD
  }

}
