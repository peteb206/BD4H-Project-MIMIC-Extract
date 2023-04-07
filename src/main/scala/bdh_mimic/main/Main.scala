package bdh_mimic.main

import com.google.cloud.spark.bigquery._
import bdh_mimic.model.{PatientStatic, queryResult_test}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

//Plan create jar files from project and load into gcp dataproc to run job and write table out to bigquery
//gcp https://www.youtube.com/watch?v=XriOHFKrzLM
//create jar with intellij https://sqlrelease.com/create-jar-in-intellij-idea-for-maven-based-scala-spark-project

object Main {

  def main(args: Array[String]) {

    //Bigquery: https://sidshome.wordpress.com/2022/06/19/query-google-biqquery-from-a-scala-console-app/

    //Set app for .jar
    val spark = SparkSession.builder.appName("simp").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val patients_static: RDD[PatientStatic] = get_patients_static_variables(spark)

    patients_static.take(1).foreach(println)

    //Spark bigQuery Notes/ Examples
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

  }
  def get_patients_static_variables(spark: SparkSession, age: Int = 15, visit: Int = 1,
                   icuDurMin: Int = 12, icuDurMax: Int = 10*24): RDD[PatientStatic] = {
    import spark.implicits._

    //Function to get patients static data
    //See query patient_static

    //takes SparkSession, Patient age, ICU admission visit, ICU duration hr min,  ICU duration hr max

    //Variable Concept (Table 1 Paper)
    //age patient age (masked as 300 for patients older than 89 years old in MIMIC-III)
    //ethnicity patient ethnicity
    //gender patient gender
    //insurance patient insurance type
    //admittime hospital admission time
    //dischtime hospital discharge time
    //intime ICU admission time
    //outtime ICU discharge time
    //admission_type type of hospital admission
    //first_careunit type of ICU when first admitted

    //Values in RDD
    //Admission Table Columns:
    //SUBJECT_ID
    //HADM_ID
    //ADMITTIME
    //DISCHTIME
    //ADMISSION_TYPE
    //INSURANCE

    //Patient Table Columns:
    //DOB
    //GENDER

    //ICU Table Columns:
    //INTIME
    //OUTTIME
    //FIRST_CAREUNIT
    //ICUDur = OUTTIME - INTIME
    //visit = DENSE_RANK() OVER(PARTITION BY icu.SUBJECT_ID ORDER BY icu.INTIME) as visit

    //As outlined in prior work:
    //the subject is an adult (age of at least 15
    //at time of admission), the stay is the first known ICU admission for
    //the subject, and the total duration of the stay is at least 12 hours
    //and less than 10 days
    //Source: MIMIC-Extract: A Data Extraction, Preprocessing, and
    //Representation Pipeline for MIMIC-III

    val patient_stat: RDD[PatientStatic] = spark.read.format("bigquery")
      .option("table", "bdh6250-380417.MIMIC_Extract.patients_static")
      .load()
      .filter(s"(ICUDur BETWEEN $icuDurMin and $icuDurMax) AND (visit = $visit) AND (age >= $age)")
      .as[PatientStatic].rdd

    //TO-DO work through how to get Timestamp

    //For Paper (Site): Our pipeline
    //presents values for static variables as they originally appear in
    //MIMIC-III raw data with no additional outlier removal. For example,
    //age for patients older than eighty-nine is masked as 300 in MIMICIII for privacy reasons,
    // and our pipeline preserves this

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


    patient_stat
  }
}