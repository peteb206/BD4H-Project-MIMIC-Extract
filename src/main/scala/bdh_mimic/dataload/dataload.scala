package bdh_mimic.dataload

import bdh_mimic.model.{Events, Items, PatientStatic}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_timestamp

object dataload {

  def get_patients_static_variables(spark: SparkSession, age: Int = 15, visit: Int = 1,
                                    icuDurMin: Int = 12, icuDurMax: Int = 10 * 24): RDD[PatientStatic] = {
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
      .withColumn("ADMITTIME", to_timestamp($"ADMITTIME"))
      .withColumn("DISCHTIME", to_timestamp($"DISCHTIME"))
      .withColumn("INTIME", to_timestamp($"INTIME"))
      .withColumn("OUTTIME", to_timestamp($"OUTTIME"))
      .as[PatientStatic]
      .rdd

    //For Paper (Site): Our pipeline
    //presents values for static variables as they originally appear in
    //MIMIC-III raw data with no additional outlier removal. For example,
    //age for patients older than eighty-nine is masked as 300 in MIMICIII for privacy reasons,
    // and our pipeline preserves this

    patient_stat
  }

  def get_icu_events(spark: SparkSession): (RDD[Events], RDD[Events]) = {
    import spark.implicits._

    //queries used
    //query icustay_chartevents, table icustay_charevents
    //query icustay_labevents, table icustay_labevents

    //icustay_chartevents
    //Columns

    //ICU Table
    //Subject_ID
    //HADM_ID
    //ICUSTAY_ID

    //Chart Events
    //CHARTTIME
    //ITEM_ID
    //VALUE
    //VALUEUOM

    val icu_chart: RDD[Events] = spark.read.format("bigquery")
      .option("table", "bdh6250-380417.MIMIC_Extract.icustay_chartevents")
      .load()
      .withColumn("CHARTTIME", to_timestamp($"CHARTTIME"))
      .withColumn("VALUE", $"VALUE".cast("Double"))
      .as[Events]
      .rdd

    //Filtered to Chart Time in ICU Stay & No Errors & ValueNUM not null

    //icustay_labevents
    //Columns

    //ICU Table
    //Subject_ID
    //HADM_ID
    //ICUSTAY_ID

    //Lab Events
    //CHARTTIME
    //ITEM_ID
    //VALUE
    //VALUEUOM

    val icu_lab: RDD[Events] = spark.read.format("bigquery")
      .option("table", "bdh6250-380417.MIMIC_Extract.icustay_labevents")
      .load()
      .withColumn("CHARTTIME", to_timestamp($"CHARTTIME"))
      .withColumn("VALUE", $"VALUE".cast("Double"))
      .as[Events]
      .rdd

    //Filtered to Chart Time in 6 hour interval of Stay &  ValueNUM > 0

    (icu_chart, icu_lab)
  }

  def get_items(spark: SparkSession): RDD[Items] = {

    import spark.implicits._
    //items, query items
    //Item name table

    val items: RDD[Items] = spark.read.format("bigquery")
      .option("table", "bdh6250-380417.MIMIC_Extract.items")
      .load()
      .as[Items]
      .rdd

    items
  }

}
