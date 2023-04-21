package bdh_mimic.utils

import bdh_mimic.model.{Events, HourlyAgg, Items, PatientStatic, ValRange, Intervention}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{avg, count, sum, to_timestamp, window}

object utils {

  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._

  def outlier_removal(items: RDD[Items], icu_chart: RDD[Events]
                     ): RDD[Events] = {

    //Fnc to remove and impute outliers
    val sc = spark.sparkContext

    //https://www.nature.com/articles/sdata201635 Nice Chart of MIMIC Data

//    def read_filter()
    //See variable_ranges.csv in resource_data
    //Note in paper not clear if outlier detections is from source paper or code base csv
    //It appears to be missing values in the dataset from GCP physionet compaired to past research
    //https://stackoverflow.com/questions/29704333/spark-load-csv-file-as-dataframe
    val outlier_ranges = spark.read.format("csv")
      .option("header", "true")
      .load("resource_data/variable_ranges.csv")
      .withColumn("OUTLIER_LOW", $"OUTLIER_LOW".cast("Double"))
      .withColumn("VALID_LOW", $"VALID_LOW".cast("Double"))
      .withColumn("IMPUTE", $"IMPUTE".cast("Double"))
      .withColumn("VALID_HIGH", $"VALID_HIGH".cast("Double"))
      .withColumn("OUTLIER_HIGH", $"OUTLIER_HIGH".cast("Double"))
      .as[ValRange].rdd

//    outlier_ranges.cache()

    val ranges_seq = outlier_ranges.map(x => x.LEVEL2).collect().toSeq

    val items_seq = items.map(x => x.LABEL).collect().toSeq

    val items_filter = items.filter(x => ranges_seq.contains(x.LABEL))

    val items_map = items_filter.map(row => (row.LABEL, row.ITEMID)).collect().toMap

    val outlier_ranges_filter = outlier_ranges.filter(x => items_seq.contains(x.LEVEL2))

    val valid_ranges = outlier_ranges_filter.map( x => (items_map.get(x.LEVEL2).get, x.VALID_LOW, x.VALID_HIGH, x.IMPUTE))

//    val sqlrange = valid_ranges.toDF()
//    sqlrange.show()

    val range_broadcast = sc.broadcast(valid_ranges.collect())

    val result = icu_chart.map { event =>
      val valid_range = range_broadcast.value.filter { case (id, _, _, _) => id == event.ITEMID }
      if (valid_range.nonEmpty) {
        val (_, vallow, valhigh, impute) = valid_range.head
        if (event.VALUE.getOrElse(Double.NaN) < vallow || event.VALUE.getOrElse(Double.NaN) > valhigh) {
          event.copy(VALUE = Some(impute))
        } else if (event.VALUE.fold(false)(_.isNaN)) {
          event.copy(VALUE = Some(impute))
        }
        else {
          event
        } }
//      } else if (event.VALUE.fold(false)(_.isNaN)) {
//        event.copy(VALUE = 0.0)
//      }
      else {
        event
      }
    }

    val test_res = result.filter(x => x.ITEMID == "227429" && x.VALUE.getOrElse(0.0) >= 20.85)

    val testerdf = test_res.toDF()

    testerdf.show()

    result
  }

  def hourly_agg(icu_chart: RDD[Events]): RDD[HourlyAgg] = {
    //this function caculates hourly agg
    icu_chart
      .toDF()
      .na.drop() // Don't include null values in aggregation
      .groupBy($"SUBJECT_ID", $"HADM_ID", $"ICUSTAY_ID", $"ITEMID", $"VALUEUOM",
        window($"CHARTTIME", "1 hour"))
      .agg(sum($"VALUE") as "VALUE_SUM", avg($"VALUE") as "VALUE_AVG", count($"VALUE") as "VALUE_COUNT")
      .withColumn("CHARTTIME_START", to_timestamp($"window.start"))
      .withColumn("CHARTTIME_END", to_timestamp($"window.end"))
      .as[HourlyAgg]
      .rdd
  }

  def interventions(table: String): RDD[Intervention] = {
    //this function creates intervention datasets
      if (table != "icu_ventilator_dur") {
          val df = spark.read.format("bigquery")
            .option("table", s"bdh6250-380417.MIMIC_Extract.$table")
            .option("readDataFormat", "AVRO")
            .load()
            .na.drop()
            .withColumn("starttime", to_timestamp($"starttime"))
            .withColumn("endtime", to_timestamp($"endtime"))

          val df1 = df.groupBy($"SUBJECT_ID", $"HADM_ID", $"icustay_id",
            window($"starttime", "1 hour"))
            .agg(count($"vasonum") as "intervention_count")
            .withColumn("windowstart", to_timestamp($"window.start"))
            .withColumn("windowend", to_timestamp($"window.end"))

          val df_final = df1.select("SUBJECT_ID","HADM_ID","icustay_id","windowstart", "windowend", "intervention_count")

//          df_final.show(5)

        df_final.as[Intervention].rdd
      }
      else {
        val df = spark.read.format("bigquery")
          .option("table", "bdh6250-380417.MIMIC_Extract.icu_ventilator_dur")
          .option("readDataFormat", "AVRO")
          .load()
          .na.drop()
          .withColumn("starttime", to_timestamp($"starttime"))
          .withColumn("endtime", to_timestamp($"endtime"))

        val df1 = df.groupBy($"SUBJECT_ID", $"HADM_ID", $"icustay_id",
          window($"starttime", "1 hour"))
          .agg(count($"ventnum") as "intervention_count")
          .withColumn("windowstart", to_timestamp($"window.start"))
          .withColumn("windowend", to_timestamp($"window.end"))

        val df_final = df1.select("SUBJECT_ID", "HADM_ID", "icustay_id", "windowstart", "windowend", "intervention_count")

        //          df_final.show(5)

        df_final.as[Intervention].rdd
      }

  }
  def to_csv(df: DataFrame, name: String): Unit = {
    //write an output to csv
    df.write.option("header",true).csv(s"resource_data/outputs/$name")
  }

}
