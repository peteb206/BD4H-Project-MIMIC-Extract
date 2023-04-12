package bdh_mimic.utils

import bdh_mimic.model.{Events, Items, PatientStatic, ValRange, ItemMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object utils {

  def outlier_removal(spark: SparkSession, items: RDD[Items], icu_chart: RDD[Events]
                     ): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    //https://www.nature.com/articles/sdata201635 Nice Chart of MIMIC Data

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

    outlier_ranges.cache()

    val ranges_seq = outlier_ranges.map(x => x.LEVEL2).collect().toSeq

    val items_seq = items.map(x => x.LABEL).collect().toSeq

    val items_filter = items.filter(x => ranges_seq.contains(x.LABEL))

    val items_map = items_filter.map(row => (row.LABEL, row.ITEMID)).collect().toMap

    val outlier_ranges_filter = outlier_ranges.filter(x => items_seq.contains(x.LEVEL2))

    val valid_ranges: RDD[ItemMap] = outlier_ranges_filter.map( x => (items_map.get(x.LEVEL2).get, x.VALID_LOW, x.VALID_HIGH, x.IMPUTE))

//    valid_ranges.take(1).foreach(println)

    //https://stackoverflow.com/questions/43345703/combine-two-rdds spark sql maybe cleaner here

    //we can load with context; data is structured filter and use sql
    val range_item_seq = valid_ranges.map(x => x.ITEMID).collect().toSeq

    val icu_chart_filter = icu_chart.filter(x => range_item_seq.contains(x.ITEMID))

//    val icu_chart_nofilter = icu_chart.filter(x => !(range_item_seq.contains(x.ITEMID)))

//    icu_chart_filter.take(1).foreach(println)

//    val icu_chart_outlier = icu_chart_filter.map(x => (x.ITEMID, Seq(x.SUBJECT_ID, x.HADM_ID, x.ICUSTAY_ID, x.CHARTTIME,
//      x.ITEMID, x.VALUE, x.VALUEUOM))).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7)))

//    val outlier_chart = icu_chart_outlier.join(valid_ranges)
//
//    outlier_chart.take(1).foreach(println)

    val rangedf = valid_ranges.toDF("ItemID", "ValidLow", "ValidHigh","Impute")

//    val icudf = icu_chart_filter.toDF("Subject","adminID","ICUID","ChartTime","ItemID",
//      "VALUE", "VALUEUOM")
//
//    icudf.show()

    rangedf.show()

    items_map
  }

}
