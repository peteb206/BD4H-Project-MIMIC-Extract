package bdh_mimic.utils

import bdh_mimic.model.{Events, Items, PatientStatic, ValRange}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast

object utils {

  def outlier_removal(spark: SparkSession, items: RDD[Items], icu_chart: RDD[Events]
                     ): RDD[Events] = {
    //Fnc to remove and impute outliers
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

//    outlier_ranges.cache()

    val ranges_seq = outlier_ranges.map(x => x.LEVEL2).collect().toSeq

    val items_seq = items.map(x => x.LABEL).collect().toSeq

    val items_filter = items.filter(x => ranges_seq.contains(x.LABEL))

    val items_map = items_filter.map(row => (row.LABEL, row.ITEMID)).collect().toMap

    val outlier_ranges_filter = outlier_ranges.filter(x => items_seq.contains(x.LEVEL2))

    val valid_ranges = outlier_ranges_filter.map( x => (items_map.get(x.LEVEL2).get, x.VALID_LOW, x.VALID_HIGH, x.IMPUTE))

    val range_broadcast = sc.broadcast(valid_ranges.collect())

    val result = icu_chart.map { event =>
      val valid_range = range_broadcast.value.filter { case (id, _, _, _) => id == event.ITEMID }
      if (valid_range.nonEmpty) {
        val (_, vallow, valhigh, impute) = valid_range.head
        if (event.VALUE < vallow || event.VALUE > valhigh) {
          event.copy(VALUE = impute)
        } else {
          event
        }
      } else {
        event
      }
    }

    //Errors with null when collecting
//    icu_chart.filter(x => x.VALUE != null).filter(x => x.ITEMID == 226707 && x.VALUE > 240).take(1).foreach(println)
//
//    result.filter(x => x.VALUE != null).filter(x => x.ITEMID == 226707 && x.VALUE == 170).take(1).foreach(println)

    result
  }

}
