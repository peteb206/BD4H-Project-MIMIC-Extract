package bdh_mimic.utils

import bdh_mimic.model.{Events, Items, PatientStatic}
import org.apache.spark.rdd.RDD

object utils {

  def unit_conversion(icu_chart: RDD[Events], items: RDD[Items]): RDD[Events] = {

    val item_map = items.map(row => (row.ITEMID -> row.UNITNAME)).collectAsMap()

    item_map.take(1).foreach(println)

    icu_chart
  }

}
