package bdh_mimic.model

import java.sql.Date

case class queryResult_test(ICUSTAY_ID: String, SUBJECT_ID: String, HADM_ID: String, icd9_codes: Seq[String])

case class dop_dur(SUBJECT_ID: Long, HADM_ID: Long, icustay_id: Long, vasonum: Long, starttime: String, endtime: String)