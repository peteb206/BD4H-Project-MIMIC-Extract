package bdh_mimic.model

import java.math.BigInteger
import java.sql.Date
import java.sql.Timestamp

case class queryResult_test(ICUSTAY_ID: String, SUBJECT_ID: String, HADM_ID: String, icd9_codes: Seq[String])

case class PatientStatic(SUBJECT_ID: String, HADM_ID: String, DOB: String, GENDER: String, ADMITTIME: Timestamp,
                         DISCHTIME: Timestamp, INTIME: Timestamp, OUTTIME: Timestamp, ADMISSION_TYPE: String,
                         INSURANCE: String, FIRST_CAREUNIT: String, age: Double, age_adjusted: Double,
                         ICUDur: BigInteger, visit: BigInteger)

case class Events(SUBJECT_ID: String, HADM_ID: String,ICUSTAY_ID: String,  CHARTTIME: Timestamp,
                       ITEMID: String, VALUE: Double, VALUEUOM: String)

case class Items(ITEMID: String, LABEL: String, DBSOURCE: String, LINKSTO: String, CATEGORY: String, UNITNAME: String)

case class ValRange(LEVEL2: String, OUTLIER_LOW: Double, VALID_LOW: Double, IMPUTE: Double,
                    VALID_HIGH: Double, OUTLIER_HIGH: Double)

case class ItemMap(ITEMID: String, VALID_LOW: Double, VALID_HIGH: Double, IMPUTE: Double)