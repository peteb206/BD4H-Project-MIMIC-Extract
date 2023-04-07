package bdh_mimic.model

import java.math.BigInteger
import java.sql.Date
import java.sql.Timestamp

case class queryResult_test(ICUSTAY_ID: String, SUBJECT_ID: String, HADM_ID: String, icd9_codes: Seq[String])

case class PatientStatic(SUBJECT_ID: String, HADM_ID: String, DOB: String, GENDER: String, ADMITTIME: String, DISCHTIME: String,
                         INTIME: String, OUTTIME: String, ADMISSION_TYPE: String, INSURANCE: String, FIRST_CAREUNIT: String,
                         age: Double, age_adjusted: Double, ICUDur: BigInteger, visit: BigInteger)

