ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "BD4H-Project-MIMIC-Extract"
  )

lazy val launchSettings = Seq(
  // set the main class for 'sbt run'
  mainClass in (Compile, run) := Some("bdh_mimic.main.Main")
)

libraryDependencies ++= Seq(
  // If you use organization %% moduleName % version rather than organization % moduleName % version (the difference is the double %% after the organization), sbt will add your project’s binary Scala version to the artifact name.
  // https://www.scala-sbt.org/1.x/docs/Library-Dependencies.html
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  // https://github.com/GoogleCloudDataproc/spark-bigquery-connector
  "com.google.cloud.spark" % "spark-bigquery-with-dependencies_2.12" % "0.29.0",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.2",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2",
  "org.apache.hadoop" % "hadoop-common" % "3.3.2"
)
