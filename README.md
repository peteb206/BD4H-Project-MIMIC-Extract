# BD4H-Project-MIMIC-Extract

## About
<b>Georgia Tech
<br>CSE 6250
<br>Spring 2023
<br>William Hoynes and Peter Berryman</b>

Based on research paper MIMIC-Extract: A Data Extraction, Preprocessing, and
Representation Pipeline for MIMIC-III By Shirly Wang, Matthew B.A. McDermott, Geeticka Chauhan, 
Marzyeh Ghassemi, Michael C. Hughes, Tristan Naumann and Git Repo: https://github.com/MLforHealth/MIMIC_Extract

Purpose is to use original work as bases for easier downstream manipulation of MIMIC-III datasets using Physionet BigQuery Database in Scala/Spark

Original MIMIC Extract Pipeline

<img src="https://github.com/peteb206/BD4H-Project-MIMIC-Extract/blob/main/resource_data/MIMICExtractPipeline.png" width="500" height="280">

Simplified Updated Physionet Dataset

<img src="https://github.com/peteb206/BD4H-Project-MIMIC-Extract/blob/main/resource_data/MIMICExtractUpdate.png" width="500" height="280">

## Instructions - For Grading TA Only
1. Request Editor access to the Google Cloud project found here: https://console.cloud.google.com/iam-admin/iam?project=bdh6250-380417
2. Install the Google Cloud CLI using the instructions found here: https://cloud.google.com/sdk/docs/install-sdk.
3. Run the following command from the command line:<br><code>\<path to Google Cloud CLI installation\>/google-cloud-sdk/bin/gcloud auth application-default login</code><br>
4. Choose your Google account with credentials to the project<br>
5. Run the following command from the command line:<br><code>\<path to Google Cloud CLI installation\>/google-cloud-sdk/bin/gcloud config set project bdh6250-380417</code>
6. Run <code>src/main/scala/bdh_mimic/main/Main.scala</code> from this project.
## GCP BigQuery
1. Request access to the BigQuery database provided by Physionet (requires credentialed access): https://physionet.org/content/mimiciii/1.4/
2. This will give access to the database used in this pipeline named MIMIC_Extract with corresponding queries

## Instructions - For Other Users
1. Request access to the BigQuery database provided by Physionet (requires credentialed access): https://physionet.org/content/mimiciii/1.4/
2. See file Bigquery.txt for queries to create databases for use in Scala Code
