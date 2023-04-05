# BD4H-Project-MIMIC-Extract

## About
<b>Georgia Tech
<br>CSE 6250
<br>Spring 2023
<br>William Hoynes and Peter Berryman</b>

Scala project to reproduce the original MIMIC Extract data pipeline using Scala/Spark instead of Python.

## Instructions
1. Request Editor access to the Google Cloud project found here: https://console.cloud.google.com/iam-admin/iam?project=bdh6250-380417
2. Install the Google Cloud CLI using the instructions found here: https://cloud.google.com/sdk/docs/install-sdk.
3. Run the following command from the command line:<br><code>\<path to Google Cloud CLI installation\>/google-cloud-sdk/bin/gcloud auth application-default login</code><br>
4. Choose your Google account with credentials to the project<br>
5. Run the following command from the command line:<br><code>\<path to Google Cloud CLI installation\>/google-cloud-sdk/bin/gcloud config set project bdh6250-380417</code>
6. Run <code>src/main/scala/bdh_mimic/main/Main.scala</code> from this project.