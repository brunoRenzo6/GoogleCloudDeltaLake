# Running Delta Lake on Google Cloud

 Delta Lake is now available in the latest version of Cloud Dataproc (version 1.5 Preview).
 
 ```shell
$ spark-shell --packages io.delta:delta-core_2.12:0.4.0
OR
$ spark-shell --jars /usr/lib/delta/jars/delta-core.jar
```

# MarketPlace Implementation

MarketPlace use case


![](imgs/rawCSV.PNG)

### Upload files to Google Cloud Storage
Use Cloud Storage bucket to store data.
https://cloud.google.com/storage/docs/uploading-objects#:~:text=Drag%20and%20drop%20the%20desired,that%20appears%2C%20and%20click%20Open.

### Apply Delta Table Format
* Create Dataproc cluster
* Submit a spark job to save these raw csv files now using Delta Table Format

 ```shell
$ spark-submit  --deploy-mode client --class example.hello --jars /usr/lib/delta/jars/delta-core.jar gs://bkt-scd-spark-1/deltaSpark.jar
```
[csvToDelta.scala](csvToDelta/csvToDelta.scala)
