This right here is the SPARK Hello World

. SBT Structure
. SBT with Saprk Depencencies
. .scala file with Spark imports
. .scala file with Spark Basic Functions
 
Hello World from SPARK Tutorial
https://medium.com/luckspark/scala-spark-tutorial-1-hello-world-7e66747faec
and
SPARK QUICK START
https://spark.apache.org/docs/2.3.0/quick-start.html


!If its request ClassSpecified :

Ex structure= foo-build2\src\main\scala\example\Hello.jar  (Where the object is hello) 
Then...
spark-submit --class example.hello

Ex2 structure= foo-build2\src\main\scala\Hello.jar  (Where the object is goodbye)

spark-submit --class goodbye

spark-submit  --deploy-mode client --class example.hello gs://bkt-scd-spark-1/hello_2.12-0.1.0-SNAPSHOT.jar


FINAL COMAND
spark-submit  --deploy-mode client --class example.hello --jars /usr/lib/delta/jars/delta-core.jar gs://bkt-scd-spark-1/deltaSpark.jar