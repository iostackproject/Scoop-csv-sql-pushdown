# scoop-csv-sql-pushdown
This class extends the CSV data source of Spark to pushdown SQL selections and projections.

The base version for this modified class is spark-csv 1.2
Note that by now spark-csv is no longer a library of databrics but an integral part of the spark code.

The base version of CsvRelation comes with a single implementation of the buildScan API which does not support any pushdown (neither columns nor rows).
The basic modification of this version consists at adding the richer buildScan API with comes with the two arguments permitting to pushdown column projection and row selection.

Following are the instructions to build the spark-csv package:
1. git clone https://github.com/databricks/spark-csv
2. cd spark-csv
3. git checkout tags/v1.2.0
4. git checkout -b "my-v1.2.0"
5. replace the file src/main/scala/com/databricks/spark/csv/CsvRelation.scala with pushdown modified CsvRelation.scala

Install the SBT tool to build the Spark CSV library by deploying the SBT tool:
tar -xvf sbt-0.13.9.tgz

Build the Spark CSV libraries:
sbt/bin/sbt ++2.10.4 publish-m2

Important note:
This modified Spark CSV library is build thru SBT whereas the Spark is build with the maven tool.
The way to build both the Spark and the Spark CSV library is to proceed in this order:
1. Build the Spark code (with maven)
2. Build the Spark CSV library as explained previously
3. Rebuild the spark code while taking into account the modified Spark CSV library
e.g.,  mvn -Pyarn -Phadoop-2.7.1 -Dhadoop.version=2.7.1 -DskipTests package


