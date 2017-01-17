# Spark SQL pushdown code

# Repository goal:  permit to build the Spark SQL pushdown code


This repository comprises the code and instructions to build the following components:
spark-csv
spark
hadoop
so that Spark SQL pushdown can be invoked against a Swift object store

Important note: Spark SQL pushdown is not standalone.  In order to work the following is nessessary at the swift object store
1. the storlet middleware was installed (see https://github.com/openstack/storlets)
2. the "CSV Storlet was deployed and installed (see https://github.com/openstack/storlets/tree/master/StorletSamples/java/CsvStorlet ).

# How to build the Spark SQL pushdown code:

The Spark SQL pushdown code is build by cloning, patching and building the spark-csv, spark and hadoop projects.
The BuildPushdownSpark.sh automates these tasks and will necessitate the three patch files included in this directory.
For reference, we also included the modified java files of the three projects. They are not needed for the building the projects.

The prerequisites for invoking BuildPushdownSpark.sh are:
1. git 
2. sbt version 0.13.9
3. apache maven version 3.3.9
4. protobuf version 2.5.0  ### beware that earlier or later version will probably not do the job


# scoop-csv-sql-pushdown
This class extends the CSV data source of Spark to pushdown SQL selections and projections.

The base version for this modified class is spark-csv 1.2
Note that by now spark-csv is no longer a library of databrics but an integral part of the spark code.

The base version of CsvRelation comes with a single implementation of the buildScan API which does not support any pushdown (neither columns nor rows).
The basic modification of this version consists at adding the richer buildScan API with comes with the two arguments permitting to pushdown column projection and row selection.



