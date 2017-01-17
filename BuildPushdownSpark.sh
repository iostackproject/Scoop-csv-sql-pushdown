#!/bin/bash 

############################################################
#
#  Yosef Moatti 
#
#  Functionallity:
#  Build the Spark SQL pushdown from scratch
#  Following components have to be cloned, patched and built:
#  1. joss (Java written library to access swift, used by stocator)
#  2. stocator (this is the modern driver to access swift from spark
#  3. hadoop 
#  4. spark 
#  5. spark-csv
#
#  Usage: BuildPushdownSpark < --skipClone >
#
#  the --skipClone optional switch will cause the git repositories
#  not to be cloned. The used code is the code
#  that will be found in the various component trees
#
#  For building Hadoop only:
#  cd /opt/apache/pushdown/hadoop; export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M; time mvn -Pyarn -DskipTests install package
#
#  For building Spark only:
#  cd /opt/apache/pushdown/spark; export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"; time mvn -Pyarn -Phadoop-2.7.1 -Dhadoop.version=2.7.1 -DskipTests package
#
#  For building spark-csv (which requires building spark subsequently):
#  export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"; cd /opt/apache/pushdown/spark-csv;  sbt/bin/sbt ++2.10.4 publish-m2; cd ../spark; echo " next step"; time mvn -Pyarn -Phadoop-2.7.1 -Dhadoop.version=2.7.1 -DskipTests package
#
#  Assumptions:  
#
############################################################

E_BADARGS=65  # Exit error code
BASENAME=`basename $0`
VERSION=0.26

### BUILD_DIR is the directory in which hadoop, spark, spark-csv, joss and stocator are built
BUILD_DIR=/opt/apache/pushdown
### PATCHES_DIR is the directory where all the patches were copied
PATCHES_DIR=/SHARED/GIL-PATCH/spark-additions/csv-pushdown
IVY2_BASE=/root/.ivy2/cache

############################################################
# Usage function                                           #
############################################################
# usage ()
# {
#     echo "Usage: " $BASENAME <--skipClone>
# }
############################################################
# Start of the script flow:

if [ $# -ge 1 ]
then
    if [[ $1 = "--skipClone" ]]
    then
	echo "The code trees will not be cloned and patched"
	SKIPCLONE="true"
	shift
    fi
fi

############################################################
# Test success of previous step                            #
############################################################
testSuccess ()
{
    LATEST=$?
    if [ ! "$LATEST" -eq "0" ]
    then
	echo "Value of latest return code is $LATEST"
	exit 1
    fi
}

############################################################
# Test success of previous step                            #
############################################################
runNextStep ()
{
    echo "Next step: $cmdLine ...starting at " `date | gawk '{print $4}'`
    $cmdLine
    testSuccess
    ### next command line helps to catch failures at building the next cmdLine
    cmdLine="echo \"WARNING: next command line failed to be built!\""
}


echo "Pushdown build started with $BASENAME version $VERSION ...started at " `date`

HADOOP_VERSION=2.7.1   # with 2.6.0 the swift test from local spark VM fails
SPARK_VERSION=1.6.1
SPARK_CSV_VERSION=1.2.0
STOCATOR_VERSION=pushdown-v0.4
JOSS_VERSION=pushdown-v0.2

HADOOP_PATCH_NUMBER=0002
SPARK_PATCH_NUMBER=0004   
SPARK_CSV_PATCH_NUMBER=0007

SBT_SUFIX=-0.13.9

HADOOP_PREFIX=release-
SPARK_PREFIX=v
SPARK_CSV_PREFIX=v
STOCATOR_PREFIX=
JOSS_PREFIX=

JOSS_DIR_NAME=pushdown-joss
STOCATOR_DIR_NAME=pushdown-stocator
SPARK_DIR_NAME=spark
SPARK_CSV_DIR_NAME=spark-csv
HADOOP_DIR_NAME=hadoop

echo "Used JDK is "
java -version

IBM=$(java -version 2>&1 | sed -n '/Ibm/I p');
if [ -n "$IBM" ]; then   
    echo "This is an IBM JDK"
else
    echo "This is NOT an IBM JDK"
fi

cd $BUILD_DIR
if [ ! $? -eq "0" ]
then
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
fi
testSuccess

if [ -z "$SKIPCLONE" ]; then   
    DATE=`date | sed 's/ /X/g' | sed 's/:/Y/g' | sed 's/[^0-9a-zA-Z-]*//g'  `

    HADOOP_GIT_SITE=git://git.apache.org/hadoop.git
    SPARK_GIT_SITE=git://git.apache.org/spark.git
    SPARK_CSV_GIT_SITE=https://github.com/databricks/spark-csv
    STOCATOR_GIT_SITE=https://github.com/ymoatti/pushdown-stocator    ####  https://github.com/SparkTC/stocator.git
    JOSS_GIT_SITE=https://github.com/ymoatti/pushdown-joss            ####  https://github.com/javaswift/joss

    HADOOP_PATCH=hadoop-$HADOOP_VERSION-pushdown-$HADOOP_PATCH_NUMBER.patch
    SPARK_PATCH=spark-$SPARK_VERSION-pushdown-$SPARK_PATCH_NUMBER.patch
    SPARK_CSV_PATCH=spark-csv-$SPARK_CSV_VERSION-pushdown-$SPARK_CSV_PATCH_NUMBER.patch

    mkdir -p $BUILD_DIR-$DATE
    rm -f $BUILD_DIR
    ln -s $BUILD_DIR-$DATE $BUILD_DIR

    #### JOSS
    cd $BUILD_DIR
    cmdLine="git clone $JOSS_GIT_SITE"; runNextStep
    cd $JOSS_DIR_NAME
    cmdLine="git checkout tags/$JOSS_PREFIX$JOSS_VERSION"; runNextStep
    cmdLine="git checkout -b "my-"$JOSS_PREFIX$JOSS_VERSION"; runNextStep
    # no patch to apply 

    #### STOCATOR
    cd $BUILD_DIR
    cmdLine="git clone $STOCATOR_GIT_SITE"; runNextStep
    cd $STOCATOR_DIR_NAME
    cmdLine="git checkout tags/$STOCATOR_PREFIX$STOCATOR_VERSION"; runNextStep
    cmdLine="git checkout -b "my-"$STOCATOR_PREFIX$STOCATOR_VERSION"; runNextStep
    # no patch to apply 

    #### hadoop
    cd $BUILD_DIR
    cmdLine="git clone $HADOOP_GIT_SITE"; runNextStep
    cd $HADOOP_DIR_NAME
    cmdLine="git checkout tags/$HADOOP_PREFIX$HADOOP_VERSION"; runNextStep
    cmdLine="git checkout -b "my-"$HADOOP_PREFIX$HADOOP_VERSION"; runNextStep
    cmdLine="git apply $PATCHES_DIR/$HADOOP_PATCH"; runNextStep

    #### SPARK_CSV
    cd $BUILD_DIR
    cmdLine="git clone $SPARK_CSV_GIT_SITE"; runNextStep
    cd $SPARK_CSV_DIR_NAME
    cmdLine="git checkout tags/$SPARK_CSV_PREFIX$SPARK_CSV_VERSION"; runNextStep
    cmdLine="git checkout -b "my-"$SPARK_CSV_PREFIX$SPARK_CSV_VERSION"; runNextStep
    cmdLine="git apply $PATCHES_DIR/$SPARK_CSV_PATCH"; runNextStep
   
    #### SPARK
    cd $BUILD_DIR
    cmdLine="git clone $SPARK_GIT_SITE"; runNextStep
    cd $SPARK_DIR_NAME
    cmdLine="git checkout tags/$SPARK_PREFIX$SPARK_VERSION"; runNextStep
    cmdLine="git checkout -b "my-"$SPARK_PREFIX$SPARK_VERSION"; runNextStep
    cmdLine="git apply $PATCHES_DIR/$SPARK_PATCH"; runNextStep
    cd ..

    if [ -n "$IBM" ]; then   
    	### Fix Hadoop file which causes compilation errors for IBM JDK:
    	echo "We now fix the TestSecureLogins.java for compiling with IBM JDK..."
    	cd $BUILD_DIR/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry/src/test/java/org/apache/hadoop/registry/secure/;
    	cat TestSecureLogins.java | sed 's/import com.sun.security.auth.module.Krb5LoginModule/import com.ibm.security.auth.module.Krb5LoginModule/' > xxyyzz
    	testSuccess
    	mv xxyyzz TestSecureLogins.java
    fi
else
    echo "skiping the clone, checkout and patch steps "
fi

echo "now building the code... "


cd $BUILD_DIR/$JOSS_DIR_NAME
cmdLine="time mvn install"; runNextStep

cd $BUILD_DIR/$STOCATOR_DIR_NAME
#cmdLine="time mvn clean package -Pall-in-one"; runNextStep
cmdLine="time mvn install"; runNextStep

cd $BUILD_DIR/$HADOOP_DIR_NAME
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
cmdLine="time mvn -Pyarn -DskipTests install package"; runNextStep

echo "BUILD_DIR is $BUILD_DIR"
echo "SPARK_DIR_NAME is $SPARK_DIR_NAME"
cd $BUILD_DIR
cd $SPARK_DIR_NAME
pwd
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
cmdLine="time mvn -Pyarn -Phadoop-$HADOOP_VERSION -Dhadoop.version=$HADOOP_VERSION -DskipTests package"; runNextStep

cd $BUILD_DIR
mkdir -p $IVY2_BASE/org.apache.spark/spark-core_2.10/jars
cmdLine="cp $SPARK_DIR_NAME/core/target/spark-core_2.10-$SPARK_VERSION.jar $IVY2_BASE/org.apache.spark/spark-core_2.10/jars"; runNextStep

mkdir -p $IVY2_BASE/org.apache.hadoop/hadoop-openstack/jars
cmdLine="cp $HADOOP_DIR_NAME/hadoop-tools/hadoop-openstack/target/hadoop-openstack-$HADOOP_VERSION.jar $IVY2_BASE/org.apache.hadoop/hadoop-openstack/jars"; runNextStep

mkdir -p $IVY2_BASE/org.apache.hadoop/hadoop-mapreduce-client-core/jars
cmdLine="cp $HADOOP_DIR_NAME/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/target/hadoop-mapreduce-client-core-$HADOOP_VERSION.jar $IVY2_BASE/org.apache.hadoop/hadoop-mapreduce-client-core/jars"; runNextStep

echo "display the latest jars copied to in $IVY2_BASE"
find $IVY2_BASE -name \*.jar -mmin -5 -exec ls -lt {} \;


echo "Next step: build spark-csv based on modified SPARK...starting at " `date | gawk '{print $4}'`
cd $BUILD_DIR/$SPARK_CSV_DIR_NAME
cmdLine="cp -f /SHARED/GIL-PATCH/sbt$SBT_SUFIX.tgz ."; runNextStep
rm -rf sbt 2>/etc/null
tar -xvf sbt$SBT_SUFIX.tgz
cmdLine="sbt/bin/sbt ++2.10.4 publish-m2"; runNextStep

echo "Next step: rebuild spark again using now good spark-csv...starting at " `date | gawk '{print $4}'`
cd $BUILD_DIR/$SPARK_DIR_NAME
cmdLine="time mvn -Pyarn -Phadoop-$HADOOP_VERSION -Dhadoop.version=$HADOOP_VERSION -DskipTests package"; runNextStep

echo "Completed at " `date`
