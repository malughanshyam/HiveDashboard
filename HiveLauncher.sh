#!/bin/bash
if [ $# != 1 ]
then
    echo "Incorrect number of arguments !"
    #echo "Usage : $0 hiveUserName hiveHost dbName hiveQueryFile"
    echo "Usage : $0 hiveQueryFile"
    echo
    exit 1
fi

if [ ! -f $4 ]
then
   echo "File $4 doesn't exist!"
   exit 1
fi

if [ ! -r $4 ]
then
   echo "$4 is not readable"
   exit 1
fi 

# Parameters
hiveHost="172.16.226.129:10000";
dbName="default";
hiveUser="hive";
jobID="$hiveUser`date '+%Y%m%d%H%M%S'`" 	

HIVE_JARS=./HiveJDBCjars 
 
for i in ${HIVE_JARS}/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

java -cp $CLASSPATH HiveExecutor $jobID $hiveUser $hiveHost $dbName $1

#java -cp $CLASSPATH HiveExecutor $1 $2 $3 $4
