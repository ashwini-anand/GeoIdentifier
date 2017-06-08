# GeoIdentifier
This application finds the column having geographical location in a hive table.<br/>

<b>Steps to run this application</b>
1) Login to the system where hive is installed. 
2) set your HIVE_HOME if not already set.<br/>
$ export HIVE_HOME="/usr/hdp/2.6.0.3-8/hive"
3) Put "opennlp-tools-1.8.0.jar" in /root/jars folder
4) Go to src folder of this project and execute following commands<br/>
$ javac -classpath /root/jars/opennlp-tools-1.8.0.jar GeoId1.java <br/>
$ java -classpath $HIVE_HOME/lib/\*:/usr/hdp/2.6.0.3-8/hadoop/\*:/root/jars/opennlp-tools-1.8.0.jar: GeoId1 \<databaseName\> \<tableName\> <br/>
e.g. java -classpath $HIVE_HOME/lib/\*:/usr/hdp/2.6.0.3-8/hadoop/\*:/root/jars/opennlp-tools-1.8.0.jar: GeoId1 default sales

