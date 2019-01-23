# Assignment0
This contains the 0th assignment of the SSDS course of IISc (DS256) taught by Dr Yogesh Simmhan

How to execute?

Go to the directory containing pom.xml & run
	* mvn clean compile package

then check the target directory which can be found at => /ds256-a0/Code/target
Find the following jar => Assignment0-1.0-SNAPSHOT-jar-with-dependencies.jar

To run use the following command (must be in the directory where you have spark-submit )

	* ./spark-submit --class in.ds256.Assignment0.InterGraph --master yarn --num-executors 2 --deploy-mode cluster --driver-memory 512m --executor-memory 1G --executor-cores 2 <path to jat>/Assignment0-1.0-SNAPSHOT-jar-with-dependencies.jar command_line_arguments


* The output for FreqTag.java will be stored at => outputFile/keys" & outputFile/values (outputFile is provided as command line argument)
* The output for TopOccurrence.java will be stored at => outputFile+"/keys" & outputFile/values (outputFile is provided as command line argument)
* The output for InterGraph.java will be stored at => vertexFile & edgeFile (both provided as command line arguement)

All outputs will be stored in the hdfs


