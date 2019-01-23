# README #

This is the public git repository for Assignment 0 of DS256 - Scalable Systems for Data Science (Jan 2019) course offered in the Computational and Data Science Department at the Indian Institute of Science. Students are expected to fork this repository for their assignment. Instruction for assignment submission are available at http://cds.iisc.ac.in/courses/ds256/

### Directory Structure ###

* Code
	* src/main/scala/
		* FreqTag.scala
		* TopCoOccurrence.scala
		* InterGraph.scala
	* build.sbt
* Logs
	* logs.txt
* Results
	* a0_\*.txt (Contains results for all the questions)
* Project_Report.pdf (To be added by the student)

### Contents of logs.txt file ###
The logs.txt should contain the application ID of the jobs run for each question. The current file is an example that should be modified by the students to contain the appropriate application IDs.
Students are also expected to maintain the output of the command `yarn logs -applicationId <application ID>` for each of the application IDs in the logs.txt file in /home/<-username->/ds256/logs/Assignment0 (this directory is on the linux filesystem and not in hdfs) as a backup in case of any failures.
