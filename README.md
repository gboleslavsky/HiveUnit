# HiveUnit
I decided to write this tool after I realized that most Hive developers do not write unit tests for their queries. By unit tests I mean:
- automated tests 
- that developers can run locally 
- to test important new queries 
- and check if they have damaged what worked before while developing something new  
- before committing to a remote repository, to make sure they have not broken the build. 

After looking at tools that are available for hive 1.xx, I did not find any that are easy to use by developers with SQL background. The fact that Hadoop is almost impossible to install on Windows makes it even more difficult.  This framework facilitates Test Driven Development of Hive scripts without any dependencies on local install of any Hadoop software. It is designed to run as a java jar, invoked by shell scripts, one set of scripts per OS. It uses Spark 2.XX to run hql, which creates local HDFS behind the scenes. 
I chose excel as the format for a test specification because it is the easiest way to create typed tabular data and is often used to document test cases, and the input and expected data needed for each test case. There were a couple of reasons I wrote this as a set of independent but composable commands that read and write artifacts on the local file system. Typically big data teams use a variety of technologies, so functionality to compare expected to actual and report on the results should work with any tabular text. Also, some steps in test execution, such as loading data from a spreadsheet to a set of tables, are useful on their own. 
Command level API consists of five commands, and can be trivially extended to support an arbitrary number of commands, each with its own set of parameters. Commands supported so far:

- to_hql -- converts test definitions to hql -- given an .xlsx file formatted as shown in test definition spreadsheets in the data folder, generates a folder on the file system, currently named hql, with a set of folders for each spreadsheet, each folder with its own subfolders, one per statement type, of HQL DDL and DML statements, needed to conduct an automated test. Currently Excel .xlsx format is supported, as well csv. Can be trivially extended to support typed csv as well as any typed or untyped tabular data source for input and expected data.

- load -- given the hql folder created by to_hql (previous command), runs CREATE TABLE and INSERT scripts to load input data

- hive -- runs script or scripts under test, as configured in the conf sheet of the test definition spreadsheet

- assert -- obtains ACTUAL results by running SELECT statements generated by to_hql, compares to EXPECTED results specified in the test definition spreadsheet and generates a report with results of comparison

- clean -- runs TRUNCATE statements generated by to_hql for both input and expected tables to prepare for the next run

Possible enhancements include a tool to mark up a large script so that individual small queries can be defined as scripts under test. This codebase also needs unit tests. I am thinking of rewiriting this in Scala, I'm really curious how much smaller it'll be, and it's a lot easier to maintain since most of the utilities I had to write in java 8 (all of H.java) exist in Scala, either as language features or in commonly used libraries. 
