streamsx.hbase
==============

This toolkit allows Streams to write tuples into HBASE and to read tuples from Apache HBASE (https://hbase.apache.org/). To use it, you set HADOOP_HOME and HBASE_HOME, and it picks up the HBASE configuration from there.  Web page with SPLDoc for operators and samples: http://ibmstreams.github.io/streamsx.hbase/

It uses the operator parameter values to determine what's a row, columnFamily, columnQualifier, value, and so on. It includes:
*    HBASEPut, including checkAndPut support
*    HBASEGet
*    HBASEDelete, including checkAndDelete support
*    HBASEIncrement
*    HBASEScan

It includes at least one sample for each of those operators.

This is tested with HBase 0.94.3 and Hadoop 1.1.0, but is expected to work for any later version of hadoop or HBASE.  

## Setup

To run these operators, you must install Apache HBASE (https://hbase.apache.org/).  Apache HBASE usually uses Apache Hadoop (http://hadoop.apache.org/) and Apache Zookeeper (http://zookeeper.apache.org/).  An installation of IBM's BigInsights includes everything you need.  

The SPLDoc for this toolkit includes some information on getting started once you have HBASE and HADOOP http://ibmstreams.github.io/streamsx.hbase/com.ibm.streamsx.hbase/doc/spldoc/html/index.html

Please see the individual product pages for instructions on installation. 

## Building the toolkit

The toolkit uses maven (http://maven.apache.org/). Maven will d 
* Set M2_HOME to point to the maven directory.
* Pick the correct pom file for your install.  In com.ibm.streamsx.hbase, there are three example pom files: 
      * pom-v094.xml: HBASE 0.94, hadoop 1
      * pom-v096-hadoop1.xml, HBASE 0.96, hadoop 1
      * pom-v096-hadoop2.xml, HBASE 0.96, hadoop 2
  copy the correct file for your HBASE and Hadoop install to pom.xml
* run ant at the top level.  This will build the toolkit, but also download all the necessary jars into opt/downloaded.  These jars are used at toolkit build time, but also at the toolkit runtime.

##Configuration

These operators need HBASE configuration information in order to run.  It uses `hbase-site.xml` to do that.   You can supply that in two ways:
* You can set `HBASE_HOME`, the operator will look under `HBASE_HOME/conf/hbase-site.xml` for HBASE configuration information.  This is probably the easiest thing to do if the operator is running on the HBASE host.  
* You can copy hbase-site.xml from your HBASE install's conf directory and then use `hbaseSite` parameter to point to `hbase-site.xml`.  You still need to set `HBASE_HOME`, but it need not point to anything, ie, `export HBASE_HOME=/dev/null`.

## Getting started

One you are started, look under samples at  
* PutSample
* PutRecord
* GetSample
* GetRecord

## Troubleshooting

If you run into trouble at any point, please enter an issue on GitHub.  

##Contributing
This repository is using the fork-and-pull model (https://help.github.com/articles/using-pull-requests).  If you'd like to contribute code, fork a copy of the repository, make changes, and when ready, issue a pull request.  For more details, see the wiki in the IBMStreams/toolkits repository.
