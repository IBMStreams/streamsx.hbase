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

This is tested with HBase 0.94.3 and Hadoop 1.1.0.  

## Setup

To run these operators, you must install Apache HBASE (https://hbase.apache.org/).  Apache HBASE usually uses Apache Hadoop (http://hadoop.apache.org/) and Apache Zookeeper (http://zookeeper.apache.org/).  An installation of IBM's BigInsights includes everything you need.  

The SPLDoc for this toolkit includes some information on getting started once you have HBASE and HADOOP http://ibmstreams.github.io/streamsx.hbase/com.ibm.streamsx.hbase/doc/spldoc/html/index.html

Please see the individual product pages for instructions on installation.  

##Configuration

To run these operators, you must set `HBASE_HOME` and `HADOOP_HOME` in your environment.  Furthermore, `HBASE_HOME/conf/hbase-site.xml` should be correspond to a running hbase instance (eg, the zookeeper referenced there should be running) since that is what the operator uses to configure itself.   The operator does not need to run on the same host as HBASE.  If you are running Streams on an HBASE host, just set HBASE_HOME to point the the directory for HBASE (ie, the directory that is the parent of the bin directory the conf directory).  

If you are running Streams on a different host than where you ran HBASE, copy that whole directory to the Streams host.  
##Contributing
This repository is using the fork-and-pull model (https://help.github.com/articles/using-pull-requests).  If you'd like to contribute code, fork a copy of the repository, make changes, and when ready, issue a pull request.  For more details, see the wiki in the IBMStreams/toolkits repository.
