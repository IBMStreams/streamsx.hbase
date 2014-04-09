streamsx.hbase
==============

This toolkit allows Streams to write tuples into HBASE and to read tuples from HBASE. To use it, you set HADOOP_HOME and HBASE_HOME, and it picks up the HBASE configuarion from there.  Web page with SPLDoc for operators and samples: http://ibmstreams.github.io/streamsx.hbase/

It uses the operator parameter values to determine what's a row, columnFamily, columnQualifier, value, and so on. It includes:
*    HBASEPut, including checkAndPut support
*    HBASEGet
*    HBASEDelete, including checkAndDelete support
*    HBASEIncrement
*    HBASEScan

It includes at least one sample for each of those operators.

This is tested with HBase 0.94.3.

## Configuration

To run these operators, you must set `HBASE_HOME` and `HADOOP_HOME` in your environment.  Furthermore, `HBASE_HOME` must contain a valid `hbase-site.xml`, since that is what the operator uses to configure it self.  However, the operator does not need to run on the same host as HBASE, so long as it has an `hbase-site.xml` and all the HBASE and hadoop libraries.  
