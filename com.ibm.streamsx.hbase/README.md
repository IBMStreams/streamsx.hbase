This toolkit is for interacting with Apache HBASE.  It assumes Apache HBASE
is already installed and running.

This toolkit provides Streams operators to work with the HBASE API,
so the operators are named after the HBASE API calls.  There is another
toolkit for HBASE that takes a more database-like approach.

Operators:

**HBASEPut**: Put a tuple into HBASE.  The row and value must be supplied in the tuple.
The columnFamily and columnQualifier can be either from operator parameters or from
the tuple.  To use checkAndPut, the incoming tuple needs to include an attribute
specifying the check to perform.  See the example for details.

**HBASEGet**: Get a tuple from HBASE.  The row must be specified in the input tuple.
The columnFamily and columnQualifier may be in the input tuple, operator parameters,
or unspecified.  The values from HBASE are placed in an attribute of the output
tuple.  That attribute must be either a string, a long, or a map.  See examples for details.

**HBASEDelete**: Delete a tuple from HBASE.  The row must be supplied in the tuple.
The columnFamily and columnQualifier can either be operator parameters or from the tuple.
To use use checkAndDelete, the incoming tuple must include and attribute specifying the
check to perform.  See the example for details.

**HBASEIncrement**: Increment a value in HBASE.  The row must be supplied in the input tuple.
The columnFamily and columnQualifier can be from either the operator parameters or from
the input tuple.  The increment value can be in the tuple, and operator parameter,
or unspecified.  If unspecified, the increment value uses the default. 

**HBASEScan**: Scan a table (ie, get all tuples in the table).  The output can be limited to
specific column families or to a column family, column qualifier pair.  A start row
and an end row may also be specified.

**Kerberos Authentication**
The streamsx.hbase toolkit support from version 3.1.0 kerberos authentication
Kerberos authentication is a network protocol to provide strong authentication for client/server applications.

If not done already, enable Kerberos authentication on your Hadoop cluster using the following links to enable the kerberos authentication.

https://www.cloudera.com/documentation/enterprise/5-7-x/topics/cdh_sg_hbase_authentication.html
https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.2.0/com.ibm.swg.im.infosphere.biginsights.admin.doc/doc/admin_iop_kerberos.html

After enabling the Kerberos authentication, copy the hbase server keytab and hbase configuration file "hbase-site.xml" from hadoop server into your IBM Streams server in a directory and use them in your SPL application.

https://github.com/IBMStreams/streamsx.hbase/wiki/How-to-use-kerberos-authentication-in-streamsx.hbase-toolkit


