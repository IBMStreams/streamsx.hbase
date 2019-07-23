# How to connect to several HBASE databases from one SPL application

With the new version of **streamsx.hbase** toolkit (version 3.6.0 or higher ) it is possible to connect to several HBase databases from one IBM stream SPL application.

https://github.com/IBMStreams/streamsx.hbase/releases

It is possible to read data from one HBase database and write them into other HBase database.

Or it is possible to split the incoming streams in two or  **n**  streams and write every stream in a different HBase database.

### Preparation

The following SPL sample demonstrates how to connect from one IBM stream application to two different HBase servers.   

Before you start with the SPL application, please perform the following steps:


1. Add the IP Address and the host names of both Hadoop clusters into ```/etc/hosts``` file of stream server.
1. Copy the **hbase-site-xml** file from Hadoop Cluster 1 into **etc** directory of your SPL project and rename it to:<br/>
   ```etc/hbase-site-1.xml```
1. Copy the **hbase-site-xml** file from Hadoop Cluster 2 into **etc** directory of your SPL project and rename it to: <br/>
   ```etc/hbase-site-2.xml```
1. Copy the hbase-user keytab from Hadoop Cluster 1 into **etc** directory of your SPL project and rename it to: <br/>
   ```etc/hbase.headless-1.keytab```
1. Copy the hbase-user keytab from Hadoop Cluster 2 into **etc** directory of your SPL project and rename it to:<br/>
   ```etc/hbase.headless-2.keytab```
1. Change the name of principals with your HBase principals in SPL file. 
1. Copy the kerberos configuration file from your kerberos server in the main etc directory of your stream server. <br/> 
```/etc/krb5.conf``` 
1. Check the HBase keytabs with kinit tool.<br />
    For example:<br />
    `kinit -k -t etc/hbase.headless-1.keytab  hbase-cluster1@HDP2.COM`<br/>
    `kinit -k -t etc/hbase.headless-2.keytab  hbase-cluster2@HDP2.COM`

1. Create a test table on both HBASE databases:<br/>
`hbase shell`<br/> 
`create 'test_table' , all'`
 
In some configuration the **Kerberos key distribution centre** is installed on the same Hadoop server.<br/>
Make sure that the both Hadoop server have the same **realm** as kerberos configuration.

The **ambari** creates for two Hadoop servers separate keytab files with the same name.<br/>
For example: <br/>
```/etc/security/keytabs/hbase.headless.keytab```<br/>
But they are different keytab files and have different principals.

More details in :
https://github.com/IBMStreams/streamsx.hbase/wiki/How-to-use-kerberos-authentication-in-streamsx.hbase-toolkit

### SPL sample
Here is the SPL sample:
```
/*
This SPL application demonstrates how to put the input stream in two or several HBase databases.
It is also possible to put one row at the same time in two different HBase databases.
The custom operator splits the input stream in two streams.
The HBaseSink_1 operator writes the input1 stream into HBase database 1 (hbaseSite :  "etc/hbase-site-1.xml";)
The HBaseSink_2 operator writes the input2 stream into HBase database 2 (hbaseSite :  "etc/hbase-site-2.xml";)
*/
namespace application ;

use com.ibm.streamsx.hbase::HBASEPut ;

composite HbaseTest {
param
    expression<rstring> $authKeytab1 : getSubmissionTimeValue("authKeytab1", "etc/hbase.headless-1.keytab");
    expression<rstring> $authPrincipal1 : getSubmissionTimeValue("authPrincipal1", "hbase-cluster1@HDP2.COM");
    expression<rstring> $authKeytab2 : getSubmissionTimeValue("authKeytab2", "etc/hbase.headless-2.keytab");
    expression<rstring> $authPrincipal2 : getSubmissionTimeValue("authPrincipal2", "hbase-cluster2@HDP2.COM");
type
    HbasePut = rstring table, rstring key, rstring value, rstring hbaseSite ;
graph

// read input files
// The csv file (data/input.csv) has in the last column the name of HBase cluster 
// #table, key, value, hbaseSite
// test_table, key1, value1, hbase1
// test_table, key2, value2, hbase2
// test_table, key3, value2, Both

    stream<HbasePut> readInputs = FileSource(){
        param
            file : "input.csv" ;
            format : csv ;
    }

    // This custom operator splits the input streams in two separate streams (conditional to the value of "hbaseSite")
    (stream<HbasePut> Input_1 ; stream<HbasePut> Input_2)= Custom(readInputs as I){
        logic
        onTuple readInputs : {
            printStringLn((rstring)readInputs);
            if(hbaseSite == "hbase1"){
                submit(I, Input_1);
            }

            else if(hbaseSite == "hbase2"){
                submit(I, Input_2);
            }

            else if(hbaseSite == "Both"){
                submit(I, Input_1);
                submit(I, Input_2);
            }

            else {
                printStringLn("hbaseSite parameter should be hbase1 or hbase2 or Both");
            }

        }

    }

    ()as HBaseSink_1 = HBaseSink(Input_1){
        param
            authKeytab : $authKeytab1 ;
            authPrincipal : $authPrincipal1 ;
            hbaseSite : "etc/hbase-site-1.xml" ;
    }

    ()as HBaseSink_2 = HBaseSink(Input_2){
        param
            authKeytab : $authKeytab2 ;
            authPrincipal : $authPrincipal2 ;
            hbaseSite : "etc/hbase-site-2.xml" ;
    }

    /*

    // It is also possibe to write the data into 3. or 4. HBase cluster 

       () as ANOTHER_sndHBaseSin = HBaseSink(input3){
        param
            authKeytab     : $authKeytab3;
            authPrincipal     : $authPrincipal3; 
            hbaseSite     : "etc/hbase-site-3.xml";
        }
    */
}


```

----------------------------
The SPL code of **HBaseSink composite** 
```
/*
//  This composite is a wrapper for HBASEPut operator.
//  The HBAS operators needs at least 3 parameters to connect to a HBase server. 
//  1- hbaseSite: The HBase configuration file hbase-site.xml.   
//  2- authKeytab : A keytab is a file containing pairs of Kerberos principals 
//     and encrypted keys that are derived from the Kerberos password.
//  3- authPrincipal: A Kerberos principal for keytab .
//  */

namespace application ;
use com.ibm.streamsx.hbase::HBASEPut ;

public composite HBaseSink(input Data ) {
    param
        expression<rstring> $authKeytab ;
        expression<rstring> $authPrincipal ;
        expression<rstring> $hbaseSite ;
    graph

        // prints input data
        ()as printData = Custom(Data){
            logic
                onTuple Data : printStringLn((rstring)Data);
        }

        // puts tuples from input data into a HBase table                 
        (stream<boolean success> putToHbase ; stream<rstring errorText> Error)= HBASEPut(Data){
            param
                authPrincipal : $authPrincipal ;
                authKeytab : $authKeytab ;
                hbaseSite : $hbaseSite ;
                tableNameAttribute : table ;
                rowAttrName : "key" ;
                valueAttrName : "value" ;
                staticColumnFamily : "all" ;
                staticColumnQualifier : "all" ;
                successAttr : "success" ;
                vmArg :"-Djava.security.krb5.conf=/etc/krb5.conf";
        }

        // print the value of success attribute 
        ()as printPutToHbase = Custom(putToHbase){
            logic
                onTuple putToHbase : printStringLn((rstring)putToHbase);
        }
        
        // prints error messages in case of any error during the put action
        ()as printError = Custom(Error){
            logic
                onTuple Error : printStringLn((rstring)Error);
        }
    }
```



