/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.ibm.gsk.ikeyman.keystore.entry.Entry;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.Parameter;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;

/**
 * Scan an HBASE table and output the specified tuples.
 * 
 */
@PrimitiveOperator(name="HBASEScan", namespace="com.ibm.streamsx.hbase",
description="Scan an HBASE table.  Each row/columnFamily/columnQualifer/value entry is mapped to a tuple, with the row populating the row attribute, the columnFamily populating teh columnFamily attribute, the columnQualifier attribute populating the columnQualifier attribute, and the value populating the value attribute.  The value may either be a long or a string, all other values must be rstring.  An optional start row and end row may be specified.")
@OutputPorts({@OutputPortSet(description="Tuples found", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class HBASEScan extends HBASEOperator{

	/*
	 * Used to describe the way tuples will be populated.  It is set at initialization time, and then used on process
	 * to determine what to do.  
	 */
	private enum OutputMode {
		TUPLES,
		RECORD
	}
	
	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */
    private Thread processThread;
    
    private OutputMode outputMode= null;
    protected String startRow = null;
    protected String endRow = null;
    static final String START_ROW_PARAM="startRow";
    static final String END_ROW_PARAM="endRow";
    
    private int outRow = -1;
    private int outColumnF = -1;
    private int outColumnQ = -1;
    private OutputMapper outValue = null;
    
    private String outAttrName = "value";
    private int outAttrIndex = -1;
    
    private String resultCountAttrName = null;
    private int resultCountIndex = -1;
    
    // Used only in record mode
    private Set<String> recordNames = null;
    private StreamSchema recordSchema = null;
    
    @Parameter(name=HBASEGet.SUCCESS_PARAM_NAME,optional=true,description=
    		"Output attribute in which to put the number of results found.  When the result is a tuple, "+
    		"is the number attributes in that tuple that were populated.")
    public void setResultCountName(String name) {
    	resultCountAttrName = name;
    }
    
    @Parameter(name=START_ROW_PARAM,optional=true,description="Row to use to start the scan (inclusive)")
    public void setStartRow(String row) {
    	startRow = row;
    }
    
    @Parameter(name=END_ROW_PARAM,optional=true,description="Row to use to stop the scan (exclusive)")
    public void setEndRow(String row) {
    	endRow = row;
    }
    
    @Parameter(name=HBASEGet.OUT_PARAM_NAME,optional=true,description="Name of the attribute in which to put the value."
    			+"Defaults to value.  If it is a tuple type, the attribute names are used as columnQualifiers"
    			+ "if multiple families are included in the scan, and they have the ame columnQualifiers, there is no "
    			+ "way of knowing which columnFamily was used to populate a tuple attribute")
    public void setOutAttrName(String name) {
    	outAttrName = name;
    }
    
    @ContextCheck(compile=true)
    public static void checks(OperatorContextChecker checker) {
    	checker.checkDependentParameters("endRow", "startRow");
    }
    
    /**
     * Process parameters and get setup of scan.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        processThread = getOperatorContext().getThreadFactory().newThread(
                new Runnable() {

                    @Override
                    public void run() {
                        try {
                            produceTuples();
                        } catch (Exception e) {
                            Logger.getLogger(this.getClass()).error("Operator error", e);
                        }                    
                    }
                    
                });
        
        /*
         * Set the thread not to be a daemon to ensure that the SPL runtime
         * will wait for the thread to complete before determining the
         * operator is complete.
         */
        processThread.setDaemon(false);
        
        // Now check that the output is hte proper format.
        StreamingOutput<OutputTuple> output = getOutput(0);
        StreamSchema outSchema = output.getStreamSchema();
    
        if (resultCountAttrName != null) {
        	resultCountIndex = checkAndGetIndex(outSchema,resultCountAttrName,true) ;
        }
		Attribute outAttr = outSchema.getAttribute(outAttrName);
		if (outAttr == null) {
				throw new Exception("Expected attribute "+outAttrName+" to be present, but not found");
		}
        outAttrIndex = outAttr.getIndex();
        outRow = checkAndGetIndex(outSchema,"row",false);
        outColumnF = checkAndGetIndex(outSchema,"columnFamily",false);
        outColumnQ = checkAndGetIndex(outSchema,"columnQualifier",false);
        if (outAttr.getType().getMetaType() == MetaType.TUPLE) {
        	outputMode = OutputMode.RECORD;
        	TupleType temp = ((TupleType)outSchema.getAttribute(outAttrIndex).getType());
        	recordSchema = temp.getTupleSchema();
        	recordNames =recordSchema.getAttributeNames();
        }
        else {
        	outputMode = OutputMode.TUPLES;
        	outValue = new OutputMapper(outSchema, outAttrName,charset);
        }
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    	// Start a thread for producing tuples because operator 
    	// implementations must not block and must return control to the caller.
        processThread.start();
    }
    
    /**
     * Submit new tuples to the output stream
     * @throws Exception if an error occurs while submitting a tuple
     */
    private void produceTuples() throws Exception  {
        final StreamingOutput<OutputTuple> out = getOutput(0);
        
        Scan myScan;
        
        if (startRow != null && endRow != null) {
        	myScan = new Scan(startRow.getBytes(charset),endRow.getBytes(charset));
        }
        else if (startRow != null)  {
        	myScan = new Scan(startRow.getBytes(charset));
        }
	else if (endRow != null) {
	    myScan = new Scan(endRow.getBytes(charset));
	}
        else {
        	myScan = new Scan();
        }
        
        if (staticColumnFamilyList != null && staticColumnQualifierList != null) {
        	
        	for (String fam: staticColumnFamilyList) {
        		for (String qual: staticColumnQualifierList) {
        			myScan.addColumn(fam.getBytes(charset), qual.getBytes(charset));
        		}
        	}
        }
        else if (staticColumnFamilyList != null) {
        	for (String fam: staticColumnFamilyList) {
        		myScan.addFamily(fam.getBytes(charset));
        	}
        }
        
        ResultScanner results = myTable.getScanner(myScan);
       
        Result currRow = results.next();
        while (currRow != null) {
        String row = new String(currRow.getRow(),charset);
        NavigableMap<byte[],NavigableMap<byte[],byte[]>> allValues = currRow.getNoVersionMap();
        if (OutputMode.TUPLES == outputMode) {
        for (byte[] family: allValues.keySet()) {
        	for (byte [] qual: allValues.get(family).keySet()) {
        		OutputTuple tuple = out.newTuple();
        		
        		if (outRow >= 0)
        			tuple.setString(outRow,row);
        		if (outColumnF >= 0)
			    tuple.setString(outColumnF,new String(family,charset));
        		
        		if (outColumnQ >= 0)
			    tuple.setString(outColumnQ,new String(qual,charset));
        		
        		outValue.populate(tuple,allValues.get(family).get(qual));
        		if (resultCountIndex >=0) {
        			tuple.setInt(resultCountIndex,1);
        		}
                out.submit(tuple);
        	}
        }
        }
        else if (OutputMode.RECORD == outputMode) {
        	Map<String,RString> fields = null;
        	for (byte[] family: allValues.keySet()) {
        	Map<String,RString>	 tmpMap = extractRStrings(recordNames,allValues.get(family));
        	if (fields == null) {
        		fields = tmpMap;
        	}
        	else {
        		fields.putAll(tmpMap);
        	}
        	}
        	OutputTuple tuple = out.newTuple();
        	tuple.setTuple(outAttrIndex, recordSchema.getTuple(fields));
    		if (outRow >= 0)
    			tuple.setString(outRow,row);
        	if (resultCountIndex >=0) {
        		tuple.setInt(resultCountIndex, fields.size());
        	}
        	out.submit(tuple);
        }
        if (OutputMode.TUPLES == outputMode) {
        	out.punctuate(Punctuation.WINDOW_MARKER);
        }
        currRow = results.next();
        }
     
        results.close();
        
        out.punctuate(Punctuation.FINAL_MARKER);
    }

    /**
     * Shutdown this operator, which will interrupt the thread
     * executing the <code>produceTuples()</code> method.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        if (processThread != null) {
            processThread.interrupt();
            processThread = null;
        }

        // Must call super.shutdown()
        super.shutdown();
    }
}
