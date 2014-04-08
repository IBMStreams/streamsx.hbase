/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.ibm.gsk.ikeyman.keystore.entry.Entry;
import com.ibm.streams.operator.model.Parameter;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Scan an HBASE table and output the specified tuples.
 * 
 */
@PrimitiveOperator(name="HBASEScan", namespace="com.ibm.streamsx.hbase",
description="Scan an HBASE table ")
@OutputPorts({@OutputPortSet(description="Tuples found", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class HBASEScan extends HBASEOperator{

	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */
    private Thread processThread;
    
    protected String startRow = null;
    protected String endRow = null;
    static final String START_ROW_PARAM="startRow";
    static final String END_ROW_PARAM="endRow";
    
    private int outRow = -1;
    private int outColumnF = -1;
    private int outColumnQ = -1;
    private OutputMapper outValue = null;
    
    @Parameter(name=START_ROW_PARAM,optional=true)
    public void setStartRow(String row) {
    	startRow = row;
    }
    
    @Parameter(name=END_ROW_PARAM,optional=true)
    public void setEndRow(String row) {
    	endRow = row;
    }
    // TODO: add check that if endRow is specified, start row also is
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO:
        // If needed, insert code to establish connections or resources to communicate an external system or data store.
        // The configuration information for this may come from parameters supplied to the operator invocation, 
        // or external configuration files or a combination of the two.
        
        /*
         * Create the thread for producing tuples. 
         * The thread is created at initialize time but started.
         * The thread will be started by allPortsReady().
         */
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
        outRow = checkAndGetIndex(outSchema,"row",false);
        outColumnF = checkAndGetIndex(outSchema,"columnFamily",false);
        outColumnQ = checkAndGetIndex(outSchema,"columnQualifier",false);
        outValue = new OutputMapper(outSchema, "value");
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
        	myScan = new Scan(startRow.getBytes(),endRow.getBytes());
        }
        else if (startRow != null)  {
        	myScan = new Scan(startRow.getBytes());
        }
        else {
        	myScan = new Scan();
        }
        
        if (staticColumnFamilyList != null && staticColumnQualifierList != null) {
        	
        	for (String fam: staticColumnFamilyList) {
        		for (String qual: staticColumnQualifierList) {
        			myScan.addColumn(fam.getBytes(), qual.getBytes());
        		}
        	}
        }
        else if (staticColumnFamilyList != null) {
        	for (String fam: staticColumnFamilyList) {
        		myScan.addFamily(fam.getBytes());
        	}
        }
        
        ResultScanner results = myTable.getScanner(myScan);
       
        Result currRow = results.next();
        while (currRow != null) {
        String row = new String(currRow.getRow());
        NavigableMap<byte[],NavigableMap<byte[],byte[]>> allValues = currRow.getNoVersionMap();
        for (byte[] family: allValues.keySet()) {
        	for (byte [] qual: allValues.get(family).keySet()) {
        		OutputTuple tuple = out.newTuple();
        		
        		if (outRow >= 0)
        			tuple.setString(outRow,row);
        		if (outColumnF >= 0)
        			tuple.setString(outColumnF,new String(family));
        		
        		if (outColumnQ >= 0)
        			tuple.setString(outColumnQ,new String(qual));
        		
        		outValue.populate(tuple,allValues.get(family).get(qual));
                out.submit(tuple);
        	}
        }         
        currRow = results.next();
        }
     
        results.close();
        out.punctuate(Punctuation.WINDOW_MARKER);
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
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }
}
