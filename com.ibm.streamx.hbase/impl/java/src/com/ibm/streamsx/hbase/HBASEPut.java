/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.InputPortSet;
import java.util.ArrayList;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Class for an operator that consumes tuples and does not produce an output stream. 
 * 
 */

@PrimitiveOperator(name="HBASEPut", namespace="com.ibm.streamsx.hbase",
description="Put tuples in HBASE, with support for checkAndPut")
@InputPorts({@InputPortSet(description="Tuple to put into HBASE", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Optional port for success or failure information.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving)})

public class HBASEPut extends HBASEPutDelete {

	List<Put> putList;
	
	protected String valueAttr=null;
	final static String VALUE_NAME = "valueAttrName";
	
	@Parameter(name=VALUE_NAME,optional=false)
	public void setValueAttr(String val) {
		valueAttr = val;
	}
	
	Logger logger = Logger.getLogger(this.getClass());
	
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
		if (batchSize > 0) { 
			putList = new ArrayList<Put>(batchSize);
		}
	}

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
    	// This method is commonly used by source operators. 
    	// Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
    	
    	byte row[] = getRow(tuple);
    	byte colF[] = getColumnFamily(tuple);
    	byte colQ[] = getColumnQualifier(tuple);
    	byte value[] = tuple.getString(valueAttr).getBytes();
    	boolean success = false;
    	Put myPut = new Put(row);
    	myPut.add(colF, colQ, value);
    	
    	if (checkAttr != null) {
    		Tuple checkTuple = tuple.getTuple(checkAttrIndex);
    		
    		// the row attribute and the check row attribute have to match, so don't even use it.
    		byte checkRow[] = getRow(tuple);
    		byte checkColF[] = getCheckColF(checkTuple);
    		byte checkColQ[] = getCheckColQ(checkTuple);
    		byte checkValue[] = getCheckValue(checkTuple);
    		success = myTable.checkAndPut(checkRow,checkColF,checkColQ,checkValue,myPut);
    		logger.debug("Result is "+success);
    	}
    	else if (batchSize == 0) {
    		myTable.put(myPut);
    	}
    	else {
    		putList.add(myPut);
    		if (putList.size() == batchSize) {
    			myTable.put(putList);
    			putList.clear();
    		}
    	}
    	// Checks to see if an output tuple is necessary, and if so,
    	// submits it.
    	submitOutputTuple(tuple,success);
    }
    
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// TODO: If window punctuations are meaningful to the external system or data store, 
    	// insert code here to process the incoming punctuation.
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        if (myTable != null && putList != null && putList.size() > 0) {
        	myTable.put(putList);
        }
        super.shutdown();
    }
    
}
