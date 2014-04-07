/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import org.apache.hadoop.hbase.client.Increment;
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
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Parameter;
/**
 * Class for an operator that receives a tuple and then optionally submits a tuple. 
 * This pattern supports a number of input streams and a single output stream. 
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li> 
 * <li>process() handles a tuple arriving on an input port 
 * <li>processPuncuation() handles a punctuation mark arriving on an input port 
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time, 
 * such as a request to stop a PE or cancel a job. 
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks, 
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other, 
 * which lead to these methods being called concurrently by different threads.</p> 
 */
@PrimitiveOperator(name="HBASEIncrement", namespace="streamsx.bigdata.hbase",
description="Java Operator HBASEIncrement")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
public class HBASEIncrement extends HBASEOperatorWithInput {
	
	String incrAttr = null;
	MetaType incrAttrType = null;
	int incrAttrIndex = -1;
	protected long defaultIncr= 1;
	private static final String INCREMENT_ATTR_PARAM="incrementAttrName";
	private static final String STATIC_INCREMENT_VALUE="increment";
	
	@Parameter(name=INCREMENT_ATTR_PARAM,optional=true)
	public void setIncrAttr(String name) {
		incrAttr=name;
	}
	
	@Parameter(name=STATIC_INCREMENT_VALUE, optional = true) 
	public void setIncr(long _inc){
		defaultIncr = _inc;
	}
	
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
        

        if (incrAttr != null) {
            StreamingInput<Tuple> input = context.getStreamingInputs().get(0);
            StreamSchema inputSchema = input.getStreamSchema();
            Attribute attr = inputSchema.getAttribute(incrAttr);
            if (attr==null) {
            	throw new Exception("Expected to find "+incrAttr+" in input tuple, but did not");
            }
            incrAttrIndex = attr.getIndex();
            incrAttrType = attr.getType().getMetaType();
            if (MetaType.INT16 != incrAttrType &&
                MetaType.INT32 != incrAttrType &&
                MetaType.INT64 != incrAttrType) {
            	throw new Exception("Incrementing with attributes of type "+incrAttrType+ " not supported; use int16, int32, or int645");
            }
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
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port. 
     * </P>
     * @param inputStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public final void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {

    	
    	byte row[] = getRow(tuple);
    	byte colF[] = getColumnFamily(tuple);
    	byte colQ[] = getColumnQualifier(tuple);
    	
    	long incr = defaultIncr;
    	if (incrAttrIndex >0 ) {
    		if (incrAttrType == MetaType.INT16) {
    			incr = tuple.getShort(incrAttrIndex);
    		}
    		else if (incrAttrType == MetaType.INT32) {
    			incr = tuple.getInt(incrAttrIndex);
    		}
    		else if (incrAttrType == MetaType.INT64) {
    			incr = tuple.getLong(incrAttrIndex);
    		}
    	}
    	long newValue = myTable.incrementColumnValue(row, colF, colQ, incr);
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
    	// For window markers, punctuate all output ports 
    	super.processPunctuation(stream, mark);
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }
}
