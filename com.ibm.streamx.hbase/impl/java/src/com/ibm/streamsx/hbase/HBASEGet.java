/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
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
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;

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
@PrimitiveOperator(name="HBASEGet", namespace="streamsx.bigdata.hbase",
description="Java Operator HBASEGet")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class HBASEGet extends HBASEOperatorWithInput {

	private enum OutputMode {
		VALUE,
		VALUE_LIST,
		QUAL_TO_VALUE,
		QUAL_TO_LIST,
		FAMILY_TO_VALUE,
		FAMILY_TO_LIST,
	}
	

	
	static final String OUT_PARAM_NAME ="outAttrName";
	static final String SUCCESS_PARAM_NAME="outputCountAttr";
	private OutputMode outputMode;
	private String outAttrName;
	private String successAttr = null;
	private OutputMapper primativeOutputMapper = null;
	
	@Parameter(name=SUCCESS_PARAM_NAME,optional=true)
	public void setSuccessAttr(String name) {
		successAttr = name;
	}
	@Parameter(name=OUT_PARAM_NAME,optional=false)
	public void setOutAttrName(String name) {
		outAttrName = name;
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
        
        List<StreamingOutput<OutputTuple>> outputs = context.getStreamingOutputs();
        if (outputs.size() != 1) {
        	throw new Exception ("Wrong number of outputs; expected 1 found "+outputs.size());
        }
        StreamingOutput<OutputTuple> output = outputs.get(0);
        StreamSchema outSchema = output.getStreamSchema();
        Attribute outAttr = outSchema.getAttribute(outAttrName);
        Type outType = outAttr.getType();
        MetaType mType = outType.getMetaType();
        if(MetaType.RSTRING == mType || MetaType.INT64 == mType) {
        	outputMode = OutputMode.VALUE;
        	if ((colFamBytes == null && columnFamilyAttr == null)
        		|| (colQualBytes == null && columnQualifierAttr == null)) {
        		throw new Exception("If output is of type rstring or long, both "+COL_FAM_PARAM_NAME+" and "+COL_QUAL_PARAM_NAME+" must be specified");
        	}
        	primativeOutputMapper = new OutputMapper(outSchema,outAttrName);
        }
        else if (mType.isMap()) {
        	Type elementType = ((CollectionType)outType).getElementType();
        	MetaType elementMetaType = elementType.getMetaType();
        	if (MetaType.RSTRING == elementMetaType) {
        		outputMode = OutputMode.QUAL_TO_VALUE;
        		if (columnFamilyAttr == null  && colFamBytes == null)
        			throw new Exception("If output is of type map<rstring,rstring>, then "+COL_FAM_PARAM_NAME+" must be set");
        	}
        	else if (elementMetaType.isMap()) {
        		MetaType elementElementMetaType = ((CollectionType)elementType).getElementType().getMetaType();
        		if (elementElementMetaType == MetaType.RSTRING) {
        			outputMode = OutputMode.FAMILY_TO_VALUE;
        		}
        		else {
        			throw new Exception("Invalid type "+mType+" in attribute given by "+OUT_PARAM_NAME);
        		}
        	}
        }
        else {
        	throw new Exception("Attribute "+outAttrName+" must be of type rstring, list<rstring>, or a map");
        }
        
        if (outputMode == OutputMode.VALUE_LIST || 
        		outputMode == OutputMode.FAMILY_TO_LIST || 
        		outputMode == OutputMode.QUAL_TO_LIST) {
        	throw new Exception("Not supported output type.");
        }
        System.out.println("outputMode="+outputMode);
        System.out.println("Exit init get: StaticColFamily: "+staticColumnFamilyList.get(0));
	}
	
	Map<RString,RString> makeStringMap(Map<byte[],byte[]> inMap) {
		if (inMap == null) {
			return new HashMap<RString,RString>(0);
		}
		Map<RString,RString> toReturn = new HashMap<RString,RString>(inMap.size());
		for (byte[] key: inMap.keySet()) {
			toReturn.put(new RString(key), new RString(inMap.get(key)));
		}
		return toReturn;
	}
	
	Map<RString,Map<RString,RString>> makeMapOfMap(NavigableMap<byte[],NavigableMap<byte[],byte[]>> inMap) {
		Map<RString,Map<RString,RString>> toReturn = new HashMap<RString,Map<RString,RString>>(inMap.size());
		for (byte[] key: inMap.keySet()) {
			toReturn.put(new RString(key),makeStringMap(inMap.get(key)));
		}
		return toReturn;
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
        System.out.println("exit all ports ready, get: StaticColFamily: "+staticColumnFamilyList.get(0));
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
    	// Create a new tuple for output port 0
        StreamingOutput<OutputTuple> outStream = getOutput(0);
        OutputTuple outTuple = outStream.newTuple();

        // Copy across all matching attributes.
        outTuple.assign(tuple);
        Get myGet = new Get(getRow(tuple));
        byte colF[] =null;
        byte colQ[] =null;
        if (colFamBytes != null || 
        	columnFamilyAttr != null) {
            colF = getColumnFamily(tuple);
        	if (colQualBytes!= null ||
        			columnQualifierAttr != null)  {
        	       colQ = getColumnQualifier(tuple);
        	       myGet.addColumn(colF, colQ);
        	}
        	else {
        		myGet.addFamily(colF);
        	}
        }

        Result r = myTable.get(myGet);
        
        int numResults = r.size();
        if (successAttr != null) {
        	outTuple.setInt(successAttr,numResults);
        }
        switch (outputMode) {
        case VALUE:
        	if (numResults > 0)
        		primativeOutputMapper.populate(outTuple,r.getValue(colF, colQ));
        	break;
        case QUAL_TO_VALUE:
        	outTuple.setMap(outAttrName, makeStringMap(r.getFamilyMap(colF)));
        	break;
        case FAMILY_TO_VALUE:
        		outTuple.setMap(outAttrName,makeMapOfMap(r.getNoVersionMap()));
        		break;
        case VALUE_LIST:
        case QUAL_TO_LIST:
        case FAMILY_TO_LIST:
        				throw new Exception("Unsupported output type "+outputMode);
        }
       
        // TODO: Insert code to perform transformation on output tuple as needed:
        // outTuple.setString("AttributeName", "AttributeValue");

        // Submit new tuple to output port 0
        outStream.submit(outTuple);
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
