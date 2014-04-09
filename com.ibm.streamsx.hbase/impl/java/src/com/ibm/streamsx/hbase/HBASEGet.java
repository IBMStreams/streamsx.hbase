/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

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
 * Gets a tuple or set of tuples from HBASE.
 * <P>
 * The input may be a
 * <ul>
 * <li> row id 
 * <li> row id and column family
 * <li> row id, column family, and column qualifier
 * </ul>
 * 
 * 
 * In the first two cases, the output must be a map, in the third it is a value.  
 */
@PrimitiveOperator(name="HBASEGet", namespace="com.ibm.streamsx.hbase",
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
	Logger logger = Logger.getLogger(this.getClass());

	
	static final String OUT_PARAM_NAME ="outAttrName";
	static final String SUCCESS_PARAM_NAME="outputCountAttr";
	private OutputMode outputMode;
	private String outAttrName;
	private String successAttr = null;
	private OutputMapper primativeOutputMapper = null;
	
	@Parameter(name=SUCCESS_PARAM_NAME,description="Name of attribute in which to put whether the get is successful",optional=true)
	public void setSuccessAttr(String name) {
		successAttr = name;
	}
	@Parameter(name=OUT_PARAM_NAME,description="Name of the attribute in which to put the result of the get.", optional=false)
	public void setOutAttrName(String name) {
		outAttrName = name;
	}
	
	
    /**
     * Initialize this operator. Establishes that the input tuple type is valid.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
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
       	logger.info("outputMode="+outputMode);
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
	    if (inMap == null) {
		return new HashMap<RString,Map<RString,RString>>();
	    }
		Map<RString,Map<RString,RString>> toReturn = new HashMap<RString,Map<RString,RString>>(inMap.size());
		for (byte[] key: inMap.keySet()) {
			toReturn.put(new RString(key),makeStringMap(inMap.get(key)));
		}
		return toReturn;
	}


    /**
     * Get the specified tuple or tuples from HBASE.
     * <P>
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

}
