/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

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
import com.ibm.streams.operator.meta.TupleType;
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
 * 
 * The input may be a
 * 
 * * row id 
 * * row id and column family
 * * row id, column family, and column qualifier
 * 
 * When the input is: row id, columnFamily, and columnQualifier
 * the outAttribute must be a value type (currently just rstring)
 * 
 * When the input is a rowid and columnFamily,
 * the outAttribute can be:
 * *  a map of columnQualifiers to values
 * * a tuple, where the tuple attribute names correspond to column qualifiers
 * 
 * When the input is a rowid
 * the out attribute must be a map of maps of columnQualifers to values.
 * 
 */
@PrimitiveOperator(name="HBASEGet", namespace="com.ibm.streamsx.hbase",
description="Get tuples from HBASE; similar to enrich from database operators.  It places the result in the parameter described by "+HBASEGet.OUT_PARAM_NAME+"  The operator accepts three types of queries.  In the simplest case, a row, columnFamily, and columnQualifier is specified, and the output value is the single value in that entry.  The type of the value may be long or rstring.  If the columnQualifier is left unspecified, then "+HBASEGet.OUT_PARAM_NAME+" is populated with a map of columnQualifiers to values."+" If columnFamily is also left unspecified, then "+HBASEGet.OUT_PARAM_NAME+" is populated with a map of columnFamilies to a map of columnQualifiers to values.  In all cases, if an attribute of name "+HBASEGet.SUCCESS_PARAM_NAME+" exists on the output port, it will be populated with the number of values found.  This can help distinguish between the case when the value returned is zero and the cae where no such entry existed in hbase.")
    @InputPorts({@InputPortSet(description="Description of which tuples to get", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Input tuple with value or values from HBASE", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving)})
public class HBASEGet extends HBASEOperatorWithInput {

	/*
	 * Used to describe the way tuples will be populated.  It is set at initialization time, and then used on process
	 * to determine what to do.  
	 */
	private enum OutputMode {
		VALUE,
		VALUE_LIST,
		RECORD,
		QUAL_TO_VALUE,
		QUAL_TO_LIST,
		FAMILY_TO_VALUE,
		FAMILY_TO_LIST,
	}
	
	// The next two are used in Record mode only.
	private Set<String> colQualifiers = null;
	StreamSchema recordSchema = null;
	

	Logger logger = Logger.getLogger(this.getClass());

	
	static final String OUT_PARAM_NAME ="outAttrName";
	static final String SUCCESS_PARAM_NAME="outputCountAttr";
	private OutputMode outputMode;
	private String outAttrName;
	private String successAttr = null;
	private OutputMapper primativeOutputMapper = null;
	
	@Parameter(name=SUCCESS_PARAM_NAME,description="Name of attribute of the output port in which to put the count of values populated.",optional=true)
	public void setSuccessAttr(String name) {
		successAttr = name;
	}
	@Parameter(name=OUT_PARAM_NAME,description="Name of the attribute of the output port in which to put the result of the get.  Its type depends on whether a columnFamily or columnQualifier was set.", optional=true)
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
        	primativeOutputMapper = new OutputMapper(outSchema,outAttrName,charset);
        }
        else if (mType.isMap()) {
        	Type elementType = ((CollectionType)outType).getElementType();
        	MetaType elementMetaType = elementType.getMetaType();
        	if (MetaType.RSTRING == elementMetaType) {
        		outputMode = OutputMode.QUAL_TO_VALUE;
        		if (columnFamilyAttr == null  && colFamBytes == null)
        			throw new Exception("If output is of type map<rstring,rstring>, then "+COL_FAM_PARAM_NAME+" or "+STATIC_COLF_NAME+" must be set");
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
        else if (MetaType.TUPLE == mType){
    		if (columnFamilyAttr == null  && colFamBytes == null)
    			throw new Exception("If output is of type tuple, then "+COL_FAM_PARAM_NAME+" or "+STATIC_COLF_NAME+" must be set");
        	outputMode = OutputMode.RECORD;
        	
        	// These two need to be populated.  Let's do it once during initialization.
        	recordSchema = ((TupleType)outType).getTupleSchema();
        	colQualifiers = recordSchema.getAttributeNames();
        	if (logger.isInfoEnabled()) {
        	logger.info("Expect to find the following columnQualifers: ");
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
		    toReturn.put(new RString(new String(key,charset)), new RString(new String(inMap.get(key),charset)));
		}
		return toReturn;
	}
	
	Map<RString,Map<RString,RString>> makeMapOfMap(NavigableMap<byte[],NavigableMap<byte[],byte[]>> inMap) {
	    if (inMap == null) {
		return new HashMap<RString,Map<RString,RString>>();
	    }
		Map<RString,Map<RString,RString>> toReturn = new HashMap<RString,Map<RString,RString>>(inMap.size());
		for (byte[] key: inMap.keySet()) {
		    toReturn.put(new RString(new String(key,charset)),makeStringMap(inMap.get(key)));
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
        case RECORD:
        	if (numResults > 0) {
        		// use the family map to get the outTupleFields.
        		Map<String,RString> outTupleFields =  extractRStrings(colQualifiers,r.getFamilyMap(colF));
        		// In this case, we reset the number of results.  This way, a down stream operator can check that all were
        		// populated.
        		numResults = outTupleFields.size();
        		outTuple.setTuple(outAttrName, recordSchema.getTuple(outTupleFields));
        	}
        	break;
        case VALUE_LIST:
        case QUAL_TO_LIST:
        case FAMILY_TO_LIST:
        				throw new Exception("Unsupported output type "+outputMode);
        }
        // Set the num results, if needed.
        if (successAttr != null) {
        	outTuple.setInt(successAttr,numResults);
        }
        
        // Submit new tuple to output port 0
        outStream.submit(outTuple);
    }
    


}
