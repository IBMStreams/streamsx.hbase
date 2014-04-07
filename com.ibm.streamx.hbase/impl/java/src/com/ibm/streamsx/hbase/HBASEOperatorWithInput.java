// Licensed Materials - Property of IBM
// Streams Toolkit for HBASE access
// (c) Copyright IBM Corp. 2013
// All rights reserved.

package com.ibm.streamsx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;

public abstract class HBASEOperatorWithInput extends HBASEOperator {
	protected String rowAttr = null;
	protected String columnFamilyAttr =null ;
	protected String columnQualifierAttr=null;
	
	protected int rowAttrIndex = -1;
	protected int colFamilyIndex =-1;
	protected int colQualifierIndex=-1;

	static final String COL_FAM_PARAM_NAME = "columnFamilyAttrName";
	static final String COL_QUAL_PARAM_NAME = "columnQualifierAttrName";
	static final String TABLE_PARAM_NAME = "tableName";
	static final String ROW_PARAM_NAME = "rowAttrName";
	byte colFamBytes[] = null;
	byte colQualBytes[] = null;	
	
	@Parameter(name=COL_FAM_PARAM_NAME,optional=true)
	public void setColumnFamilyAttr(String colF) {
	columnFamilyAttr = colF;
	}
	
	@Parameter(name=COL_QUAL_PARAM_NAME,optional=true)
	public void setColumnQualifierAttr(String colQ) {
		columnQualifierAttr = colQ;	
	}

	@Parameter(name=ROW_PARAM_NAME,optional=false)
	public void setRowAttr(String row) {
		rowAttr = row;
	}
	
	protected byte[] getRow(Tuple tuple) {
        return tuple.getString(rowAttrIndex).getBytes();
	}
	
	protected byte[] getColumnFamily(Tuple tuple) {
		if (colFamBytes == null ) {
			return tuple.getString(colFamilyIndex).getBytes();
		}
		else {
			return colFamBytes;
		}
	}
	protected byte[] getColumnQualifier(Tuple tuple) {
		
		if (colQualBytes == null)
			return tuple.getString(colQualifierIndex).getBytes();
		else {
			return colQualBytes;
		}
	}

	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
		
        StreamingInput<Tuple> input = context.getStreamingInputs().get(0);
        StreamSchema inputSchema = input.getStreamSchema();
        if (rowAttr != null) {
        	rowAttrIndex = checkAndGetIndex(inputSchema, rowAttr);
        }
        if (columnFamilyAttr != null) {
        	colFamilyIndex = checkAndGetIndex(inputSchema,columnFamilyAttr);
        }
        if (columnQualifierAttr != null) {
        	colQualifierIndex = checkAndGetIndex(inputSchema,columnQualifierAttr);
        }
        
        if (staticColumnQualifierList != null) {
        	colQualBytes = staticColumnQualifierList.get(0).getBytes();
        	if (staticColumnQualifierList.size() > 1 ) {
        		throw new Exception("Only one staticColumnQualifier supported for this operator");
        	}
        }
        if (staticColumnFamilyList != null ) {
        	colFamBytes = staticColumnFamilyList.get(0).getBytes();
        	if (staticColumnFamilyList.size() > 1 ) {
        		throw new Exception("Only one staticColumnFamily supported for this operator");
        	}
        }
	}
}
