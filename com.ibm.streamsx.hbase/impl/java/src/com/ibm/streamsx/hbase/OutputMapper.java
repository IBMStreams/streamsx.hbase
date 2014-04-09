/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;

public class OutputMapper {
	
	private int attrIndex = -1;
	private MetaType attrMetaType = null;
	
	OutputMapper(StreamSchema schema, String attrName) throws Exception{
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			throw new Exception ("Expected to find attribute "+attrName);
		}
		attrIndex = attr.getIndex();
		attrMetaType = attr.getType().getMetaType();
		if (attrMetaType != MetaType.RSTRING &
				MetaType.INT64  != attrMetaType) {
			throw new Exception("Unsupported type "+attrMetaType+" for attribute "+attrName);
		}
	}
	
	public void populate(OutputTuple tuple, byte[] value) {
		if (attrMetaType == MetaType.INT64) {
			tuple.setLong(attrIndex, ByteBuffer.wrap(value).getLong());
		}
		else if (attrMetaType == MetaType.RSTRING) {	
			tuple.setString(attrIndex, new String(value));
		}
	}

}