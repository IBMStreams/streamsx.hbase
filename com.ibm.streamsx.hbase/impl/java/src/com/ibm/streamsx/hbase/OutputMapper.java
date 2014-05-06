/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;

public class OutputMapper {
	
	private int attrIndex = -1;
	private MetaType attrMetaType = null;
    private Charset charset = null;
	
	    OutputMapper(StreamSchema schema, String attrName,Charset incharset) throws Exception{
	    	charset = incharset;
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			throw new Exception ("Expected to find attribute "+attrName);
		}
		attrIndex = attr.getIndex();
		attrMetaType = attr.getType().getMetaType();
		if (attrMetaType != MetaType.RSTRING &&
				MetaType.INT64  != attrMetaType &&
				MetaType.BLOB != attrMetaType) {
			throw new Exception("Unsupported type "+attrMetaType+" for attribute "+attrName);
		}
	}
	
	public void populate(OutputTuple tuple, byte[] value) {
		if (attrMetaType == MetaType.INT64) {
			tuple.setLong(attrIndex, ByteBuffer.wrap(value).getLong());
		}
		else if (attrMetaType == MetaType.RSTRING) {	
		    tuple.setString(attrIndex, new String(value,charset));
		}
		else if (attrMetaType == MetaType.BLOB) {
			tuple.setBlob(attrIndex,ValueFactory.newBlob(value));
		}
	}

}
