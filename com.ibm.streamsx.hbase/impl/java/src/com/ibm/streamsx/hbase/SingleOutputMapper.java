/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NavigableMap;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;

public class SingleOutputMapper extends OutputMapper{
	
	private MetaType attrMetaType = null;
	
	    SingleOutputMapper(int attrIx, Type type,Charset incharset) throws Exception{
	    	super(attrIx,incharset);
	    	attrMetaType = type.getMetaType();
	    }


	public boolean isCellPopulator() {
		return true;
	}
	  
	public int populate(OutputTuple tuple, NavigableMap<Long,byte[]> values) {
		Map.Entry<Long, byte[]> latestEntry = values.firstEntry();
		byte[] value = latestEntry.getValue();
		if (value == null) {
			return 0;
		}
		
		if (attrMetaType == MetaType.INT64) {
			tuple.setLong(attrIndex, ByteBuffer.wrap(value).getLong());
		}
		else if (attrMetaType == MetaType.USTRING) {
			tuple.setString(attrIndex, new String(value,charset));
		}
		else if (attrMetaType == MetaType.RSTRING) {	
		    tuple.setObject(attrIndex, new RString(value));
		}
		else if (attrMetaType == MetaType.BLOB) {
			tuple.setBlob(attrIndex,ValueFactory.newBlob(value));
		}
		return 1;
		
	}
}
