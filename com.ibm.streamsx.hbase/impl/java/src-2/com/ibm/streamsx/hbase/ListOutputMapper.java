package com.ibm.streamsx.hbase;

import java.nio.charset.Charset;
import java.util.NavigableMap;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;

public class ListOutputMapper extends OutputMapper {

	StreamSchema tupleSchema = null;
	MetaType valueType = null;

	public boolean isCellPopulator() {
		return true;
	}

	ListOutputMapper(int attrIx, Type type, Charset incharset) throws Exception {
		super(attrIx, incharset);
		tupleSchema = checkListType(type, "ListOutputMapper, value at index "
				+ attrIx);
		valueType = tupleSchema.getAttribute(0).getType().getMetaType();
	}

	public int populate(OutputTuple tuple, NavigableMap<Long, byte[]> values) {
		if (values.isEmpty()) {
			return 0;
		}
		tuple.setList(attrIndex, makeTupleList(tupleSchema, valueType, values));
		return 1;
	}

}
