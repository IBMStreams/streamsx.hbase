package com.ibm.streamsx.hbase;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.TupleType;

public class RecordOutputMapper extends OutputMapper {

	String attrNames[];
	byte columnQualifiers[][];
	MetaType outTypes[];
	StreamSchema tupleSchema[];
	StreamSchema recordSchema = null;

	boolean isRecordPopulator() {
		return true;
	}

	/**
	 * Establish the mapping data structures. To make the tuple processing go as
	 * fast as possible, and to get errors as soon as possible, we do all the
	 * type checking here.
	 * 
	 * We store: * columnQualifiers * types * the tuple schema (if we are
	 * grabbing multiple versions)
	 * 
	 * @param schema
	 *            The record this will populate.
	 * @param charset
	 *            The character set used to convert the attribute names to
	 *            bytes.
	 * @throws Exception
	 *             Throws an exception if the type is unmappable.
	 */
	protected RecordOutputMapper(int _attrIndex, Type thisType, Charset _charset)
			throws Exception {
		super(_attrIndex, _charset);
		StreamSchema schema = ((TupleType) thisType).getTupleSchema();
		recordSchema = schema;
		int numAttr = schema.getAttributeCount();
		attrNames = new String[numAttr];
		columnQualifiers = new byte[numAttr][];
		outTypes = new MetaType[numAttr];
		tupleSchema = new StreamSchema[numAttr];
		for (int i = 0; i < numAttr; i++) {
			Attribute attr = schema.getAttribute(i);
			attrNames[i] = attr.getName();
			columnQualifiers[i] = attrNames[i].getBytes(charset);
			MetaType mType = attr.getType().getMetaType();
			if (isAllowedBasicType(mType)) {
				outTypes[i] = mType;
				tupleSchema[i] = null;
			} else if (MetaType.LIST == mType) {
				// Okay, so it's a list. Now we are again very particular about
				// the schema.

				// So, now we know
				// * it is a list
				// * has exactly two elements.
				// * first element is a supported basic type
				// * second element is a long for the timestamp
				tupleSchema[i] = checkListType(attr.getType(), attr.getName());
				outTypes[i] = tupleSchema[i].getAttribute(0).getType()
						.getMetaType();
			} else {
				throw new Exception("Attribute " + attr.getName()
						+ " has unsupported type " + attr.getType());
			}
		}
	}

	/**
	 * Populate attribute map with versions. This is the version that will be
	 * called if some attributes have multiple versions.
	 * 
	 * @param tuple
	 *            Output tuple to be populated
	 * @param resultMap
	 *            The results we got from HBASE.
	 */
	public int populateRecord(
			OutputTuple tuple,
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap) {
		Map<String, Object> inMap = new HashMap<String, Object>(
				attrNames.length);
		for (byte fam[] : resultMap.keySet()) {
			for (int i = 0; i < columnQualifiers.length; i++) {
				byte colQ[] = columnQualifiers[i];
				if (resultMap.get(fam).containsKey(colQ)) {
					Map.Entry<Long, byte[]> lastestEntry = resultMap.get(fam)
							.get(colQ).firstEntry();
					if (lastestEntry == null) {
						// If there is no entry then let's just skip to the next
						// columnQualifier
						continue;
					}
					if (tupleSchema[i] == null) {
						// easy to populate!
						inMap.put(
								attrNames[i],
								castFromBytes(lastestEntry.getValue(),
										outTypes[i], charset));
					} else {
						inMap.put(
								attrNames[i],
								makeTupleList(tupleSchema[i], outTypes[i],
										resultMap.get(fam).get(colQ)));
					}
				}
			}
		}
		tuple.setTuple(attrIndex, recordSchema.getTuple(inMap));
		return inMap.size();
	}

}
