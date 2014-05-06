package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;

public class PopulateTuple {

	String attrNames[];
	byte columnQualifiers[][];
	MetaType outTypes[];
	StreamSchema tupleSchema[];
	Charset charset = HBASEOperator.DEFAULT_CHAR_SET;
	
	/**
	 * Determine whether the type is one of the supported types.
	 * I should have used "primitive" instead of "basic" but I can't spell.
	 * 
	 * @param mType
	 * @return true if it's an allowed type, false otherwise
	 */
	static boolean isAllowedBasicType(MetaType mType) {
		if (mType == MetaType.BLOB ||
				mType == MetaType.RSTRING ||
				mType == MetaType.INT64) {
			return true;
		}
		return false;
	}
	

	/**
	 * Establish the mapping data structures.
	 * To make the tuple processing go as fast as possible, and to get errors as soon as possible,
	 * we do all the type checking here.   
	 * 
	 * We store:
	 * * columnQualifiers
	 * * types 
	 * * the tuple schema (if we are grabbing multiple versions)
	 * 
	 * @param schema  The record this will populate.
	 * @param charset  The character set used to convert the attribute names to bytes.
	 * @throws Exception Throws an exception if the type is unmappable.
	 */
	public PopulateTuple(StreamSchema schema,Charset _charset) throws Exception {
		charset = _charset;
		int numAttr = schema.getAttributeCount();
		attrNames = new String[numAttr];
		columnQualifiers =  new byte[numAttr][];
		outTypes = new MetaType[numAttr];
		tupleSchema = new StreamSchema[numAttr];
		for (int i =0 ; i < numAttr;i++) {
			Attribute attr = schema.getAttribute(i) ;
			attrNames[i] = attr.getName();
			columnQualifiers[i] = attrNames[i].getBytes(charset);
			MetaType mType = attr.getType().getMetaType();
			if (isAllowedBasicType(mType)) {
				outTypes[i] =mType;
				tupleSchema[i] = null;
			}
			else if ( MetaType.LIST == mType) {
				// Okay, so it's a list.  Now we are again very particular about the schema.
				CollectionType t = (CollectionType)attr.getType();
				TupleType tupType = (TupleType)t.getElementType();
				StreamSchema tupSchema = tupType.getTupleSchema();
				// if we have more or less than two attributes, don't know what to do.
				if (tupSchema.getAttributeCount() != 2) {
					throw new Exception("Attribute "+attr.getName()+" has list type with wrong number of attributes.  Found "+tupSchema.getAttributeCount()+" expected 2");
				}
				// if the second attribute isn't a long, don't know what to do.
				if (tupSchema.getAttribute(1).getType().getMetaType() != MetaType.INT64) {
					throw new Exception("Attribute "+attr.getName()+" has list type, expected second attribute to be of type int64 for timestamp");
				}
				MetaType elementMetaType = tupSchema.getAttribute(0).getType().getMetaType();
				// if first attribute isn't one of the supported types, then we throw an exception.
				if (!isAllowedBasicType(elementMetaType)) {
					throw new Exception("Attribute "+attr.getName()+" has list type, expected first attribute to be of type int64, rstring, or blob");
				}
				// So, now we know 
				// * it is a list
				// * has exactly two elements.  
				// * first element is a supported basic type
				// * second element is a long for the timestamp
				tupleSchema[i] = tupSchema;
				outTypes[i] = mType;
			}
			else {
				throw new Exception("Attribute "+attr.getName()+" has unsupported type "+attr.getType());
			}
		}
	}
	
	/**
	 * Used by HBASEGet and HBASEScan create a map suitable for tuple creation from
	 * the family map
	 * @param attrNames The names of the attributes to populate
	 * @param familyMap HBASE results
	 * @return
	 */
	public Map<String,Object> getAttributeMap(Map<String,Object> inMap,
			NavigableMap<byte[], byte[]> familyMap) {
		for (int i = 0; i < columnQualifiers.length; i++) {
			byte colQ[] = columnQualifiers[i];
			if (familyMap.containsKey(colQ)) {
				inMap.put(attrNames[i],new RString(familyMap.get(colQ)));;
			}
		}
		return inMap;
	}
	
	/**
	 * Populate attribute map with versions.  This is the version that will be called
	 * if some attributes have multiple versions.
	 * 
	 * @param inMap  This is the map that will be used to build the tuple.  The operator adds entries here.
	 * @param resultMap  The results we got from HBASE.
	 * @return
	 */
	public Map<String,Object> getAttributeMapWithVersions(Map<String,Object> inMap,
			 NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> resultMap) {
		for (byte fam[]: resultMap.keySet()) {
			for(int i = 0; i < columnQualifiers.length; i++) {
				byte colQ[] = columnQualifiers[i];
				if (resultMap.get(fam).containsKey(colQ)) {
					Map.Entry<Long, byte[]> lastEntry = resultMap.get(fam).get(colQ).lastEntry();
					if (lastEntry == null) {
						// If there is no entry then let's just skip to the next columnQualifier
						continue;
					}
					if (tupleSchema[i] == null) {
						// easy to populate!	
						inMap.put(attrNames[i], castFromBytes(lastEntry.getValue(),outTypes[i]));
					}
					else {
						for (Map.Entry<Long,byte[]> entry : resultMap.get(fam).get(colQ).entrySet()) {
							Object[] tupleArray= new Object[2];
							tupleArray[0] = entry.getKey();
							tupleArray[1] = castFromBytes(entry.getValue(),outTypes[i]);
							Tuple newTuple = tupleSchema[i].getTuple(tupleArray);
						}
					}
				}
			}
		}
		
		return inMap;
	}


	private Object castFromBytes(byte[] value, MetaType metaType){
		if (MetaType.INT64 == metaType) {
			return ByteBuffer.wrap(value).getLong();
		}
		else if (MetaType.RSTRING == metaType) {
			return new String(value,charset);
		}
		else if (MetaType.BLOB == metaType) {
			return ValueFactory.newBlob(value);
		}
		else {
			throw new RuntimeException("Cannot create object from "+metaType);
		}
	}
	
}
