package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;

public abstract class OutputMapper {

	protected Charset charset = HBASEOperator.RSTRING_CHAR_SET;
	protected int attrIndex = -1;

	/**
	 * Determine whether the type is one of the supported types. I should have
	 * used "primitive" instead of "basic" but I can't spell.
	 * 
	 * @param mType
	 * @return true if it's an allowed type, false otherwise
	 */
	static boolean isAllowedBasicType(MetaType mType) {
		if (mType == MetaType.BLOB || mType == MetaType.RSTRING
				|| mType == MetaType.USTRING || mType == MetaType.INT64) {
			return true;
		}
		return false;
	}

	/**
	 * This function checks that the type listType * is a list * is a list of
	 * type tuple * each tuple is one of the allowed basic types followed by a
	 * long
	 * 
	 * @param listType
	 *            The type of the supposed list.
	 * @param attrName
	 *            The name of the attribute--useful for logging only.
	 * @return the schedule for the tuple. The caller needs to store this to
	 *         make tuples.
	 * @throws Exception
	 *             Throws an exception if listType isn't a suitable type.
	 */
	protected static StreamSchema checkListType(Type listType, String attrName)
			throws Exception {
		CollectionType t = (CollectionType) listType;
		TupleType tupType = (TupleType) t.getElementType();
		StreamSchema tupSchema = tupType.getTupleSchema();
		// if we have more or less than two attributes, don't know what to do.
		if (tupSchema.getAttributeCount() != 2) {
			throw new Exception("Attribute " + attrName
					+ " has list type with wrong number of attributes.  Found "
					+ tupSchema.getAttributeCount() + " expected 2");
		}
		// if the second attribute isn't a long, don't know what to do.
		if (tupSchema.getAttribute(1).getType().getMetaType() != MetaType.INT64) {
			throw new Exception(
					"Attribute "
							+ attrName
							+ " has list type, expected second attribute to be of type int64 for timestamp");
		}
		MetaType elementMetaType = tupSchema.getAttribute(0).getType()
				.getMetaType();
		// if first attribute isn't one of the supported types, then we throw an
		// exception.
		if (!isAllowedBasicType(elementMetaType)) {
			throw new Exception(
					"Attribute "
							+ attrName
							+ " has list type, expected first attribute to be of type int64, rstring, or blob");
		}
		return tupSchema;
	}

	/**
	 * This is a preliminary check that the type is one that can be handled by
	 * createOutputMapper. This does only a superficial examination of the
	 * type--even if true, createOutputMapper may still throw an exception.
	 * 
	 * @param mType
	 *            The meta type of the field.
	 * @return
	 */
	static boolean isAllowedType(MetaType mType) {
		if (isAllowedBasicType(mType) || MetaType.LIST == mType
				|| MetaType.TUPLE == mType) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * If true, populations a data structure for an entire row. Calling populate
	 * will result in an exception.
	 * 
	 * @return
	 */
	boolean isRecordPopulator() {
		return false;
	}

	/**
	 * If true, populates from as single cell. The caller must already have
	 * narrowed the data to a single column family and column qualifier. Calling
	 * populateRecord will throw and exception.
	 * 
	 * @return
	 */
	boolean isCellPopulator() {
		return false;
	}

	/**
	 * Get an object from the byte array, when the type is given by the second
	 * argument.
	 * 
	 * @param value
	 *            Byte array containing the object
	 * @param metaType
	 *            The type of the object
	 * @param charset
	 *            character set -- used for USTRINGs
	 * @return
	 */
	protected static Object castFromBytes(byte[] value, MetaType metaType,
			Charset charset) {
		if (MetaType.INT64 == metaType) {
			return ByteBuffer.wrap(value).getLong();
		} else if (MetaType.RSTRING == metaType) {
			return new RString(value);
		} else if (MetaType.BLOB == metaType) {
			return ValueFactory.newBlob(value);
		} else if (MetaType.USTRING == metaType) {
			return new String(value, charset);
		} else {
			throw new RuntimeException("Cannot create object from " + metaType);
		}
	}

	protected OutputMapper(int attrIx, Charset incharset) {
		charset = incharset;
		attrIndex = attrIx;

	}

	/**
	 * Function to be used to create an output mapper. It determines based on
	 * the schema and the attribute name whether the mapper is a single output
	 * mapper (populates a single value), a list output mapper (populates
	 * multiple versions of a single entry), or a record output mapper.
	 * 
	 * @param schema
	 *            The schema of the tuple that will be populated.
	 * @param attrName
	 *            The name of the attribute in that tuple representing the
	 *            value.
	 * @param incharset
	 *            The character set.
	 * @return an OutputMapper that will populate an output tuple
	 * @throws Exception
	 *             If the type of the attribute isn't supported.
	 */
	static OutputMapper createOutputMapper(StreamSchema schema,
			String attrName, Charset incharset) throws Exception {
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			throw new Exception("Expected to find attribute " + attrName);
		}
		int attrIndex = attr.getIndex();
		MetaType attrMetaType = attr.getType().getMetaType();
		if (attrMetaType == MetaType.LIST) {
			return new ListOutputMapper(attrIndex, attr.getType(), incharset);

		} else if (isAllowedBasicType(attrMetaType)) {
			return new SingleOutputMapper(attrIndex, attr.getType(), incharset);
		} else if (attrMetaType == MetaType.TUPLE) {
			return new RecordOutputMapper(attrIndex, attr.getType(), incharset);
		} else {
			throw new Exception("Unsupported type " + attrMetaType
					+ " for attribute " + attrName);
		}
	}

	/**
	 * Populates a tuple from a map of timestamps to values. It takes as input
	 * the data for a single entry. This only makes sense for ListOutputMapper
	 * or SingleOutputMapper.
	 * 
	 * @param tuple
	 *            the output tuple to populate
	 * @param values
	 *            The map of timestamps to values.
	 * @return number of fields populated.
	 * @throws Exception
	 *             throws an exception if not a ListOutputMapper or
	 *             SingleOutputMapper
	 */
	public int populate(OutputTuple tuple, NavigableMap<Long, byte[]> values)
			throws Exception {
		throw new Exception("Invalid tuple population method");
	}

	/**
	 * Populates a tuple attribute of the output tuple. This populates a tuple
	 * attribute of the output tuple, treating the names of attribute as
	 * columnQualifiers, based on a map of columnFamiles to a map of
	 * columnQualifiers to a map of timestamps to values (ie, all the data for a
	 * particular row).
	 * 
	 * @param tuple
	 *            output tuple to populate
	 * @param resultMap
	 *            result map for that row
	 * @return number of entries populated
	 * @throws Exception
	 */
	public int populateRecord(
			OutputTuple tuple,
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap)
			throws Exception {
		throw new Exception("Invalid tuple population method");
	}

	protected List<Tuple> makeTupleList(StreamSchema tupleSchema,
			MetaType valueType, NavigableMap<Long, byte[]> fromHBASE) {
		List<Tuple> tupleList = new ArrayList<Tuple>(fromHBASE.size());

		for (Map.Entry<Long, byte[]> entry : fromHBASE.entrySet()) {
			Object[] tupleArray = new Object[2];
			tupleArray[1] = entry.getKey();
			tupleArray[0] = castFromBytes(entry.getValue(), valueType, charset);
			Tuple newTuple = tupleSchema.getTuple(tupleArray);
			tupleList.add(newTuple);
		}
		return tupleList;
	}
}
