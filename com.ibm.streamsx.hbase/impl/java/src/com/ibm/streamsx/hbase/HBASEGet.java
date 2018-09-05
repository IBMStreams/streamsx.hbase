/* Copyright (C) 2013-2018, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.model.Icons;
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
 * * row id * row id and column family * row id, column family, and column
 * qualifier
 * 
 * When the input is: row id, columnFamily, and columnQualifier the outAttribute
 * must be a value type (currently just rstring)
 * 
 * When the input is a rowid and columnFamily, the outAttribute can be: * a map
 * of columnQualifiers to values * a tuple, where the tuple attribute names
 * correspond to column qualifiers
 * 
 * When the input is a rowid the out attribute must be a map of maps of
 * columnQualifers to values.
 * 
 */
@PrimitiveOperator(name = "HBASEGet", namespace = "com.ibm.streamsx.hbase", description = "The `HBASEGet` operator gets tuples from HBASE. It is similar to the `ODBCEnrich` operator in the Database Toolkit.  It puts the result in the attribute of the output port that is specified in the "
		+ HBASEGet.OUT_PARAM_NAME
		   + " parameter. The operator accepts four types of queries.  In the simplest case, you specify a row, columnFamily, and columnQualifier, and the output value is the single value in that entry. \\n"
+"    stream<rstring who, rstring infoType, rstring requestedDetail, rstring value, \\n"
+"           int32 numResults> queryResults = HBASEGet(queries) {\\n"
+"              param\\n"
+"                 tableName : \\\"streamsSample_lotr\\\";\\n"
+"                 rowAttrName : \\\"who\\\" ;\\n"
+"                 columnFamilyAttrName : \\\"infoType\\\" ;\\n"
+"                 columnQualifierAttrName : \\\"requestedDetail\\\" ;\\n"
+"                 outAttrName : \\\"value\\\" ;\\n"
+"                 outputCountAttr : \\\"numResults\\\" ;\\n"
+"                }\\n"
+"\\nIf the type of the attributed given by outAttrName is a tuple, it interprets the "
+" the attribute names of the output attribute as the column qualifiers, thus getting "
+" multiple values at once." 
+"\\nSuppose that you represent a book in HBase as a row, with a single column family,"
+" and entries with different column qualifiers to represent the title, the author_fname, the author_lname, "
+" and the year.  We could do separate queries for each column family using the "
+" the approach above, but as a short cut, we let you populate the whole tuple at once "
+" Let `GetBookType` represent a the type of a book.\\n" 
+"    type GetBookType = rstring title,rstring author_fname, rstring author_lname, rstring year, rstring fiction;\\n"
+ HBASEOperator.DOC_BLANKLINE
+ "Then we query the table as follows:\\n"
+ "    stream<rstring key,GetBookType value> enriched = HBASEGet(keyStream) {\\n"
+ "      param\\n"
+ "        rowAttrName: \\\"key\\\";\\n"
+ "        tableName: \\\"streamsSample_books\\\";\\n"
+ "        staticColumnFamily: \\\"all\\\";\\n"
+ "        outAttrName: \\\"value\\\";\\n"
+ "    }\\n"
+HBASEOperator.DOC_BLANKLINE
+" Additionally, you can get all the entries for a given row-columnFamily pair by supplying an output attribute that is of type map.  The map will be populated with columnqualifiers mapping to their corresponding values.  If you wan all the entries for a given row, you can supply an output attribute that is of type map to map.  The map will take column families to a map of columnqualfiers to values.  See the GetSample in samples for details."
+HBASEOperator.DOC_BLANKLINE
+"If you wish to get multiple versions of a given entry, you can do that by providing using a list type instead of a primitive type.  "
+"In all cases, if an attribute with the name "
		+ HBASEGet.SUCCESS_PARAM_NAME
		+ " exists on the output port, it is populated with the number of values found.  This behavior can help you distinguish between the case where the value returned is zero and the case where no such entry existed in HBase."
		+ HBASEGet.consistentCutInfo + HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Description of which tuples to get", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Input tuple with value or values from HBASE", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
@Icons(location32 = "impl/java/icons/HBASEGet_32.gif", location16 = "impl/java/icons/HBASEGet_16.gif")
public class HBASEGet extends HBASEOperatorWithInput {

	public static final String consistentCutInfo = HBASEOperator.consistentCutIntroducer
			+ "The `HBASEGet` operator can be in a consistent region.  It is treated as a stateless operator, which means that if the underlying HBASE table changes between the first time a tuple is sent and when it is replayed, the `HBASEGet` operator gives a different answer. "
			+ "As a result, if you use this operator in a consistent region in conjuction with an operator that changes the state of a tuple "
			+ "you could get unexpected behavior. This might happen, for example, if the `HBASEGet` operator feeds a functor that increments a value, and then puts the tuple back into HBase by using the `HBASEPut` operator. "
			+ "\\nHBASEGet is not supported as the source of a consistent region.";

	/*
	 * Used to describe the way tuples will be populated. It is set at
	 * initialization time, and then used on process to determine what to do.
	 */
	private enum OutputMode {
		CELL, RECORD, QUAL_TO_VALUE, FAMILY_TO_VALUE,
	}

	private OutputMapper outMapper;
	private int maxVersions = -1;
	private long minTimestamp = Long.MIN_VALUE;

	Logger logger = Logger.getLogger(this.getClass());

	static final String OUT_PARAM_NAME = "outAttrName";
	static final String SUCCESS_PARAM_NAME = "outputCountAttr";
	public static final String MIN_TIMESTAMP_PARAM_NAME = "minTimestamp";
	public static final String MIN_TIMESTAMP_DESC = "This parameter specifies the minimum timestamp that is used for queries.  The operator does not return any entries with a timestamp older than this value.  Unless you specify the **maxVersions** parameter, the opertor returns only one entry in this time range.";
	public static final String MAX_VERSIONS_PARAM_NAME = "maxVersions";
	public static final String MAX_VERSIONS_DESC = "This parameter specifies the maximum number of versions that the operator returns.  It defaults to a value of one.  A value of 0 indicates that the operator gets all versions.";
	private OutputMode outputMode;
	private static final String defaultOutAttrName = "value";
	private String outAttrName = defaultOutAttrName;;
	private String successAttr = null;

	@Parameter(name = SUCCESS_PARAM_NAME, description = "This parameter specifies the name of attribute of the output port where the operator puts a count of the values it populated.", optional = true)
	public void setSuccessAttr(String name) {
		successAttr = name;
	}

	@Parameter(name = OUT_PARAM_NAME, description = "This parameter specifies the name of the attribute of the output port in which the operator puts the retrieval results.  The data type for the attribute depends on whether you specified a columnFamily or columnQualifier.", optional = true)
	public void setOutAttrName(String name) {
		outAttrName = name;
	}

	@Parameter(name = MIN_TIMESTAMP_PARAM_NAME, optional = true, description = MIN_TIMESTAMP_DESC)
	public void setMinTimestamp(long inTs) {
		minTimestamp = inTs;
	}

	@Parameter(name = MAX_VERSIONS_PARAM_NAME, optional = true, description = MAX_VERSIONS_DESC)
	public void setMaxVersions(int inMax) {
		maxVersions = inMax;
	}

	@ContextCheck(compile = true)
	public static void compileChecks(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		int numOut = context.getNumberOfStreamingOutputs();
		if (numOut != 1) {
			checker.setInvalidContext(Messages.getString("HBASE_GET_INVALID_OUTPUT", numOut), null);
		}
		checkConsistentRegionSource(checker, "HBASEGet");
	}

	@ContextCheck(compile = false)
	public static void runtimeChecks(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		Set<String> params = context.getParameterNames();
		String outName = defaultOutAttrName;
		if (params.contains(OUT_PARAM_NAME)) {
			outName = context.getParameterValues(OUT_PARAM_NAME).get(0);
		}

		StreamingOutput<OutputTuple> outStream = context.getStreamingOutputs()
				.get(0);
		checker.checkRequiredAttributes(outStream, outName);
		// StreamSchema outSchema = outStream.getStreamSchema();
		// Attribute outAttr = outSchema.getAttribute(outAttrName);
	}

	/**
	 * Initialize this operator. Establishes that the input tuple type is valid.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		logger.trace("Operator " + context.getName() + " initializing in PE: "
				+ context.getPE().getPEId() + " in Job: "
				+ context.getPE().getJobId());

		List<StreamingOutput<OutputTuple>> outputs = context
				.getStreamingOutputs();
		// we already checked that the number of outputs is correct.
		StreamingOutput<OutputTuple> output = outputs.get(0);
		StreamSchema outSchema = output.getStreamSchema();
		Attribute outAttr = outSchema.getAttribute(outAttrName);
		Type outType = outAttr.getType();
		MetaType mType = outType.getMetaType();
		if (OutputMapper.isAllowedType(mType)) {
			outMapper = OutputMapper.createOutputMapper(outSchema, outAttrName,
					charset);
			if (outMapper.isCellPopulator()) {
				outputMode = OutputMode.CELL;
				if ((colFamBytes == null && columnFamilyAttr == null)
						|| (colQualBytes == null && columnQualifierAttr == null)) {
					throw new Exception(
							"If output is of type rstring or long, both "
									+ COL_FAM_PARAM_NAME + " and "
									+ COL_QUAL_PARAM_NAME
									+ " must be specified");
				}
			} else if (outMapper.isRecordPopulator()) {
				outputMode = OutputMode.RECORD;
			} else {
				throw new Exception(
						"Output mapper is neither record populator or cell populator");
			}
		} else if (mType.isMap()) {
			Type elementType = ((CollectionType) outType).getElementType();
			MetaType elementMetaType = elementType.getMetaType();
			if (MetaType.RSTRING == elementMetaType) {
				outputMode = OutputMode.QUAL_TO_VALUE;
				if (columnFamilyAttr == null && colFamBytes == null)
					throw new Exception(
							"If output is of type map<rstring,rstring>, then "
									+ COL_FAM_PARAM_NAME + " or "
									+ STATIC_COLF_NAME + " must be set");
			} else if (elementMetaType.isMap()) {
				MetaType elementElementMetaType = ((CollectionType) elementType)
						.getElementType().getMetaType();
				if (elementElementMetaType == MetaType.RSTRING) {
					outputMode = OutputMode.FAMILY_TO_VALUE;
				} else {
					throw new Exception("Invalid type " + mType
							+ " in attribute given by " + OUT_PARAM_NAME);
				}
			}
		} else {
			throw new Exception("Attribute " + outAttrName
					+ " must be of type rstring, list<rstring>, or a map");
		}
		logger.info("outputMode=" + outputMode);
	}

	Map<RString, RString> makeStringMap(Map<byte[], byte[]> inMap) {
		if (inMap == null) {
			return new HashMap<RString, RString>(0);
		}
		Map<RString, RString> toReturn = new HashMap<RString, RString>(
				inMap.size());
		for (byte[] key : inMap.keySet()) {
			toReturn.put(new RString(new String(key, charset)), new RString(
					new String(inMap.get(key), charset)));
		}
		return toReturn;
	}

	Map<RString, Map<RString, RString>> makeMapOfMap(
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> inMap) {
		if (inMap == null) {
			return new HashMap<RString, Map<RString, RString>>();
		}
		Map<RString, Map<RString, RString>> toReturn = new HashMap<RString, Map<RString, RString>>(
				inMap.size());
		for (byte[] key : inMap.keySet()) {
			toReturn.put(new RString(new String(key, charset)),
					makeStringMap(inMap.get(key)));
		}
		return toReturn;
	}

	/**
	 * Get the specified tuple or tuples from HBASE.
	 * <P>
	 * 
	 * @param inputStream
	 *            Port the tuple is arriving on.
	 * @param tuple
	 *            Object representing the incoming tuple.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
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
		if (maxVersions > 0) {
			myGet.setMaxVersions(maxVersions);
		} else if (maxVersions == 0) {
			myGet.setMaxVersions();
		}
		if (minTimestamp != Long.MIN_VALUE) {
			if (logger.isInfoEnabled())
				logger.info(Messages.getString("HBASE_GET_SET_TIME_RANGE", minTimestamp, Long.MAX_VALUE));
			myGet.setTimeRange(minTimestamp, Long.MAX_VALUE);
		}
		byte colF[] = null;
		byte colQ[] = null;
		if (colFamBytes != null || columnFamilyAttr != null) {
			colF = getColumnFamily(tuple);
			if (colQualBytes != null || columnQualifierAttr != null) {
				colQ = getColumnQualifier(tuple);
				myGet.addColumn(colF, colQ);
			} else {
				myGet.addFamily(colF);
			}
		}
	//	HTableInterface myTable = connection.getTable(tableNameBytes);
		Table myTable = getHTable();
		Result r = myTable.get(myGet);

		int numResults = r.size();

		switch (outputMode) {
		case CELL:
			if (numResults > 0)
				outMapper.populate(outTuple, r.getMap().get(colF).get(colQ));
			break;
		case RECORD:
			if (numResults > 0) {
				numResults = outMapper.populateRecord(outTuple, r.getMap());
				// In this case, we reset the number of results. This way, a
				// down stream operator can check that all were
				// populated.
			}
			break;
		case QUAL_TO_VALUE:
			outTuple.setMap(outAttrName, makeStringMap(r.getFamilyMap(colF)));
			break;
		case FAMILY_TO_VALUE:
			outTuple.setMap(outAttrName, makeMapOfMap(r.getNoVersionMap()));
			break;
		}
		// Set the num results, if needed.
		if (successAttr != null) {
			outTuple.setInt(successAttr, numResults);
		}

		// Submit new tuple to output port 0
		outStream.submit(outTuple);
		myTable.close();
	}

}
