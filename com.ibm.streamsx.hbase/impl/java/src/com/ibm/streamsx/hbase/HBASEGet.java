/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
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
@PrimitiveOperator(name = "HBASEGet", namespace = "com.ibm.streamsx.hbase", description = "Get tuples from HBASE; similar to enrich from database operators.  It places the result in the parameter described by "
		+ HBASEGet.OUT_PARAM_NAME
		+ "  The operator accepts three types of queries.  In the simplest case, a row, columnFamily, and columnQualifier is specified, and the output value is the single value in that entry.  The type of the value may be long or rstring.  If the columnQualifier is left unspecified, then "
		+ HBASEGet.OUT_PARAM_NAME
		+ " is populated with a map of columnQualifiers to values."
		+ " If columnFamily is also left unspecified, then "
		+ HBASEGet.OUT_PARAM_NAME
		+ " is populated with a map of columnFamilies to a map of columnQualifiers to values.  In all cases, if an attribute of name "
		+ HBASEGet.SUCCESS_PARAM_NAME
		+ " exists on the output port, it will be populated with the number of values found.  This can help distinguish between the case when the value returned is zero and the case where no such entry existed in HBase."
		+ HBASEGet.consistentCutInfo + HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Description of which tuples to get", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Input tuple with value or values from HBASE", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
@Icons(location32 = "impl/java/icons/HBASEGet_32.gif", location16 = "impl/java/icons/HBASEGet_16.gif")
public class HBASEGet extends HBASEOperatorWithInput {

	public static final String consistentCutInfo = HBASEOperator.consistentCutIntroducer
			+ " HBASEGet is allowed in a consistent region.  It is treated as a stateless operator, which means that if the underlying HBASE table changes between the first time a tuple is sent and when it is replayed, HBASEGet will give a different answer"
			+ "As a result, if used in a consistent region in conjuction with an operator that changes the state of a tuple (ie, HBASEGet feeds a functor that increments a value, and then puts the tuple back into HBase with HBASEPut"
			+ "you could get unexpected behavior."
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
	public static final String MIN_TIMESTAMP_DESC = "The minimum timestamp to be used for queries.  No entries with a timestamp older than this value will be returned.  Note that unless you set maxVersions, you will get either only one entry in this time range.";
	public static final String MAX_VERSIONS_PARAM_NAME = "maxVersions";
	public static final String MAX_VERSIONS_DESC = "The maximum number of versions returned.  Defaults to one.  A value of 0 means get all versions.";
	private OutputMode outputMode;
	private static final String defaultOutAttrName = "value";
	private String outAttrName = defaultOutAttrName;;
	private String successAttr = null;
	private SingleOutputMapper primativeOutputMapper = null;

	@Parameter(name = SUCCESS_PARAM_NAME, description = "Name of attribute of the output port in which to put the count of values populated.", optional = true)
	public void setSuccessAttr(String name) {
		successAttr = name;
	}

	@Parameter(name = OUT_PARAM_NAME, description = "Name of the attribute of the output port in which to put the result of the get.  Its type depends on whether a columnFamily or columnQualifier was set.", optional = true)
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
			checker.setInvalidContext(
					"Wrong number of outputs; expected 1 found " + numOut,
					new Object[0]);
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
				logger.info("Setting time range to [" + minTimestamp + ", "
						+ Long.MAX_VALUE + ")");
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
		HTableInterface myTable = connection.getTable(tableNameBytes);
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
