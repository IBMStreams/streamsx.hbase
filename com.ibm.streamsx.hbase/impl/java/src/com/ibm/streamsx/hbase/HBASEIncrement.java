/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.ConsistentRegionContext;

/**
 * Increment a particular HBASE entry. The row, columnFamily, and
 * columnQualifier must all be specified, either as parameters or they must come
 * from the tuples.
 */

@PrimitiveOperator(name = "HBASEIncrement", namespace = "com.ibm.streamsx.hbase", description = "Increment the specified HBASE entry.  Uses the HTable.increment.  The value to increment by may be specified as an operator parameter or as an attribute in the input tuple."
		+ HBASEIncrement.CONSISTENT_REGION_INFO
		+ HBASEOperator.DOC_BLANKLINE
		+ HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Tuples describing entry to increment", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@Icons(location32 = "impl/java/icons/HBASEIncrement_32.gif", location16 = "impl/java/icons/HBASEIncrement_16.gif")
public class HBASEIncrement extends HBASEOperatorWithInput {

	static final String CONSISTENT_REGION_INFO = HBASEOperator.consistentCutIntroducer
			+ " HBASEIncrement is not allowed in a consistent region.";
	String incrAttr = null;
	MetaType incrAttrType = null;
	int incrAttrIndex = -1;
	protected long defaultIncr = 1;
	private static final String INCREMENT_ATTR_PARAM = "incrementAttrName";
	private static final String STATIC_INCREMENT_VALUE = "increment";

	@Parameter(name = INCREMENT_ATTR_PARAM, optional = true, description = "Attribute to be used to determine the increment. Cannot be used with "
			+ STATIC_INCREMENT_VALUE)
	public void setIncrAttr(String name) {
		incrAttr = name;
	}

	@Parameter(name = STATIC_INCREMENT_VALUE, optional = true, description = "Value by which to increment.  Cannot be specified with "
			+ INCREMENT_ATTR_PARAM)
	public void setIncr(long _inc) {
		defaultIncr = _inc;
	}

	@ContextCheck(compile = true)
	public static void checkIncrement(OperatorContextChecker checker) {
		checker.checkExcludedParameters(INCREMENT_ATTR_PARAM,
				STATIC_INCREMENT_VALUE);
		checker.checkExcludedParameters(STATIC_INCREMENT_VALUE,
				INCREMENT_ATTR_PARAM);

		// Now we check whether we're in a consistent region.
		ConsistentRegionContext ccContext = checker.getOperatorContext()
				.getOptionalContext(ConsistentRegionContext.class);
		if (ccContext != null) {
			checker.setInvalidContext(getNoCCString(),
					new Object[] { "HBASEIncrement" });
		}
	}

	/**
	 * Checks that a row, columnFamily, and columnQualifier are all specified,
	 * either in the tuple or as static values. Checks that the increment
	 * attribute, if specified, is a valid attribute and a usable type.
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
		Logger.getLogger(this.getClass()).trace(
				"Operator " + context.getName() + " initializing in PE: "
						+ context.getPE().getPEId() + " in Job: "
						+ context.getPE().getJobId());

		if (incrAttr != null) {
			StreamingInput<Tuple> input = context.getStreamingInputs().get(0);
			StreamSchema inputSchema = input.getStreamSchema();
			Attribute attr = inputSchema.getAttribute(incrAttr);
			if (attr == null) {
				throw new Exception("Expected to find " + incrAttr
						+ " in input tuple, but did not");
			}
			incrAttrIndex = attr.getIndex();
			incrAttrType = attr.getType().getMetaType();
			if (MetaType.INT16 != incrAttrType
					&& MetaType.INT32 != incrAttrType
					&& MetaType.INT64 != incrAttrType) {
				throw new Exception("Incrementing with attributes of type "
						+ incrAttrType
						+ " not supported; use int16, int32, or int645");
			}
		}
	}

	/**
	 * Increment the HBASE entry.
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

		byte row[] = getRow(tuple);
		byte colF[] = getColumnFamily(tuple);
		byte colQ[] = getColumnQualifier(tuple);

		long incr = defaultIncr;
		if (incrAttrIndex > 0) {
			if (incrAttrType == MetaType.INT16) {
				incr = tuple.getShort(incrAttrIndex);
			} else if (incrAttrType == MetaType.INT32) {
				incr = tuple.getInt(incrAttrIndex);
			} else if (incrAttrType == MetaType.INT64) {
				incr = tuple.getLong(incrAttrIndex);
			}
		}
		HTableInterface myTable = connection.getTable(tableNameBytes);
		long newValue = myTable.incrementColumnValue(row, colF, colQ, incr);
		myTable.close();
	}

}
