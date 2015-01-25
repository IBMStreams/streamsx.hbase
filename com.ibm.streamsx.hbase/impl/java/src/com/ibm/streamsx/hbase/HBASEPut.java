/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.TupleType;
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

/**
 * Class for an operator that consumes tuples and does not produce an output
 * stream.
 * 
 */

@PrimitiveOperator(name = "HBASEPut", namespace = "com.ibm.streamsx.hbase", description = "Put tuples in HBASE, with support for checkAndPut.  In the value is a primitive type, a Put must have a row, columnFamily, columnQualifier,"
		+ "and value specified.  The row and value are from the input tuple, specified by the attribute "
		+ HBASEOperator.ROW_PARAM_NAME
		+ " and "
		+ HBASEPut.VALUE_NAME
		+ ", respectively.  The columnFamily and "
		+ "columnQualifier may be specified in the same way (via "
		+ HBASEOperatorWithInput.COL_FAM_PARAM_NAME
		+ " and "
		+ HBASEOperatorWithInput.COL_QUAL_PARAM_NAME
		+ " respectively), or they may be the same for all "
		+ "tuples, by setting "
		+ HBASEOperator.STATIC_COLF_NAME
		+ " and "
		+ HBASEOperator.STATIC_COLQ_NAME
		+ ".  Currently "
		+ "all must be of type rstring. "
		+ "If the value is a tuple type, then attribute names of the tuple will be interpreted as the columnQualifiers "
		+ " for the correponding values.  See the PutRecord sample application for an example."
		+ "To allow for locking, HBASE supports a conditional put.  That is supported "
		+ "in this operator via the "
		+ HBASEPutDelete.CHECK_ATTR_PARAM
		+ ".  If that parameter is set, then the input "
		+ "attribute it refers to must be a valid check type--see the description for details.  On a put, the condition"
		+ "is checked.  If it passes, the put happens, if not, the put fails.  To check the success or failure of the "
		+ "put, the operator has an optional output port.  The attribute "
		+ HBASEPutDelete.SUCCESS_PARAM
		+ " on the output "
		+ "port will be set to true if the put happens, and false otherwise."
		+ HBASEPut.consistentCutInfo + HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Tuple to put into HBASE", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Optional port for success or failure information.", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
@Icons(location32 = "impl/java/icons/HBASEPut_32.gif", location16 = "impl/java/icons/HBASEPut_16.gif")
public class HBASEPut extends HBASEPutDelete {

	public static final String consistentCutInfo = HBASEOperator.consistentCutIntroducer
			+ "HBASEPut may be in a consistent region, but it may not be the start of a consistent region.\\n"
			+ "At drain points, it flushes its internal buffer, and at resets, in clears its internal buffer."
			+ "Thus, it ensures at-least-once tuple processing, but does not guarentee exactly-once tuple processing; thus if there is a reset, the "
			+ "same entry may be put twice.  If used with HBASEGet to do a Get, modify, Put on the same entry in a consistent region, you could end up doing"
			+ "the modification twice, so that is not recommended.\\n"
			+ "If you need exactly-once tuple processing, using checkAndPut along with sequence numbers may be possible.";

	List<Put> putList;

	private enum PutMode {
		ENTRY, RECORD
	};

	private PutMode putMode = null;
	protected String valueAttr = null;
	final static String VALUE_NAME = "valueAttrName";
	protected byte[][] qualifierArray = null;
	protected MetaType[] attrType = null;
	private int valueAttrIndex = -1;
	private MetaType valueAttrType = null;

	@Parameter(name = VALUE_NAME, optional = false, description = "Name of the attribute containing the value to put into the table")
	public void setValueAttr(String val) {
		valueAttr = val;
	}

	/**
	 * Do any necessary compile time checks. It calls the checker of the super
	 * class.
	 * 
	 * @param checker
	 */
	@ContextCheck(compile = true)
	public static void checkDeleteAll(OperatorContextChecker checker) {
		HBASEPutDelete.compileTimeChecks(checker, "HBASEPut");
	}

	Logger logger = Logger.getLogger(this.getClass());

	/**
	 * Initialize this operator. Create the list to store the batch.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		if (batchSize > 0) {
			putList = new ArrayList<Put>(batchSize);
		}
		StreamingInput<Tuple> inputPort = context.getStreamingInputs().get(0);
		StreamSchema schema = inputPort.getStreamSchema();
		Attribute attr = schema.getAttribute(valueAttr);

		if (attr.getType().getMetaType() == MetaType.TUPLE) {
			// In this mode, we treat the attribute name as the column qualifer.
			putMode = PutMode.RECORD;
			// Let's get all the attribute names, and store them in the
			// qualifier array.
			StreamSchema valueSchema = ((TupleType) attr.getType())
					.getTupleSchema();
			qualifierArray = new byte[valueSchema.getAttributeCount()][];
			attrType = new MetaType[valueSchema.getAttributeCount()];
			for (int i = 0; i < valueSchema.getAttributeCount(); i++) {
				qualifierArray[i] = valueSchema.getAttribute(i).getName()
						.getBytes(charset);
				attrType[i] = valueSchema.getAttribute(i).getType()
						.getMetaType();
			}
		} else {
			valueAttrIndex = attr.getIndex();
			valueAttrType = attr.getType().getMetaType();
			putMode = PutMode.ENTRY;
		}
	}

	/**
	 * Process an incoming tuple. Either put it on the put list, or call the
	 * HBASE put.
	 * 
	 * @param stream
	 *            Port the tuple is arriving on.
	 * @param tuple
	 *            Object representing the incoming tuple.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {

		byte row[] = getRow(tuple);
		byte colF[] = getColumnFamily(tuple);
		boolean success = false;
		Put myPut = new Put(row);

		switch (putMode) {

		case ENTRY:
			byte colQ[] = getColumnQualifier(tuple);
			byte value[] = getBytes(tuple, valueAttrIndex, valueAttrType);
			myPut.add(colF, colQ, value);
			break;
		case RECORD:
			Tuple values = tuple.getTuple(valueAttr);
			for (int i = 0; i < qualifierArray.length; i++) {
				myPut.add(colF, qualifierArray[i],
						getBytes(values, i, attrType[i]));
			}
			break;
		default:
			// It should be impossible to get here.
			throw new Exception("Unsupported Put type");
		}
		HTableInterface myTable = connection.getTable(tableNameBytes);
		if (checkAttr != null) {
			Tuple checkTuple = tuple.getTuple(checkAttrIndex);

			// the row attribute and the check row attribute have to match, so
			// don't even look
			// in the check attribute for hte row.
			byte checkRow[] = getRow(tuple);
			byte checkColF[] = getBytes(checkTuple, checkColFIndex,
					checkColFType);
			byte checkColQ[] = getBytes(checkTuple, checkColQIndex,
					checkColQType);
			byte checkValue[] = getCheckValue(checkTuple);

			success = myTable.checkAndPut(checkRow, checkColF, checkColQ,
					checkValue, myPut);
			logger.debug("Result is " + success);
		} else if (batchSize == 0) {
			myTable.put(myPut);
		} else {
			synchronized (listLock) {
				putList.add(myPut);
				if (putList.size() >= batchSize) {
					myTable.put(putList);
					putList.clear();
				}
			}
		}
		// Checks to see if an output tuple is necessary, and if so,
		// submits it.
		submitOutputTuple(tuple, success);
		myTable.close();
	}

	/**
	 * Empty the buffer. Called by shutdown and processPunctuation.
	 */
	@Override
	protected synchronized void flushBuffer() throws IOException {
		if (connection != null && !connection.isClosed()) {
			HTableInterface myTable = connection.getTable(tableNameBytes);
			synchronized (listLock) {
				if (myTable != null && putList != null && putList.size() > 0) {
					myTable.put(putList);
				}
			}
			myTable.close();
		}
	}

	@Override
	protected synchronized void clearBuffer() {
		synchronized (listLock) {
			if (putList != null && putList.size() > 0) {
				putList.clear();
			}
		}
	}

}
