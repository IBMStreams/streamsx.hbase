/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
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
import com.ibm.streams.operator.state.ConsistentRegionContext;

/**
 * Accepts tuples on input stream and makes the corresponding delete in the
 * HBASE table. .
 * <P>
 */
@PrimitiveOperator(name = "HBASEDelete", namespace = "com.ibm.streamsx.hbase", description = "Delete an entry, an entire row, a columnFamily in a row, or a columnFamily, columnQualifier pair in a row from HBASE, with optional checkAndDelete.  The mode in which the operator is working depends on the parameters.  To delete an entire row, specify only the row.  To delete a columnFamily, specify the row and the columnFamily (either via the staticColumnFamily parameter or the columnFamilyAttrName parameter), and to delete just a single entry, specify the row, columnFamily, and columnQualifier (either via the staticColumnQualifer or the columnQualiferAttrName parameter).  To support locking, HBASE allows for a conditional delete.  To use the conditional delete, you must set "
		+ HBASEPutDelete.CHECK_ATTR_PARAM
		+ " which gives the attribute on the input port containing a the tuple that describes the check.  If the check fails, the delete isn't done  To distinguish between failed and successful deletes, you can have an optional output port. The attribute of the output tuple give by "
		+ HBASEPutDelete.SUCCESS_PARAM
		+ " is set to true if the delete succeeded, and false otherwise."
		+ HBASEDelete.consistentCutInfo + HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Representation of tuple to delete", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Copies tuple from input, setting "
		+ HBASEPutDelete.SUCCESS_PARAM + " if "
		+ HBASEPutDelete.CHECK_ATTR_PARAM + " is specified", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
@Icons(location32 = "impl/java/icons/HBASEDelete_32.gif", location16 = "impl/java/icons/HBASEDelete_16.gif")
public class HBASEDelete extends HBASEPutDelete {

	public static final String consistentCutInfo = HBASEOperator.consistentCutIntroducer
			+ "HBASEDelete may be in a consistent region, but it may not be the start of a consistent region.\\n"
			+ "When in a consistent region, deleteAllVersions must either be unspecified or set to true. "
			+ "HBASEDelete ensures at-least-once tuple processing, but does not guarentee exactly-once tuple processing; thus if there is a reset, the "
			+ "same entry may be deleted twice.  "
			+ "At drain points, it flushes its internal buffer, and at resets, in clears its internal buffer.";

	private enum DeleteMode {
		ROW, COLUMN_FAMILY, COLUMN
	};

	DeleteMode deleteMode = null;

	List<Delete> deleteList = null;
	org.apache.log4j.Logger logger = Logger.getLogger(this.getClass());
	private static final String DELETE_ALL_PARAM_NAME = "deleteAllVersions";
	boolean deleteAll = true;

	@Parameter(name = DELETE_ALL_PARAM_NAME, optional = true, description = "Defaults to true.  If true, delete all versions of a cell.  If false, delete only the most recent.")
	public void setDeleteAll(boolean _delete) {
		deleteAll = _delete;
	}

	/**
	 * deleteAll only has an effect when a single cell is being deleted, so
	 * let's make sure no one is misusing it. To do this, we make sure a
	 * columnQualifier is specified, either via an operator parameter or via the
	 * tuple.
	 * 
	 * @param checker
	 */
	@ContextCheck(compile = true)
	public static void checkDeleteAll(OperatorContextChecker checker) {
		HBASEPutDelete.compileTimeChecks(checker, "HBASEDelete");
		OperatorContext context = checker.getOperatorContext();
		Set<String> params = context.getParameterNames();
		if (params.contains(DELETE_ALL_PARAM_NAME)) {
			if (params.contains(HBASEOperator.STATIC_COLQ_NAME)
					|| params.contains(HBASEPutDelete.COL_QUAL_PARAM_NAME)) {
				// we're okay--
			} else {
				checker.setInvalidContext("Parameter " + DELETE_ALL_PARAM_NAME
						+ " requires that either "
						+ HBASEOperator.STATIC_COLQ_NAME + " or "
						+ HBASEPutDelete.COL_QUAL_PARAM_NAME + " be set.", null);
			}
		}
	}

	@ContextCheck(runtime = true, compile = false)
	public static void runtimeChecks(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		Set<String> params = context.getParameterNames();
		if (params.contains(DELETE_ALL_PARAM_NAME)) {
			boolean deleteAll = Boolean.parseBoolean(context
					.getParameterValues(DELETE_ALL_PARAM_NAME).get(0));
			if (!deleteAll) {
				// if we're not deleting everything, make sure we're not in a
				// consistent region.
				ConsistentRegionContext ccContext = checker
						.getOperatorContext().getOptionalContext(
								ConsistentRegionContext.class);
				if (ccContext != null) {
					checker.setInvalidContext(
							"When in a consistent region {0} must be true for {1}",
							new Object[] { DELETE_ALL_PARAM_NAME, "HBASEDelete" });
				}
			}
		}
	}

	/**
	 * Setup for execution. Parameter checking is set in the parent class. Sets
	 * deleteMode based on the set of input parameters.
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
		if (batchSize > 0) {
			deleteList = new ArrayList<Delete>(batchSize);
		}
		deleteMode = DeleteMode.ROW;
		if (colFamBytes != null || columnFamilyAttr != null) {
			if (colQualBytes != null || columnQualifierAttr != null) {
				deleteMode = DeleteMode.COLUMN;
			} else
				deleteMode = DeleteMode.COLUMN_FAMILY;
		}
	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
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
		HTableInterface myTable = connection.getTable(tableNameBytes);
		byte row[] = getRow(tuple);
		Delete myDelete = new Delete(row);

		if (DeleteMode.COLUMN_FAMILY == deleteMode) {
			byte colF[] = getColumnFamily(tuple);
			myDelete.deleteFamily(colF);
		} else if (DeleteMode.COLUMN == deleteMode) {
			byte colF[] = getColumnFamily(tuple);
			byte colQ[] = getColumnQualifier(tuple);
			if (deleteAll) {
				myDelete.deleteColumns(colF, colQ);
			} else {
				myDelete.deleteColumn(colF, colQ);
			}
		}

		boolean success = false;
		if (checkAttr != null) {
			Tuple checkTuple = tuple.getTuple(checkAttrIndex);
			// the check row and the row have to match, so don't use the
			// checkRow.
			byte checkRow[] = getRow(tuple);
			byte checkColF[] = getBytes(checkTuple, checkColFIndex,
					checkColFType);
			byte checkColQ[] = getBytes(checkTuple, checkColQIndex,
					checkColQType);
			byte checkValue[] = getCheckValue(checkTuple);
			success = myTable.checkAndDelete(checkRow, checkColF, checkColQ,
					checkValue, myDelete);
		} else if (batchSize == 0) {
			logger.debug("Deleting " + myDelete);
			myTable.delete(myDelete);
		} else {
			synchronized (listLock) {
				deleteList.add(myDelete);
				if (deleteList.size() >= batchSize) {
					myTable.delete(deleteList);
					deleteList.clear();
				}
			}
		}

		// Checks to see if an output tuple is necessary, and if so,
		// submits it.
		submitOutputTuple(tuple, success);
		myTable.close();
	}

	/**
	 * WE do not synchronize this method, because we already have a lock on the
	 * accesses to the delete list.
	 * 
	 */
	@Override
	protected void flushBuffer() throws IOException {
		if (connection != null && !connection.isClosed()) {
			HTableInterface myTable = connection.getTable(tableNameBytes);
			if (myTable != null && deleteList != null && deleteList.size() > 0) {
				synchronized (listLock) {
					if (deleteList != null && deleteList.size() > 0) {
						myTable.delete(deleteList);
					}
				}
			}
		}
	}

	/**
	 * Clear the list of pending deletes. Called by reset and
	 * resetToInitialState. Any deletes that have already been sent are not
	 * undone.
	 */
	@Override
	protected void clearBuffer() {
		synchronized (listLock) {
			if (deleteList != null && deleteList.size() > 0) {
				deleteList.clear();
			}
		}

	}
}
