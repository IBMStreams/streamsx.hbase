/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.log4j.Logger;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Class for an operator that consumes tuples and does not produce an output
 * stream. This pattern supports a number of input streams and no output
 * streams.
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to
 * process and submit tuples</li>
 * <li>process() handles a tuple arriving on an input port
 * <li>processPuncuation() handles a punctuation mark arriving on an input port
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any
 * time, such as a request to stop a PE or cancel a job. Thus the shutdown() may
 * occur while the operator is processing tuples, punctuation marks, or even
 * during port ready notification.</li>
 * </ul>
 * <p>
 * With the exception of operator initialization, all the other events may occur
 * concurrently with each other, which lead to these methods being called
 * concurrently by different threads.
 * </p>
 */
@PrimitiveOperator(name = "HBASEDelete", namespace = "streamsx.bigdata.hbase", description = "Java Operator HBASEDelete")
@InputPorts({ @InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
public class HBASEDelete extends HBASEPutDelete {

	private enum DeleteMode {
		ROW, COLUMN_FAMILY, COLUMN
	};

	DeleteMode deleteMode = null;

	List<Delete> deleteList = null;

	/**
	 * Initialize this operator. Called once before any tuples are processed.
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
		// TODO add a proper check to make sure that column qualifer
		// is not used without column family

	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		// This method is commonly used by source operators.
		// Operators that process incoming tuples generally do not need this
		// notification.
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace(
				"Operator " + context.getName()
						+ " all ports are ready in PE: "
						+ context.getPE().getPEId() + " in Job: "
						+ context.getPE().getJobId());
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

		byte row[] = getRow(tuple);
		Delete myDelete = new Delete(row);

		if (DeleteMode.COLUMN_FAMILY == deleteMode) {
			byte colF[] = getColumnFamily(tuple);
			myDelete.deleteFamily(colF);
		} else if (DeleteMode.COLUMN == deleteMode) {
			byte colF[] = getColumnFamily(tuple);
			byte colQ[] = getColumnQualifier(tuple);
			myDelete.deleteColumns(colF, colQ);
		}

		boolean success = false;
		if (checkAttr != null) {
			Tuple checkTuple = tuple.getTuple(checkAttrIndex);
			// the check row and the row have to match, so don't use the
			// checkRow.
			byte checkRow[] = getRow(tuple);
			byte checkColF[] = getCheckColF(checkTuple);
			byte checkColQ[] = getCheckColQ(checkTuple);
			byte checkValue[] = getCheckValue(checkTuple);
			success = myTable.checkAndDelete(checkRow, checkColF, checkColQ,
					checkValue, myDelete);
		} else if (batchSize == 0) {
			System.out.println("Deleting " + myDelete);
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
	}

	/**
	 * Process an incoming punctuation that arrived on the specified port.
	 * 
	 * @param stream
	 *            Port the punctuation is arriving on.
	 * @param mark
	 *            The punctuation mark
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream,
			Punctuation mark) throws Exception {
		if (Punctuation.FINAL_MARKER == mark) {
			synchronized (listLock) {
				if (batchSize > 0 && myTable != null && deleteList != null
						&& deleteList.size() > 0) {
					myTable.delete(deleteList);
					deleteList = null;
				} else if (deleteList != null && deleteList.size() == 0) {
					deleteList = null;
				}
			}
		}
		super.processPunctuation(stream, mark);
	}

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace(
				"Operator " + context.getName() + " shutting down in PE: "
						+ context.getPE().getPEId() + " in Job: "
						+ context.getPE().getJobId());

		// Must call super.shutdown()
		super.shutdown();
	}

}
