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
 * Accepts tuples on input stream and makes the corresponding delete in the 
 * HBASE table.  .
 * <P>
 */
@PrimitiveOperator(name = "HBASEDelete", namespace = "com.ibm.streamsx.hbase", description = "Delete an entry, an entire row, a columnFamily in a row, or a columnFamily, columnQualifier pair in a row from HBASE, with optional checkAndDelete.  The mode in which the operator is working depends on the parameters.  To delete an entire row, specify only the row.  To delete a columnFamily, specify the row and the columnFamily (either via the staticColumnFamily parameter or the columnFamilyAttrName parameter), and to delete just a single entry, specify the row, columnFamily, and columnQualifier (either via the staticColumnQualifer or the columnQualiferAttrName parameter).  To support locking, HBASE allows for a conditional delete.  To use the conditional delete, you must set "+HBASEPutDelete.CHECK_ATTR_PARAM+" which gives the attribute on the input port containing a the tuple that describes the check.  If the check fails, the delete isn't done  To distinguish between failed and successful deletes, you can have an optional output port. The attribute of the output tuple give by "+HBASEPutDelete.SUCCESS_PARAM+" is set to true if the delete succeeded, and false otherwise.")
@InputPorts({ @InputPortSet(description = "Representation of tuple to delete", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ @OutputPortSet(description = "Copies tuple from input, setting "+HBASEPutDelete.SUCCESS_PARAM+" if "+HBASEPutDelete.CHECK_ATTR_PARAM+" is specified", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })
public class HBASEDelete extends HBASEPutDelete {

	private enum DeleteMode {
		ROW, COLUMN_FAMILY, COLUMN
	};

	DeleteMode deleteMode = null;

	List<Delete> deleteList = null;
	org.apache.log4j.Logger logger = Logger.getLogger(this.getClass());



	/**
	 * Setup for execution. Parameter checking is set in the parent class.
	 * Sets deleteMode based on the set of input parameters.
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
	              if (myTable != null && deleteList != null && deleteList.size() > 0) {
	            	  synchronized (listLock) {
	            		  if (deleteList != null && deleteList.size() >0) { 
	            			  myTable.delete(deleteList);
	            		  }
	            	  }
	            }
	
	               // Must call super.shutdown()
	               super.shutdown();
	       }
	

}
