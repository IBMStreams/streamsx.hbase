/* Copyright (C) 2013-2018, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
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

@PrimitiveOperator(name = "HBASEPut", namespace = "com.ibm.streamsx.hbase", description = "The `HBASEPut` operator puts tuples into an Hbase table. It includes support for checkAndPut.  If the value is a primitive type, a Put method must have a row, columnFamily, columnQualifier,"
		+ "and value specified.  The row and value are derived from the input tuple, which is specified by the "
		+ HBASEOperator.ROW_PARAM_NAME
		+ " and "
		+ HBASEPut.VALUE_NAME
		+ " parameters.  The columnFamily and "
		+ "columnQualifier can be specified in the same way, by using the "
		+ HBASEOperatorWithInput.COL_FAM_PARAM_NAME
		+ " and "
		+ HBASEOperatorWithInput.COL_QUAL_PARAM_NAME
		+ " parameters. Alternatively, they can be the same for all "
		+ "tuples, by setting the "
		+ HBASEOperator.STATIC_COLF_NAME
		+ " and "
		+ HBASEOperator.STATIC_COLQ_NAME
		+ " parameters. "
	        + HBASEOperator.DOC_BLANKLINE
		+ "Here is an example: \\n"             
+"               () as allSink = HBASEPut(full)\\n"
+"                {\\n"
+"                        param\\n"
+"                                tableName : \\\"streamsSample_lotr\\\" ;\\n"
+"                                rowAttrName : \\\"character\\\" ;\\n"
+"                                columnFamilyAttrName : \\\"colF\\\" ;\\n"
+"                                columnQualifierAttrName : \\\"colQ\\\" ;\\n"
+"                                valueAttrName : \\\"value\\\" ;\\n"
+"                }\\n"

		+ "If the value is a tuple type, then the attribute names of the tuple are interpreted as the columnQualifiers "
		+ " for the correponding values.  Here is an snippet from the PutRecord sample application."
+" We create the toHBASE stream: \\n"
+"                stream<rstring key, tuple<rstring title, rstring author_fname,\\n"
+"                        rstring author_lname, rstring year, rstring rating> bookData> toHBASE =\\n"
+"                        Functor(bookStream)\\n"
+"                {\\n"
+"                      //// ...\\n"
+"                }\\n"
+" Then we can use HBASEPut as follows:\\n"
+"                () as putsink = HBASEPut(toHBASE)\\n"
+"                {\\n"
+"                        param\\n"
+"                                rowAttrName : \\\"key\\\" ;\\n"
+"                                tableName : \\\"streamsSample_books\\\" ;\\n"
+"                                staticColumnFamily : \\\"all\\\" ;\\n"
+"                                valueAttrName : \\\"bookData\\\" ;\\n"
+"                }\\n"
+HBASEOperator.DOC_BLANKLINE
		+ "To support locking, HBASE supports a conditional put operation.  This operator supports that operation "
		+ "by using the "
		+ HBASEPutDelete.CHECK_ATTR_PARAM
		+ " parameter.  If that parameter is set, then the input "
		+ "attribute it refers to must be a valid check type.  For more information, see the parameter description. "
		+ "On a put operation, the condition is checked."
		+ "If it passes, the put operation happens; if not, the put operation fails.  To check the success or failure of the "
		+ "put operation, use an optional output port.  The attribute that is specified in the "
		+ HBASEPutDelete.SUCCESS_PARAM
		+ " parameter on the output "
		+ "port is set to true if the put operation occurs, and false otherwise."
		+ HBASEPut.consistentCutInfo + HBASEOperator.commonDesc)
@InputPorts({ @InputPortSet(description = "Tuple to put into HBASE", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({ 
	@OutputPortSet(description = "Optional port for success or failure information.", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving),
	@OutputPortSet(description = "Optional port for error information. This port submits error message when an error occurs while HBase actions.", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Preserving) })

@Icons(location32 = "impl/java/icons/HBASEPut_32.gif", location16 = "impl/java/icons/HBASEPut_16.gif")
public class HBASEPut extends HBASEPutDelete {

	public static final String consistentCutInfo = HBASEOperator.consistentCutIntroducer
			+ "The `HBASEPut` operator can be in a consistent region, but it cannot be the start of a consistent region.\\n"
			+ "At drain points, it flushes its internal buffer. At resets, it clears its internal buffer."
			+ "The operator ensures at-least-once tuple processing, but does not guarentee exactly-once tuple processing. "
			+ "If there is a reset, the same entry might be put twice. "
			+ "If you use this operator with the `HBASEGet` operator to do a get, modify, and put operation on the same entry in a consistent region, you could end up doing"
			+ "the modification twice.  That scenario is not recommended.\\n"
			+ "If you need exactly-once tuple processing, it might be possible to use checkAndPut with sequence numbers.";

	List<Put> putList;

	private enum PutMode {
		ENTRY, RECORD
	};

	private PutMode putMode = null;
	protected String valueAttr = null;
	final static String VALUE_NAME = "valueAttrName";
	protected byte[][] qualifierArray = null;
	protected MetaType[] attrType = null;
	protected boolean bufferTransactions = false;

	protected Object tableLock = new Object();
	public static final String BUFFER_PARAM = "enableBuffer";

	@Parameter(name = BATCHSIZE_NAME, optional = true, description = "**This parameter has been deprecated as of Streams 4.2.0**.  The **" +BUFFER_PARAM+"** parameter should be used instead.  The **batchSize** parameter indicates the maximum number of Puts to buffer before sending to HBase.  Larger numbers are more efficient, but increase the risk of lost changes on operator crash.  In a consistent region, a drain flushes the buffer to HBase.")
	public void setBatchSize(int _size) {
		batchSize = _size;
	}
	
	@Parameter(name= BUFFER_PARAM, optional = true, description = "When set to true, this parameter can improve the performance of the operator because tuples received by the operator will not be immediately forwarded to the HBase server. "+ 
	"The buffer is flushed and the tuples are sent to HBase when one of the following three conditions is met: " +
	"the buffer's size limit is reached, or a window marker punctuation is received by the operator, or, during a drain operation if the operator is present "+
	"in a consistent region.  The buffer size is set in `hbase-site.xml` using the `hbase.client.write.buffer` property. "+
	" Note that although enabling buffering improves performance, there is a risk of data loss if there is a system failure before the buffer is flushed. "
	+"By default, **" + BUFFER_PARAM + "** is set to false.  This parameter cannot be combined with the **" + CHECK_ATTR_PARAM + "** parameter, since checkAndPut operations in HBase are not buffered.")
	public void setEnableBuffering(boolean bufferTuples) {
		bufferTransactions = bufferTuples;
	}
	
	
	
	@Parameter(name = VALUE_NAME, optional = false, description = "This parameter specifies the name of the attribute that contains the value that is put into the table.")
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
		if (!checker.checkExcludedParameters(CHECK_ATTR_PARAM, BUFFER_PARAM)){
			checker.setInvalidContext(Messages.getString("HBASE_PUT_INVALID_PARAM", CHECK_ATTR_PARAM, BUFFER_PARAM), null);
		}
	}
	

	/**
	 * Check if the batchSize parameter is used, if so, issue a warning.
	 * @param checker
	 */
	@ContextCheck(compile=true)
	public static void checkForBatchSizeParam(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// The hbase site must either be specified by a parameter, or we must look it up relative to an environment variable.
		 
		if ((!checker.checkExcludedParameters(BATCHSIZE_NAME,BUFFER_PARAM))){
		 	checker.setInvalidContext("The " + BATCHSIZE_NAME + " has been deprecated and should not be used.  Use the " +BUFFER_PARAM  + " parameter instead.", null);
		} else if (context.getParameterNames().contains(BATCHSIZE_NAME)) {
			System.err.println("The " + BATCHSIZE_NAME + " has been deprecated and should not be used.  Use the " +BUFFER_PARAM  + " parameter instead.");
		}
		
	}

	Logger logger = Logger.getLogger(this.getClass());
	/**
	 * When enableBuffer is true and we are disabling autoflush,
	 * keep a pointer to the hbase table. This value will be null otherwise
	 */
	private Table cachedTable = null;
	

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
//		Tuple tuple = inputPort.
		StreamSchema schema = inputPort.getStreamSchema();
		Tuple tuple = schema.getTuple();
		
		if (bufferTransactions) {
    		Logger.getLogger(this.getClass()).trace(Messages.getString("HBASE_PUT_DISABLING_FLUSH"));
    		cachedTable= getHTable(tuple);
  //  		cachedTable.setAutoFlush(false, true);
    	}

		
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
		Table myTable = null;
		if (!bufferTransactions) {
			try {
				myTable = getHTable(tuple);
			} catch (TableNotFoundException e) {
				logger.error(e.getMessage());
			}		
		}
		
		if (myTable != null) {
			try {
				switch (putMode) {
		
				case ENTRY:
					byte colQ[] = getColumnQualifier(tuple);
					byte value[] = getBytes(tuple, valueAttrIndex, valueAttrType);
					myPut.addColumn(colF, colQ, value);
					break;
				case RECORD:
					Tuple values = tuple.getTuple(valueAttr);
					for (int i = 0; i < qualifierArray.length; i++) {
						myPut.addColumn(colF, qualifierArray[i],
								getBytes(values, i, attrType[i]));
					}
					break;
				default:
					// It should be impossible to get here.
					throw new Exception("Unsupported Put type");
				}
						
				if (successAttrName != null) {
					byte checkRow[] = getRow(tuple);
					if (checkAttr != null) {
						Tuple checkTuple = tuple.getTuple(checkAttrIndex);
						// the row attribute and the check row attribute have to match, so
						// don't even look
						// in the check attribute for the row.
						byte checkColF[] = getBytes(checkTuple, checkColFIndex, checkColFType);
						byte checkColQ[] = getBytes(checkTuple, checkColQIndex, checkColQType);
						byte checkValue[] = getCheckValue(checkTuple);
			
						success = myTable.checkAndPut(checkRow, checkColF, checkColQ, checkValue, myPut);
					}else{
						// set the success value without checkTuple
						byte checkColQ[] = getColumnQualifier(tuple);
						byte checkColF[] = getColumnFamily(tuple);
						byte checkValue[] = getValue(tuple);
						success = myTable.checkAndPut(checkRow,checkColF, checkColQ, checkValue,  myPut);
					}
					logger.debug(Messages.getString("HBASE_PUT_RESULT", success));
													
				} else if (!bufferTransactions && batchSize == 0) {
					
						myTable.put(myPut);
				} else if (bufferTransactions){
					safePut(myPut);
				} else {
					synchronized (listLock) {
						putList.add(myPut);
						if (putList.size() >= batchSize) {
							logger.debug(Messages.getString("HBASE_PUT_SUBMITTING_BATCH"));
							myTable.put(putList);
							putList.clear();
						}
					}
				}
				if (!bufferTransactions){
					myTable.close();
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				submitErrorMessagee(e.getMessage());
			}
		}
		// Checks to see if an output tuple is necessary, and if so,
		// submits it.
		submitOutputTuple(tuple, success);
	}

	/**
	 * Since the table is cached we need to make sure access to it are thread safe.
	 */
	protected void safePut(Put p) throws IOException {
		synchronized (tableLock) {
			if (cachedTable != null) {
				cachedTable.put(p);
				}
			}
	}
	
	@Override
	public void shutdown() throws Exception {
		flushBuffer();
		if (cachedTable != null) {
			cachedTable.close();
			cachedTable = null;
		}
		super.shutdown();
			
	}
	
	/***
	 * Safely flush the underlying HBase buffer 
	 * @throws IOException 
	 */
	protected void safeFlush() throws IOException {
		if (connection != null && !connection.isClosed()) {
			synchronized (tableLock) {
				logger.debug(Messages.getString("HBASE_PUT_COMMIT_FLUSH"));
				
				
			    try( final BufferedMutator mutator = connection.getBufferedMutator(cachedTable.getName());) {
			            mutator.flush();
			        } catch(Exception ex) {
			            final String errorMsg = String.format("Failed with a [%s] when writing to table [%s] ", ex.getMessage(),
			            		cachedTable.getName().getNameAsString());
			            throw new IOException(errorMsg, ex);
			        }
			}
		}
	}
	
	/**
	 * Empty the buffer. Called by shutdown and processPunctuation.
	 */
	@Override
	protected   void flushBuffer() throws IOException {
		if (connection != null && !connection.isClosed()) {
			if (bufferTransactions && cachedTable != null) {
				safeFlush();
			}
			if (batchSize > 0) {
				flushInternalBuffer();
			}
		}
	}
	
	/**
	 * Flush the internal list of puts we keep when using our own batch puts instead of autoflush
	 */
	protected synchronized void flushInternalBuffer() throws IOException {
		Table table = getHTable();
		synchronized (listLock) {
			if (table != null && putList != null && putList.size() > 0) {
				logger.debug(Messages.getString("HBASE_PUT_EMPTING_BUFFER"));
				table.put(putList);
				table.close();
			}
		}
	}

	@Override
	protected void clearBuffer() throws IOException{
		if (batchSize > 0) {
			clearInternalBuffer();
		}
		if (bufferTransactions) {
			logger.debug(Messages.getString("HBASE_PUT_ATTEMPTING"));
			safeFlush();
		}
	}
	
	protected synchronized void clearInternalBuffer() {
		if (connection != null && !connection.isClosed()) {
			synchronized (listLock) {
				if (putList != null && putList.size() > 0) {
					putList.clear();
				}
			}
		}
	}
	
	
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream,
			Punctuation mark) throws Exception {
		if (bufferTransactions && Punctuation.WINDOW_MARKER == mark) {
			safeFlush();
		} else if (Punctuation.FINAL_MARKER == mark && batchSize > 0) {
			flushBuffer();
		}
	}

	
 

}