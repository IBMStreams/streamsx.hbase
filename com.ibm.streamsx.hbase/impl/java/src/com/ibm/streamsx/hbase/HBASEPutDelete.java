/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.StateHandler;

/**
 * Class for common functions of Put and Delete HBASEPut and HBASEDelete have
 * some common functions not shared by get and increment. Support for these is
 * placed here.
 * 
 * This class handles: - * batchSize parameter * success attribute * check
 * attribute * outputting a tuple and indicating success or failure when check
 * attribute is one.
 * 
 * 
 */

public abstract class HBASEPutDelete extends HBASEOperatorWithInput implements
		StateHandler {

	// These are used by Put and Delete for checkAndPut and checkAndDelete
	protected int checkColFIndex = -1;
	protected int checkColQIndex = -1;
	protected int checkValueIndex = -1;

	protected MetaType checkColFType = null, checkColQType = null,
			checkValueType = null;

	final protected Object listLock = new Object();
	protected int batchSize = 0;

	static final String BATCHSIZE_NAME = "batchSize";

	protected String checkAttr = null;
	static final String CHECK_ATTR_PARAM = "checkAttrName";
	protected int checkAttrIndex = -1;
	static final String SUCCESS_PARAM = "successAttr";
	private String successAttrName = null;
	private int successAttrIndex = -1;
	StreamingOutput<OutputTuple> outStream = null;

	@Parameter(name = SUCCESS_PARAM, optional = true, description = "Attribute on the output port to be set to true if the check passes and the action is successful")
	public void setSuccessAttr(String name) {
		successAttrName = name;
	}

	@Parameter(name = BATCHSIZE_NAME, optional = true, description = "Number of mutations to received before sending the to HBASE.  Larger numbers are more efficient, but increase the risk of lost changes on operator crash.")
	public void setBatchSize(int _size) {
		batchSize = _size;
	}

	@Parameter(name = CHECK_ATTR_PARAM, optional = true, description = "Name of the attribute specifying the tuple to check for before applying the mutation.  It must have a row, columnFamily, and columnQualifier.  It may optionally have a value.  When a value is specified, the put or delete operation will only succeed when that entry specified by the check attribute exists.  When there is no value in the type of the checkAttribute, the put or delete operation will only succeed when there is no entry for that row, columnFamily, columnQualifer combination.")
	public void setCheckAttr(String name) {
		checkAttr = name;
	}

	/**
	 * Used for compile-time checks. Called by the subclass.
	 * 
	 * @param checker
	 */
	protected static void successRequiresOutput(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		Set<String> params = context.getParameterNames();
		if (params.contains(SUCCESS_PARAM)) {

			if (context.getStreamingOutputs().size() == 0) {
				checker.setInvalidContext(
						"Parameter {0} requires an output port",
						new Object[] { SUCCESS_PARAM });
			}
		}

	}

	/**
	 * Called by the subclass. This should invoke all checks common to HBASEPut
	 * and HBASEDelete.
	 * 
	 * @param checker
	 *            the operator context checker.
	 * @param operatorName
	 *            the name of the operator, used to generate error messages.
	 */
	protected static void compileTimeChecks(OperatorContextChecker checker,
			String operatorName) {
		// If successAttr is set, then we must be using checkAttrParam
		successRequiresOutput(checker);
		checker.checkDependentParameters(SUCCESS_PARAM, CHECK_ATTR_PARAM);
		checkConsistentRegionSource(checker, operatorName);

	}

	protected void establishCheckAttrMatching(Attribute checkAttr)
			throws Exception {
		if (checkAttr.getType().getMetaType() != MetaType.TUPLE) {
			throw new Exception("Check attribute must be of type tuple");
		}
		TupleType checkTuple = (TupleType) checkAttr.getType();
		StreamSchema checkSchema = checkTuple.getTupleSchema();
		if (checkSchema.getAttribute("row") != null) {
			Logger.getLogger(this.getClass())
					.warn(checkAttr.getName()
							+ ".row is ignored, as the row for the check must be the same as the row of the put.");
		}
		checkColQIndex = checkAndGetIndex(checkSchema, "columnQualifier");
		checkColQType = checkSchema.getAttribute(checkColQIndex).getType()
				.getMetaType();
		checkColFIndex = checkAndGetIndex(checkSchema, "columnFamily");
		checkColFType = checkSchema.getAttribute(checkColFIndex).getType()
				.getMetaType();
		if (checkSchema.getAttribute("value") != null) {
			checkValueIndex = checkAndGetIndex(checkSchema, "value");
			checkValueType = checkSchema.getAttribute(checkValueIndex)
					.getType().getMetaType();
		}
	}

	byte[] getCheckValue(Tuple tuple) throws Exception {
		if (checkValueIndex > 0) {
			return getBytes(tuple, checkValueIndex, checkValueType);
		} else
			return null;
	}

	/**
	 * This checks that
	 * <ul>
	 * <li>If checkAttr is specified, then batchSize must not be specified.
	 * <li>Checks that checkAttr, if specified, exists and is the right type
	 * <li>If success attribute is specified, checks that checkAttribute is also
	 * specified.
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

		if (batchSize > 0 && checkAttr != null) {
			// TODO make this proper context check!
			throw new Exception("Cannot use checkAttr with batchSize > 0");
		}

		StreamingInput<Tuple> input = context.getStreamingInputs().get(0);
		StreamSchema inputSchema = input.getStreamSchema();
		if (checkAttr != null) {
			Attribute attr = inputSchema.getAttribute(checkAttr);
			if (attr == null) {
				throw new Exception("Expected to find attribute with name "
						+ checkAttr + " but did not");
			}
			checkAttrIndex = attr.getIndex();
			establishCheckAttrMatching(attr);
		}

		List<StreamingOutput<OutputTuple>> outputs = context
				.getStreamingOutputs();
		if (outputs.size() == 1) {
			outStream = outputs.get(0);
		}
		if (outputs.size() > 1) {
			throw new Exception("Operator only has one optional output port");
		}

		if (successAttrName != null) {
			if (checkAttrIndex < 0) {
				// TODO do context check the right way.
				throw new Exception(SUCCESS_PARAM + " only valid if "
						+ CHECK_ATTR_PARAM + " exists");
			}
			// TODO also check that success attribute is only used if there's an
			// output port
			StreamSchema outSchema = outStream.getStreamSchema();
			Attribute attr = outSchema.getAttribute(successAttrName);
			if (attr == null) {
				throw new Exception(
						"passed in success attribute, but no attribute found");
			}
			successAttrIndex = attr.getIndex();
		}
		context.registerStateHandler(this);
	}

	/**
	 * Flush the buffer. Called by shutdown, processPunctuation, and drain.
	 */
	abstract protected void flushBuffer() throws IOException;

	/**
	 * Clear the buffer of pending changes. Called by reset.
	 */
	abstract protected void clearBuffer();

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void shutdown() throws Exception {
		flushBuffer();
		super.shutdown();
	}

	@Override
	public void processPunctuation(StreamingInput<Tuple> stream,
			Punctuation mark) throws Exception {
		if (Punctuation.FINAL_MARKER == mark) {
			flushBuffer();
		}
	}

	/**
	 * Populate and submit an output tuple. If the operator is configured with
	 * an output, create an output tuple, populate it from the input tuple and
	 * the success attribute, and return.
	 * 
	 * @param inputTuple
	 *            The input tuple to use.
	 * @param success
	 *            The success attribute
	 * @throws Exception
	 *             If there is a problem with the submission.
	 */
	protected void submitOutputTuple(Tuple inputTuple, boolean success)
			throws Exception {
		if (outStream != null) {
			// Create a new tuple for output port 0
			OutputTuple outTuple = outStream.newTuple();
			// Copy across all matching attributes.
			outTuple.assign(inputTuple);
			if (successAttrIndex >= 0) {
				outTuple.setBoolean(successAttrIndex, success);
			}
			outStream.submit(outTuple);
		}
	}

	@Override
	public void drain() throws Exception {
		Logger.getLogger(this.getClass()).info(
				"Flushing pending HBase mutations");
		flushBuffer();

	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		Logger.getLogger(this.getClass()).info(
				"Clearing pending HBase mutations due to reset");
		clearBuffer();

	}

	@Override
	public void resetToInitialState() throws Exception {
		Logger.getLogger(this.getClass()).info(
				"Clearing pending HBase mutations due to resetToInitialState");
		clearBuffer();

	}

	/**
	 * Nothing to do on a close.
	 */
	@Override
	public void close() throws IOException {

	}

	/**
	 * Nothing to save on a checkpoint.
	 */
	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {

	}

	/**
	 * Nothing to do on retire checkpoint.
	 */
	@Override
	public void retireCheckpoint(long id) throws Exception {

	}
}
