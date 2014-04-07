/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.InputPortSet;
import java.util.ArrayList;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Class for common functions of Put and Delete HBASEPut and HBASEDelete have
 * some common functions not shared by get and increment. Support for these is
 * placed here.
 * 
 * This class handles: - batchSize parameter - success attribute - check
 * attribute - outputing a tuple and indicating success or failure
 * 
 */

public abstract class HBASEPutDelete extends HBASEOperatorWithInput {

	// These are used by Put and Delete for checkAndPut and checkAndDelete
	private int checkColFIndex = -1;
	private int checkColQIndex = -1;
	private int checkValueIndex = -1;

	final protected Object listLock = new Object();
	protected int batchSize = 0;

	final String BATCHSIZE_NAME = "batchSize";

	protected String checkAttr = null;
	final String CHECK_ATTR_PARAM = "checkAttrName";
	protected int checkAttrIndex = -1;
	final String SUCCESS_PARAM = "successAttr";
	private String successAttrName = null;
	private int successAttrIndex = -1;
	StreamingOutput<OutputTuple> outStream = null;

	
	@Parameter(name = SUCCESS_PARAM, optional = true)
	public void setSuccessAttr(String name) {
		successAttrName = name;
	}

	@Parameter(name = BATCHSIZE_NAME, optional = true)
	public void setBatchSize(int _size) {
		batchSize = _size;
	}

	@Parameter(name = CHECK_ATTR_PARAM, optional = true)
	public void setCheckAttr(String name) {
		checkAttr = name;
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
		checkColFIndex = checkAndGetIndex(checkSchema, "columnFamily");
		if (checkSchema.getAttribute("value") != null) {
			checkValueIndex = checkAndGetIndex(checkSchema, "value");
		}
	}

	byte[] getCheckColF(Tuple tuple) {
		return tuple.getString(checkColFIndex).getBytes();
	}

	byte[] getCheckColQ(Tuple tuple) {
		return tuple.getString(checkColQIndex).getBytes();
	}

	byte[] getCheckValue(Tuple tuple) {
		if (checkValueIndex > 0) {
			return tuple.getString(checkValueIndex).getBytes();
		} else
			return null;
	}

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
		// TODO: If window punctuations are meaningful to the external system or
		// data store,
		// insert code here to process the incoming punctuation.
	}

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void shutdown() throws Exception {
		super.shutdown();
	}

}
