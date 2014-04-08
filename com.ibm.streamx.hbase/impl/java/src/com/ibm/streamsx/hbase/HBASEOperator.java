/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
/**
 * Class for shared code between operators.
 * @author hildrum
 *
 */
@Libraries({"@HBASE_HOME@/lib/*","@HADOOP_HOME@/hadoop-core.jar","@HADOOP_HOME@/lib/*","@HBASE_HOME@/hbase.jar","@HBASE_HOME@/conf"})
public abstract class HBASEOperator extends AbstractOperator {
	protected List<String> staticColumnFamilyList= null;
	protected List<String> staticColumnQualifierList = null;
	
	private String tableName;
	protected HTable myTable;
	static final String TABLE_PARAM_NAME = "tableName";
	static final String ROW_PARAM_NAME = "rowAttrName";
	static final String STATIC_COLF_NAME = "staticColumnFamily";
	static final String STATIC_COLQ_NAME = "staticColumnQualifier";
	
	@Parameter(name=TABLE_PARAM_NAME,optional=false,description="Name of the HBASE table")
	public void setTableName(String _name) {
		tableName = _name;
	}

	@Parameter(name=STATIC_COLF_NAME, optional = true,description="Column family to be used for all tuples.  It may be a list in some cases.")
	public void setStaticColumnFamily(List<String> name) {
		staticColumnFamilyList = name;
	}
	
	@Parameter(name=STATIC_COLQ_NAME, optional = true,description="Column qualifier to be used for all tuples.  It may be a list for some operators.") 
	public void setStaticColumnQualifier(List<String> name) {
		staticColumnQualifierList = name;
	}
	
	/**
	 * Helper function to check that an attribute is the right type and return the index if so.
	 * @param schema Input schema
	 * @param attrName Attribute name
	 * @param throwException  If true, throw an exception when attribute isn't found., if false, return -1.
	 * @return
	 * @throws Exception
	 */
	protected int checkAndGetIndex(StreamSchema schema, String attrName, boolean throwException) throws Exception {
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			if (throwException)
				throw new Exception("Expected attribute "+attrName+" to be present, but not found");
			else 
				return -1;
		}
		if (attr.getType().getMetaType() != MetaType.RSTRING) {
			throw new Exception("Expected attribute "+attrName+" to have type RSTRING, found "+attr.getType().getMetaType());
		}
		return attr.getIndex();
	}
	
	protected int checkAndGetIndex(StreamSchema schema, String attrName) throws Exception{
		return checkAndGetIndex(schema,attrName,true);
	}
	
	
	/**
	 * Loads the configuration, and creates an HTable instance.  If the table doesn not exist, or cannot be
	 * accessed, it will throw an error.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
       
    	Configuration conf = new Configuration();
    	conf.addResource("hbase-site.xml");
    	myTable = new HTable(conf,tableName);
    	if (null == myTable) {
    		Logger.getLogger(this.getClass()).error("Cannot access table, failing.");
    		throw new Exception("Cannot access table.  Check configuration");
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
		if (Punctuation.FINAL_MARKER == mark) {
			myTable.close();
		}
		super.processPunctuation(stream, mark);
	}
	
	
}
