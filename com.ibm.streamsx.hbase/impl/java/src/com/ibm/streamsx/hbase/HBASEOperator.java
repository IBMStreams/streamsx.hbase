/* Copyright (C) 2013-2014, International Business Machines Corporation  */
/* All Rights Reserved                                                 */

package com.ibm.streamsx.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.net.URI;

/**
 * Class for shared code between operators.
 * @author hildrum
 *
 */
@Libraries({"opt/downloaded/*","@HBASE_HOME@/conf"})
public abstract class HBASEOperator extends AbstractOperator {
	protected List<String> staticColumnFamilyList= null;
	protected List<String> staticColumnQualifierList = null;
	public final static Charset RSTRING_CHAR_SET = Charset.forName("UTF-8");
	protected Charset charset = RSTRING_CHAR_SET;
	private String tableName = null;
	protected byte tableNameBytes[] = null;
        private String hbaseSite =null;
	protected HConnection connection =null;
	private Configuration conf;
        static final String HBASE_SITE_PARAM_NAME="hbaseSite";
	static final String TABLE_PARAM_NAME = "tableName";
	static final String ROW_PARAM_NAME = "rowAttrName";
	static final String STATIC_COLF_NAME = "staticColumnFamily";
	static final String STATIC_COLQ_NAME = "staticColumnQualifier";
	static final String CHARSET_PARAM_NAME = "charset";
	static final String VALID_TYPE_STRING="rstring, ustring, blob, or int64";
	static final int BYTES_IN_LONG = Long.SIZE/Byte.SIZE;
	
    @Parameter(name=HBASE_SITE_PARAM_NAME, optional=true,description="The hbase-site.xml file.  This is an optional parameter; if not set, the operator will look in opt/downloaded and HBASE_HOME/conf for hbase-site.xml.  It may be absolute or relative; if relative, it's relative to the application directory.")
	public void setHbaseSite(String name) {
	hbaseSite = name;
    }
	
	@Parameter(name=CHARSET_PARAM_NAME, optional=true,description="Character set to be used for converting byte[] to Strings and Strings to byte[].  Defaults to UTF-8")
	public void getCharset(String _name) {
		charset = Charset.forName(_name);
	}
	
	@Parameter(name=TABLE_PARAM_NAME,optional=false,description="Name of the HBASE table.  If it does not exist, the operator will throw an exception on startup")
	public void setTableName(String _name) {
		tableName = _name;
	}

	@Parameter(name=STATIC_COLF_NAME, optional = true,description="If this parameter is specified, it will be used as the columnFamily for all operations.  (Compare to columnFamilyAttrName.) For HBASEScan, it can have cardinality greater than one.")
	public void setStaticColumnFamily(List<String> name) {
		staticColumnFamilyList = name;
	}
	
	@Parameter(name=STATIC_COLQ_NAME, optional = true,description="If this parameter is specified, it will be used as the columnQualifier for all tuples.  HBASEScan allows it to be specified multiple times.") 
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
		if (!isValidInputType(attr.getType().getMetaType())) {
			throw new Exception("Expected attribute "+attrName+" to be one of "+VALID_TYPE_STRING+", found "+attr.getType().getMetaType());
		}
		return attr.getIndex();
	}
	
	protected static boolean isValidInputType(MetaType mType) {
		switch (mType) {
		case USTRING:
		case RSTRING:
		case INT64:
		case BLOB:
			return true;
			default:
				return false;
		}
	}
	
	protected static void isValidInputType(OperatorContextChecker checker, MetaType mType,String attrName) {
		if (isValidInputType(mType)) {
			return;
		}
		else {
				checker.setInvalidContext("Attribute "+attrName+" has invalid type "+mType, null);
		}
	}
	
	/**
	 * Subclasses should generally use this function to get a byte[] to send to HBASE from a tuple.
	 * @param tuple  The tuple containing the attribute
	 * @param attrIndex  the index of the attribute for which we are getting bytes
	 * @param mType  The attribute's meta type.
	 * @return byte[] represented the attribute
	 * @throws Exception  Throws an exception if the metaType is not one of the allowed types.
	 */
	protected byte[] getBytes(Tuple tuple, int attrIndex, MetaType mType) throws Exception {
		switch (mType) {
		case USTRING:
			return tuple.getString(attrIndex).getBytes(charset);
		case RSTRING:
			return tuple.getString(attrIndex).getBytes(RSTRING_CHAR_SET);
		case INT64:
			return ByteBuffer.allocate(BYTES_IN_LONG).putLong(tuple.getLong(attrIndex)).array();
		case BLOB:
			Blob myBlob= tuple.getBlob(attrIndex);
			byte toReturn[]=new byte[(int)myBlob.getLength()];
			myBlob.getInputStream().read(toReturn,0,(int)myBlob.getLength());
			return toReturn;
		default:
		throw new Exception("Cannot get bytes for objects of type "+mType);
		}	
	}
	
	protected int checkAndGetIndex(StreamSchema schema, String attrName) throws Exception{
		return checkAndGetIndex(schema,attrName,true);
	}
	
	/**
	 * Helper function to check that an attribute is the right type and return the index if so.
	 * We may have to eventually allow a list of types...
	 * @param schema Input schema
	 * @param attrName Attribute name
	 * @param throwException  If true, throw an exception when attribute isn't found., if false, return -1.
	 * @return
	 * @throws Exception
	 */
	protected int checkAndGetIndex(StreamSchema schema, String attrName, MetaType allowedType, boolean throwException) throws Exception {
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			if (throwException)
				throw new Exception("Expected attribute "+attrName+" to be present, but not found");
			else 
				return -1;
		}
		if (attr.getType().getMetaType() != allowedType) {
			throw new Exception("Expected attribute "+attrName+" to have type "+allowedType+", found "+attr.getType().getMetaType());
		}
		return attr.getIndex();
	}
	
	/**
	 * Loads the configuration, and creates an HTable instance.  If the table doesn't not exist, or cannot be
	 * accessed, it will throw an error.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
       
    	conf = new Configuration();
	if (hbaseSite == null) {
	    conf.addResource("hbase-site.xml");
	}
	else {
        // We need to pass the conf a Path.  Seems the safest way to do that is to create a path from a URI.
        // We want to handle both relative and absolute paths, adn I don't want to futz around prepending
        // file:/// to a string.
        // First get a URI for the application directory
	    URI toolkitRoot = context.getPE().getApplicationDirectory().toURI();
        // now, resolve the hbase site against that
        URI hbaseSiteURI = toolkitRoot.resolve(hbaseSite);
        // make a path out of it.
	    Path hbaseSitePath = new Path(hbaseSiteURI);
        // add the resource.  finally.
	    conf.addResource(hbaseSitePath);
	}
	connection = HConnectionManager.createConnection(conf);
	tableNameBytes = tableName.getBytes(charset);
	// Just check to see if the table exists.  Might as well fail on initialize instead of process.
	HTableInterface tempTable = connection.getTable(tableNameBytes);
    	if (null == tempTable) {
    		Logger.getLogger(this.getClass()).error("Cannot access table, failing.");
    		throw new Exception("Cannot access table.  Check configuration");
    	}
	tempTable.close();
	}
	
	/**
	 * Subclasses should not generally use this.  The should instead create HTableInterface via 
	 * connection.getTable(tableNameBytes).
	 * 
	 * However, HTableInterface doesn't have getStartEndKeys(), so that's why we need
	 * an actual HTable.
	 * 
	 * @return HTable object.
	 * @throws IOException
	 */
	protected HTable getHTable() throws  IOException {
		return new HTable(conf,tableNameBytes);
	}
	
	 /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
   @Override
   public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
       Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
       if (connection != null && !connection.isClosed()) {
    	   connection.close();
       }
    }
	
	/**
	 * Used in HBASEGet and HBASEScan to figure out which fields need to be looked for in the results.
	 * @param schema the scheme that will be populated from the hbase query
	 * @return An array of the byteArrays representing columnQualifiers.
	 */
	protected byte[][] getAttributeNamesAsBytes(StreamSchema schema) {
		int numAttr = schema.getAttributeCount();
		byte toReturn[][] = new byte[numAttr][];
		for (int i = 0; i < numAttr; i++) {
			toReturn[i] = schema.getAttribute(i).getName().getBytes(charset);
		}
		return toReturn;
	}
	
}
