/* Copyright (C) 2013-2018, International Business Machines Corporation  */
/* All Rights Reserved                                                   */

package com.ibm.streamsx.hbase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.types.Blob;

/**
 * Class for shared code between operators.
 * 
 * @author hildrum
 */

//@Libraries({"@HBASE_HOME@/lib/*", "@HBASE_HOME@/*"})
@Libraries({"opt/downloaded/*"})
public abstract class HBASEOperator extends AbstractOperator {
	public static final String DOC_BLANKLINE = "\\n\\n";
	static final String HBASE_SITE_PARAM_NAME = "hbaseSite";
	public static final String consistentCutIntroducer = "\\n\\n**Behavior in a consistent region**\\n\\n";
	public static final String commonDesc = "";
	// Keep the old common description around for a little while, in case we decide to add it back into the documentation.
	public static final String commonDescOld = "\\n\\n**Configuring the operator**\\n\\n"
			+ "In order to run, the operator the  HBase configuration information.  It reads this information from hbase-site.xml file."
			+ "You can either directly point the operator" + "to hbase-site.xml via the " + HBASE_SITE_PARAM_NAME
			+ " parameter, or it can look for the hbase-site.xml parameter " + "relative to the HBASE_HOME environment variable" + DOC_BLANKLINE
			+ "If your operator will run on a host that has HBase installed, then you could set "
			+ "HBASE_HOME in the operator's runtime environment, in which case the operator looks under HBASE_HOME/conf for hbase-site.xml"
			+ DOC_BLANKLINE + "If your HBase in located on a different host than streams, then use the " + HBASE_SITE_PARAM_NAME + " parameter."
			+ "To do this, copy hbase-site.xml into your application (eg, into the etc directory) and" + "point the " + HBASE_SITE_PARAM_NAME
			+ " parameter to this location, eg " + HBASE_SITE_PARAM_NAME + ": \\\"etc/hbase-site.xml\\\"";

	protected List<String> staticColumnFamilyList = null;
	protected List<String> staticColumnQualifierList = null;
	public final static Charset RSTRING_CHAR_SET = Charset.forName("UTF-8");
	protected Charset charset = RSTRING_CHAR_SET;
	public String tableName = null;
	protected byte tableNameBytes[] = null;
	private static String hbaseSite = null;
	private String fAuthPrincipal = null;
	private String fAuthKeytab = null;
	private String fFailureAction = null;
	protected Connection connection = null;
	public int successAttrIndex = -1;
	public StreamingOutput<OutputTuple> outStream = null;
	public StreamingOutput<OutputTuple> errorOutputPort = null;

	static final String TABLE_PARAM_NAME = "tableName";
	public TupleAttribute<Tuple, String> tableNameAttribute; 
	static final String TABLE_NAME_ATTRIBUTE = "tableNameAttribute";
	static final String ROW_PARAM_NAME = "rowAttrName";
	static final String STATIC_COLF_NAME = "staticColumnFamily";
	static final String STATIC_COLQ_NAME = "staticColumnQualifier";
	static final String AUTH_PRINCIPAL = "authPrincipal";
	static final String AUTH_KEYTAB = "authKeytab";
	static final String JAR_LIBS_PATH = "/opt/downloaded/*";
	
	static final String CHARSET_PARAM_NAME = "charset";
	static final String VALID_TYPE_STRING = "rstring, ustring, blob, or int64";
	static final int BYTES_IN_LONG = Long.SIZE / Byte.SIZE;

	
	/**
	 *  signifies if the operator has error port defined or not
	 * assuming in the beginning that the operator does not have an error output
	 * port by setting hasErrorPort to false. further down in the code, if the
	 * number of output ports is 2, we set to true We send data to error output
	 * port only in case where hasErrorPort is set to true which implies that
	 * the operator instance has a error output port defined.
	 */
	public boolean hasErrorPort = false;

	org.apache.log4j.Logger logger = Logger.getLogger(this.getClass());
	
	@Parameter(name = HBASE_SITE_PARAM_NAME, optional = true, description = "The **hbaseSite** parameter specifies the path of hbase-site.xml file.  This is the recommended way to specify the HBASE configuration.  If not specified, then `HBASE_HOME` must be set when the operator runs, and it will use `$HBASE_SITE/conf/hbase-site.xml`")
	public void setHbaseSite(String name) {
		hbaseSite = name;
	}

	@Parameter(name = CHARSET_PARAM_NAME, optional = true, description = "Character set to be used for converting byte[] to Strings and Strings to byte[].  Defaults to UTF-8")
	public void getCharset(String _name) {
		charset = Charset.forName(_name);
	}

	@Parameter(name = TABLE_PARAM_NAME, optional = true, description = "Name of the HBASE table. It is an optional parameter but one of these parameters must be set in opeartor: 'tableName' or 'tableNameAttribute'. Cannot be used with 'tableNameAttribute'. If the table does not exist, the operator will throw an exception")
	public void setTableName(String _name) {
		tableName = _name;
	}
	
	@Parameter(name = TABLE_NAME_ATTRIBUTE, optional = true, description = "Name of the attribute on the input tuple containing the tableName. Use this parameter to pass the table name to the operator via input port. Cannot be used with parameter 'tableName'. This is suitable for tables with the same schema.")
	public void setTableNameAttr(TupleAttribute<Tuple, String> tableNameAttribute) throws IOException {
		this.tableNameAttribute = tableNameAttribute;
	} 


	@Parameter(name = STATIC_COLF_NAME, optional = true, description = "If this parameter is specified, it will be used as the columnFamily for all operations.  (Compare to columnFamilyAttrName.) For HBASEScan, it can have cardinality greater than one.")
	public void setStaticColumnFamily(List<String> name) {
		staticColumnFamilyList = name;
	}

	@Parameter(name = STATIC_COLQ_NAME, optional = true, description = "If this parameter is specified, it will be used as the columnQualifier for all tuples.  HBASEScan allows it to be specified multiple times.")
	public void setStaticColumnQualifier(List<String> name) {
		staticColumnQualifierList = name;
	}

	@Parameter(name = AUTH_PRINCIPAL, optional = true, description = "The **authPrincipal** parameter specifies the Kerberos principal, which is typically the principal that is created for HBase server")
	public void setAuthPrincipal(String authPrincipal) {
		this.fAuthPrincipal = authPrincipal;
	}

	public String getAuthPrincipal() {
		return fAuthPrincipal;
	}

	@Parameter(name = AUTH_KEYTAB, optional = true, description = "The **authKeytab** parameter specifies the kerberos keytab file that is created for the principal.")
	public void setAuthKeytab(String authKeytab) {
		this.fAuthKeytab = authKeytab;
	}

	public String getAuthKeytab() {
		return fAuthKeytab;
	}

	public String getfailureAction() {
		return fFailureAction;
	}
	
	protected static String getNoCCString() {
		return Messages.getString("HBASE_OP_NO_CONSISTENT_REGION", "HBASEOperator");
	}

	protected static void checkConsistentRegionSource(OperatorContextChecker checker, String operatorName) {
		// Now we check whether we're in a consistent region.
		ConsistentRegionContext ccContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		if (ccContext != null && ccContext.isStartOfRegion()) {
			checker.setInvalidContext(Messages.getString("HBASE_OP_NO_CONSISTENT_REGION", "HBASEOperator"), null);
		}
	}

	/**
	 * Function for runtime context checks.
	 * 
	 * @param checker
	 */
	@ContextCheck(compile = true)
	public static void runtimeHBaseOperatorChecks(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// The hbase site must either be specified by a parameter, or we must look it up relative to an environment variable.
		if (!context.getParameterNames().contains(HBASE_SITE_PARAM_NAME)) {
			String hbaseHome = System.getenv("HBASE_HOME");
			if ((hbaseSite == null) && (hbaseHome == null)){
				checker.setInvalidContext(Messages.getString("HBASE_OP_NO_HBASE_HOME", HBASE_SITE_PARAM_NAME), null);
			}
		}
		
		if ((!context.getParameterNames().contains(TABLE_PARAM_NAME))
			&& (!context.getParameterNames().contains(TABLE_NAME_ATTRIBUTE))) {
				checker.setInvalidContext("One of these parameters must be set in opeartor: '" + TABLE_PARAM_NAME + "' or '" + TABLE_NAME_ATTRIBUTE +"'", null);
		}				
	
		if (context.getNumberOfStreamingOutputs() == 2) {
			StreamingOutput<OutputTuple> errorOutputPort = context.getStreamingOutputs().get(1);
			// The optional error output port can have only one rstring attribute.
			if (errorOutputPort.getStreamSchema().getAttribute(0).getType().getMetaType() != Type.MetaType.RSTRING) {
				checker.setInvalidContext("The first attribute in the optional error output port must be a rstring", null);
			}
		}	
	
	}

	@ContextCheck(compile = true)
	public static void checkTableName(OperatorContextChecker checker) {
		// Cannot specify both tableNameAttribute and a tableName
		checker.checkExcludedParameters(TABLE_NAME_ATTRIBUTE, TABLE_PARAM_NAME);
		checker.checkExcludedParameters(TABLE_PARAM_NAME, TABLE_NAME_ATTRIBUTE);
	}

	
	
	/**
	 * Helper function to check that an attribute is the right type and return the index if so.
	 * 
	 * @param schema
	 *            Input schema
	 * @param attrName
	 *            Attribute name
	 * @param throwException
	 *            If true, throw an exception when attribute isn't found., if false, return -1.
	 * @return
	 * @throws Exception
	 */
	protected int checkAndGetIndex(StreamSchema schema, String attrName, boolean throwException) throws Exception {
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			if (throwException)
				throw new Exception("Expected attribute " + attrName + " to be present, but not found");
			else
				return -1;
		}
		if (!isValidInputType(attr.getType().getMetaType())) {
			throw new Exception("Expected attribute " + attrName + " to be one of " + VALID_TYPE_STRING + ", found " + attr.getType().getMetaType());
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

	protected static void isValidInputType(OperatorContextChecker checker, MetaType mType, String attrName) {
		if (isValidInputType(mType)) {
			return;
		} else {
			checker.setInvalidContext(Messages.getString("HBASE_OP_INVALID_ATTR", attrName, mType), null);
		}
	}

	/**
	 * Subclasses should generally use this function to get a byte[] to send to HBASE from a tuple.
	 * 
	 * @param tuple
	 *            The tuple containing the attribute
	 * @param attrIndex
	 *            the index of the attribute for which we are getting bytes
	 * @param mType
	 *            The attribute's meta type.
	 * @return byte[] represented the attribute
	 * @throws Exception
	 *             Throws an exception if the metaType is not one of the allowed types.
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
			Blob myBlob = tuple.getBlob(attrIndex);
			byte toReturn[] = new byte[(int) myBlob.getLength()];
			myBlob.getInputStream().read(toReturn, 0, (int) myBlob.getLength());
			return toReturn;
		default:
			throw new Exception("Cannot get bytes for objects of type " + mType);
		}
	}

	protected int checkAndGetIndex(StreamSchema schema, String attrName) throws Exception {
		return checkAndGetIndex(schema, attrName, true);
	}

	/**
	 * Helper function to check that an attribute is the right type and return the index if so.
	 * We may have to eventually allow a list of types...
	 * 
	 * @param schema
	 *            Input schema
	 * @param attrName
	 *            Attribute name
	 * @param throwException
	 *            If true, throw an exception when attribute isn't found., if false, return -1.
	 * @return
	 * @throws Exception
	 */
	protected int checkAndGetIndex(StreamSchema schema, String attrName, MetaType allowedType, boolean throwException) throws Exception {
		Attribute attr = schema.getAttribute(attrName);
		if (attr == null) {
			if (throwException)
				throw new Exception("Expected attribute " + attrName + " to be present, but not found");
			else
				return -1;
		}
		if (attr.getType().getMetaType() != allowedType) {
			throw new Exception("Expected attribute " + attrName + " to have type " + allowedType + ", found " + attr.getType().getMetaType());
		}
		return attr.getIndex();
	}

	
	
	/**
	 * Loads the configuration, and creates an HTable instance. If the table doesn't not exist, or cannot be
	 * accessed, it will throw an error.
	 */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
	    logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

	    String hadoopHome = System.getenv("HADOOP_HOME");
		String hbaseHome = System.getenv("HBASE_HOME");
		String HbaseSite = hbaseSite;
		if (hbaseSite == null) {
			hbaseSite = hbaseHome + File.separator + "conf" + File.separator + "hbase-site.xml";
			HbaseSite = hbaseSite;
		} else {
			// We need to pass the absolute paths hbase-site.xml configuration file to the conf.
			if (hbaseSite.charAt(0) != '/') {
				hbaseSite = context.getPE().getApplicationDirectory().getAbsolutePath() + File.separator + hbaseSite;
			}
		}

		//	System.out.println("hbaseSite " + hbaseSite);
		// check if the file hbase-site.xml exist.
		if (hbaseSite != null) {
			File f = new File(hbaseSite);	
			if(!f.exists()){
				logger.error("\nERROR: The hbase configuration file  '" + HbaseSite + "'  doesn't exist.", null);
				logger.error(Messages.getString("HBASE_OP_NO_HBASE_HOME", HBASE_SITE_PARAM_NAME ), null);
				return;
			}
		}	 
		
	    ArrayList<String>libList = new ArrayList<>();
		String default_dir = context.getToolkitDirectory() + JAR_LIBS_PATH;
		libList.add(default_dir);
		if (hbaseHome != null) {
			libList.add(hbaseHome + "/lib/*");
			libList.add(hbaseHome + "/*");
			libList.add(hadoopHome + "/lib/*");
			libList.add(hadoopHome + "/client/*");
			libList.add(hadoopHome + "/*");
		}
	
		try {
			context.addClassLibraries(libList.toArray(new String[0]));
		} catch (Exception e) {
			logger.error(Messages.getString("HBASE_OP_NO_CLASSPATH"));
		}
	
				
		if ((fAuthKeytab != null) && (fAuthKeytab.charAt(0) != '/')) {
			// We need to pass the absolute paths keytab file to the conf.
			fAuthKeytab = context.getPE().getApplicationDirectory().getAbsolutePath() + File.separator + fAuthKeytab;
		}
		
		// check if the operator has an error output port
		for (int i=0; i < context.getNumberOfStreamingOutputs(); i++ ){			
			if ( i == 0) outStream = getOutput(i);
			if ( i == 1) errorOutputPort = getOutput(i);
		}

		
		if (tableName != null){
			tableNameBytes = tableName.getBytes(charset);
		}
				
		getConnection();
	}

	protected void getConnection() throws IOException {
		logger.info("hbaseSite:\t" + hbaseSite);
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(hbaseSite));
		if ((fAuthPrincipal != null) && (fAuthKeytab != null)) {
			// kerberos authentication
			logger.info("fAuthKeytab:\t" + fAuthKeytab);
			logger.info("fAuthPrincipal:\t" + fAuthPrincipal);
			conf.set("hadoop.security.authentication", "kerberos");
			conf.set("hbase.security.authentication", "kerberos");
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(fAuthPrincipal, fAuthKeytab);
		}

		connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf));
		Admin admin = connection.getAdmin();
		if (admin.getConnection() == null) {
			logger.error("HBase connection failed");
		}

		/*
		 * // only for test to get the list of all hbase tables.
		 * System.out.println("connection:\t" + connection.toString());
		 * System.out.println("admin:\t" + admin.toString());
		 * TableName[] names = admin.listTableNames();
		 * for (TableName name : names) {
		 * System.out.println("table name:\t" + name.getNameAsString());
		 * }
		 */
	}

	/**
	 * getHTable get an Admin from connection ant returns a table if table exists. 
	 * In case of any error it logs the error and submits error message if the operator has an error output port.
	 * 
	 * @return Table.
	 * @throws TableNotFoundException
	 *
	 */	
	protected Table getHTable() throws TableNotFoundException, IOException {
		if (tableName == null){
			return null;
		}
		
		final TableName tableTableName = TableName.valueOf(tableNameBytes);
		try (Admin admin = this.connection.getAdmin()) {
			if (!admin.tableExists(tableTableName)) {
				String errorMessage = "Table '" + tableTableName.getNameAsString()
				          + "' does not exists.";					
					try {
			    		submitErrorMessagee(errorMessage, null);
					} catch (Exception e) {
						logger.error(e.getMessage());
					}
		    		throw new TableNotFoundException(errorMessage);
		    	}
			}
		return connection.getTable(tableTableName);
	}

	
	/**
	 * getHTable get an Admin from connection ant returns a table if table exists. 
	 * In case of any error it logs the error and submits error message if the operator has an error output port.
	 * @param Tuple 
	 * @return Table.
	 * @throws TableNotFoundException
	 *
	 */	
	protected Table getHTable(Tuple tuple) throws TableNotFoundException, IOException {
		String TableNameStr = null;
		if (tableName != null) {
			TableNameStr = tableName;
		} else {
			TableNameStr = tuple.getString(tableNameAttribute.getAttribute().getIndex()); 
		}

		if (TableNameStr == null) return null;

		if (TableNameStr.length() < 1 ) return null;
		
		byte TableNameBytes[] = TableNameStr.getBytes(charset);
		final TableName tableTableName = TableName.valueOf(TableNameBytes);
		
		try (Admin admin = this.connection.getAdmin()) {
			if (!admin.tableExists(tableTableName)) {
				String errorMessage = "Table '" + tableTableName.getNameAsString()
		          + "' does not exists.";					
				try {
		    		submitErrorMessagee(errorMessage, tuple);
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				throw new TableNotFoundException(errorMessage);
			}	
		}		
		return connection.getTable(tableTableName);
	}

	
	protected Table getHTable(String sTableName) throws IOException {
		byte TableNameBytes[] = sTableName.getBytes(charset);
		final TableName tableName = TableName.valueOf(TableNameBytes);
		return connection.getTable(tableName);
	}

	protected TableName getTableName() throws IOException {
		final TableName Tablename = TableName.valueOf(tableNameBytes);
		return Tablename;
	}

	protected TableName getTableName(String sTableName) throws IOException {
		byte TableNameBytes[] = sTableName.getBytes(charset);
		final TableName Tablename = TableName.valueOf(TableNameBytes);
		return Tablename;
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
		logger.trace(
				"Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	/**
	 * Used in HBASEGet and HBASEScan to figure out which fields need to be looked for in the results.
	 * 
	 * @param schema
	 *            the scheme that will be populated from the hbase query
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
	 * Create and submit an output tuple. If the operator is configured with
	 * an error output, create an output tuple, and submit the error message.
	 * Add the current date time and the name of operator that causes error to the error message.
	 * 
	 * @param errorMessage
	 *            The input error message.
	 * @throws Exception
	 *             If there is a problem with the submission.
	 */
	protected void submitErrorMessagee(String errorMessage, Tuple inputTuple)
			throws Exception {
		if (errorOutputPort != null){
			// add current date and time and operator name to error message
			String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
			errorMessage = timeStamp + " , " + errorOutputPort.getName() + " , " + errorMessage;
			// add input tuple to error message
			if (inputTuple != null)
				errorMessage = errorMessage + " , " + inputTuple.toString();
			OutputTuple errorTuple = errorOutputPort.newTuple();
			errorTuple.setString(0, errorMessage);			
			errorOutputPort.submit(errorTuple);
		}	
	}
		
}
