package com.ibm.streamsx.hbase;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;



import com.ibm.streams.function.model.Function;

public class HelperFunctions {
	public static final String NAMESPACE="com.ibm.streamsx.hbase";
	public static final Logger trace = Logger.getLogger(HelperFunctions.class.getCanonicalName());
	//= LogManager.getLogger(HelperFunctions.class.getCanonicalName());
	
	@Function(namespace=NAMESPACE)
	public static byte[] appendLong(byte[] currentBlob,long toAppend ) {
		ByteBuffer buffer = ByteBuffer.allocate(currentBlob.length + Long.SIZE/Byte.SIZE);
		buffer.put(currentBlob);
		buffer.putLong(toAppend);
		return buffer.array();
	}
	@Function
	public static byte[] appendString(byte[] currentBlob, String toAppend) {
		byte[] stringBytes = toAppend.getBytes();
		byte[] toReturn = new byte[stringBytes.length+ currentBlob.length];
		for (int i = 0; i < currentBlob.length; i++) {
			toReturn[i] = currentBlob[i];
		}
		for (int i = 0; i < stringBytes.length; i++) {
			toReturn[i+currentBlob.length] = stringBytes[i];
		}
		return toReturn;
	}
	@Function
	public static long getLong(byte[] currentBlob,int index) {
		ByteBuffer buffer = ByteBuffer.wrap(currentBlob);
		return buffer.getLong(index);
	}
	@Function
	public static String getString(byte[] currentBlob, int offset, int length) {
		trace.log(Level.INFO,"Entering getString, currentBlob size {0}, index {1}, length {2}",new Object[] {currentBlob.length,offset,length});
		byte [] stringBytes = new byte[length];
		for (int i = 0; i < length; i++ ) {
			stringBytes[i] = currentBlob[i+offset];
		}
		return new String(stringBytes);
	}
}
