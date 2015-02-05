package com.ibm.streamsx.hbase.sample;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;



import com.ibm.streams.function.model.Function;
public class HelperFunctions {
	public static final String NAMESPACE="com.ibm.streamsx.hbase.sample";
	public static final Logger trace = Logger.getLogger(HelperFunctions.class.getCanonicalName());
	
	// The sample application uses the makeKey function to make a key,
	// but an application could use a combination of the appendLong and appendString
	// functions instead. 
	@Function(namespace=NAMESPACE,description="Append the long, in bytes, to the current blob and return the resulting blob.  The original blob is unchanged.")
	public static byte[] appendLong(byte[] currentBlob,long toAppend ) {
		ByteBuffer buffer = ByteBuffer.allocate(currentBlob.length + Long.SIZE/Byte.SIZE);
		buffer.put(currentBlob);
		buffer.putLong(toAppend);
		return buffer.array();
	}
	@Function(namespace=NAMESPACE,description="Get the type representation of the string toAppend (using the default character set), and append it to the blob, and return the result.  The original blob is unchanged.")
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
	@Function(namespace=NAMESPACE,description="Starting at index, return the long represented by bytes in the blob from index to index+7")
	public static long getLong(byte[] currentBlob,int index) {
		ByteBuffer buffer = ByteBuffer.wrap(currentBlob);
		return buffer.getLong(index);
	}
	@Function(namespace=NAMESPACE,description="Starting at index, return the string represented by the bytes index to index+length-1 in the blob.  Assumes the default character set.")
	public static String getString(byte[] currentBlob, int offset, int length) {
		trace.log(Level.INFO,"Entering getString, currentBlob size {0}, index {1}, length {2}",new Object[] {currentBlob.length,offset,length});
		byte [] stringBytes = new byte[length];
		for (int i = 0; i < length; i++ ) {
			stringBytes[i] = currentBlob[i+offset];
		}
		return new String(stringBytes);
	}
}
