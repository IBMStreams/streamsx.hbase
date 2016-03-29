package com.ibm.streamsx.hbase.sample;
import java.nio.ByteBuffer;

import com.ibm.streams.function.model.Function;

public class KeyHelper {

	/**
	 * Here our key is a concatenation of a timestamp (represented as a long) and a location.
	 *  (NOTE: This is probably not a good key, as consecutive updates will go to the same HBase tablet server, and your application won't be able to get much parallelism.) 
	 *  This function will generate two spl functions:  list<uint8> makeKey(uint64, T)  list<int8> makeKey(int64,T).  
	 *  If you pass in the time as a uint64, the resulting byte array is list<uint8>.
	 *  If you pass in the time as a int64, the resulting byte array is list<int8>.    
	 *  The type list<uint8> can be cast to a blob easily (see the SPL code), but the type  list<int8> can't, so it's probably a good idea to use unsigned types to generate keys.
	 * @param time  the time, as a long.
	 * @param location  the location
	 * @return
	 */
	@Function(name="makeKey", namespace = "com.ibm.streamsx.hbase.sample", description = " A custom key generation function.  ")
	public static byte[] makeKey(long time, String location) {
		// NOTE: you should be cautious about using getBytes without passing in a character set or an
		// encoding.  
		ByteBuffer b = ByteBuffer.allocate(Long.SIZE/Byte.SIZE + location.getBytes().length);
		b.putLong(time);
		b.put(location.getBytes());
		return b.array();
	}
	
}
