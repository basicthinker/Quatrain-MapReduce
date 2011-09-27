package org.apache.hadoop.mapred.buffer.net;

public interface BufferExchange {
	public static enum Connect{OPEN, CONNECTIONS_FULL, BUFFER_COMPLETE, ERROR};
	
	public static enum Transfer{READY, IGNORE, SUCCESS, RETRY, TERMINATE};
	
	public static enum BufferType{FILE, SNAPSHOT, STREAM};
}
