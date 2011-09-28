/**
 * 
 */
package org.apache.hadoop.mapred.buffer;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;

/**
 * @author Jinglei Ren
 *
 */
public class StreamWritable extends OutputFileWritable {

	protected StreamWritable(OutputFile file, FileSystem rfs, JobConf conf, BufferRequest request) {
		super(file, rfs, conf, request);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.buffer.OutputFileWritable#write(java.nio.channels.SocketChannel)
	 */
	@Override
	public long write(SocketChannel channel) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.buffer.OutputFileWritable#read(java.nio.channels.SocketChannel)
	 */
	@Override
	public long read(SocketChannel channel) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.buffer.OutputFileWritable#setValue(java.lang.Object)
	 */
	@Override
	public void setValue(Object value) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.buffer.OutputFileWritable#getValue()
	 */
	@Override
	public Object getValue() {
		// TODO Auto-generated method stub
		return null;
	}

}
