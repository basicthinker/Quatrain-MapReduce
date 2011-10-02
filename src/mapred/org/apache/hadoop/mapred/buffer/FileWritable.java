/**
 * 
 */
package org.apache.hadoop.mapred.buffer;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.BufferExchange.BufferType;
import org.apache.hadoop.mapred.buffer.net.BufferExchange.Transfer;

/**
 * @author Jinglei Ren
 * 
 */
public class FileWritable extends OutputFileWritable {

	private static final Log LOG = LogFactory.getLog(FileWritable.class
			.getName());

	private int next;

	/**
	 * Construct instance intended for write operation
	 * */
	public FileWritable(OutputFile file, FileSystem rfs, JobConf conf) {
		super(file, rfs, conf);
	}

	/**
	 * Construct instance intended for read operation
	 * */
	public FileWritable(OutputFile file, JobConf conf,
			InputCollector<?, ?> collector, Task task) {
		super(file, conf, collector, task);
	}

	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource.FileSource#transfer(OutputFile
	 *      file)
	 * */
	@Override
	public long write(OutputStream Ostream) throws IOException {


		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		TaskID taskid = header.owner().getTaskID();
		DataOutputStream ostream = new DataOutputStream(Ostream);
		LOG.debug("Transfer file " + file + ". Destination " + destination);
		Transfer response = transmit(ostream);

		if (response == Transfer.SUCCESS) {
			return OutputFileWritable.SUCCESS;
		} else {
			return OutputFileWritable.ERROR;
		}
	}

	@Override
	public long read(InputStream instream) throws IOException {

		DataInputStream istream = new DataInputStream(instream);
		file.setHeader(OutputFile.FileHeader.readHeader(istream));
		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		if (header.eof()) {
			System.out.println("Read in EOF");
		}
		
		if (collector.read(istream, header)) {
			this.setValue(header);
		}
		return 0;
	}

	@Override
	public void setValue(Object value) {
		// TODO Auto-generated method stub
		super.setValue(value);
	}

	@Override
	public Object getValue() {
		// TODO Auto-generated method stub
		return super.getValue();
	}

}
