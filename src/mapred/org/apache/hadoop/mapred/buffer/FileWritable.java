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

	private Map<TaskID, Integer> cursor;
	private int next;

	/**
	 * Construct instance intended for write operation
	 * */
	public FileWritable(OutputFile file, FileSystem rfs, JobConf conf) {
		super(file, rfs, conf);
		this.cursor = new HashMap<TaskID, Integer>();
		// this.next = nextPosition;
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

		System.out.print("@zhumeiqi_debug:Call write partition"
				+ this.partition);
		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		TaskID taskid = header.owner().getTaskID();
		DataOutputStream ostream = new DataOutputStream(Ostream);
		LOG.debug("Transfer file " + file + ". Destination " + destination);
		Transfer response = transmit(ostream);
		if (response == Transfer.TERMINATE) {
			return OutputFileWritable.TERMINATE;
		}
		int position = header.ids().last() + 1;

		if (response == Transfer.SUCCESS) {
			if (header.eof()) {
				LOG.debug("Transfer end of file for source task " + taskid);
				ostream.writeInt(0); // [NOTICE] Taken from method close().
			}
			cursor.put(taskid, position);
			LOG.debug("Transfer complete. New position " + cursor.get(taskid)
					+ ". Destination " + destination);
			System.out.println("@zhumeiqi_debug:finish partition"
					+ this.partition);
			return OutputFileWritable.SUCCESS;
		} else {
			LOG.debug("Unsuccessful send. Transfer response: " + response);
			return OutputFileWritable.ERROR;
		}
	}

	@Override
	public long read(InputStream instream) throws IOException {

		// OutputFile.FileHeader.readHeader(in)

		DataInputStream istream = null;
		istream = new DataInputStream(instream);
		// WritableUtils.readEnum(istream, BufferExchange.BufferType.class);
		file.setHeader(OutputFile.FileHeader.readHeader(istream));
		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		System.out.print("@zhumeiqi_debug:Call read");
		/* Get my position for this source taskid. */
		Integer position = null;
		TaskID inputTaskID = header.owner().getTaskID();
		if (collector.read(istream, header)) {
			this.setValue(header);
			synchronized (task) {
				task.notifyAll();
			}
		}
		if (header.eof()) {
			istream.readInt();
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
