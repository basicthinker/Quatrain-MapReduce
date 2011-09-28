/**
 * 
 */
package org.apache.hadoop.mapred.buffer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.OutputFile.Header;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.stanzax.quatrain.io.ChannelWritable;

/**
 * @author Jinglei Ren
 *
 */
public abstract class OutputFileWritable implements ChannelWritable {

	private static final Log LOG = LogFactory.getLog(
			OutputFileWritable.class.getName());
	
	protected OutputFile file;
	protected JobConf conf;
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchange#Transfer
	 * */
	public static final int READY = 0, IGNORE = -1, SUCCESS = 1, RETRY = 2, TERMINATE = -2;
	public static final int ERROR = -3;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#rfs
	 * */
	private FileSystem rfs;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#destination
	 * */
	protected TaskAttemptID destination;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#partition
	 * */
	protected int partition;
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSink
	 * */
	protected InputCollector<?, ?> collector;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSink
	 * */
	protected Task task;
	
	/**
	 * Construct instance intended for write operation
	 * */
	protected OutputFileWritable(OutputFile file, FileSystem rfs, JobConf conf, BufferRequest request) {
		this.file = file;
		this.rfs = rfs;
		this.conf = conf;
		this.destination = request.destination();
		this.partition = request.partition();
	}
	
	/**
	 * Construct instance intended for read operation
	 * */
	protected OutputFileWritable(OutputFile file, JobConf conf, InputCollector<?, ?> collector, Task task) {
		this.file = file;
		this.collector = collector;
		this.task = task;
	}
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#transmit(OutputFile file)
	 * */
	protected BufferExchange.Transfer transmit(DataOutputStream ostream) {
		try {
			file.open(rfs);
		} catch (IOException e) {
			/* We don't want to send anymore of this output! */
			return BufferExchange.Transfer.TERMINATE;
		}

		try {
			ostream.writeInt(Integer.MAX_VALUE); // Sending something
			OutputFile.Header header = file.seek(partition);

			OutputFile.Header.writeHeader(ostream, header);
			ostream.flush();

			/* [NOTICE] */
		//	BufferExchange.Transfer response = WritableUtils.readEnum(istream, BufferExchange.Transfer.class);
		//	if (BufferExchange.Transfer.READY == response) {
				LOG.debug(this + " sending " + header);
				write(header, file.dataInputStream(), ostream);
				return BufferExchange.Transfer.SUCCESS;
		//	}
		//	return response;
		} catch (IOException e) {
			// close(); // Close so reconnect will figure out current status.
						// Quatrain takes charge of closing sockets.
			LOG.debug(e);
		}
		return BufferExchange.Transfer.RETRY;
	}
	
	/**
	 * @param fstream 
	 * @param header 
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#write(OutputFile.Header header, DataInputStream fstream)
	 * */
	private void write(Header header, DataInputStream fstream, DataOutputStream ostream) throws IOException {
		long length = header.compressed();
		if (length == 0 && header.progress() < 1.0f) {
			return;
		}
		
		LOG.debug("Writing data for header " + header);
		long bytesSent = 0L;
		byte[] buf = new byte[64 * 1024];
		int n = fstream.read(buf, 0, (int)Math.min(length, buf.length));
		while (n > 0) {
			bytesSent += n;
			length -= n;
			ostream.write(buf, 0, n);

			n = fstream.read(buf, 0, (int) Math.min(length, buf.length));
		}
		ostream.flush();
		LOG.debug(bytesSent + " total bytes sent for header " + header);
	}
	
	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#setValue(java.lang.Object)
	 */
	@Override
	public void setValue(Object value) {
		this.value = (Header)value;
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#getValue()
	 */
	@Override
	public Object getValue() {
		return value;
	}
	
	private Header value;
	
	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#write(java.nio.channels.SocketChannel)
	 */
	@Override
	public abstract long write(SocketChannel channel) throws IOException;

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#read(java.nio.channels.SocketChannel)
	 */
	@Override
	public abstract long read(SocketChannel channel) throws IOException;

}
