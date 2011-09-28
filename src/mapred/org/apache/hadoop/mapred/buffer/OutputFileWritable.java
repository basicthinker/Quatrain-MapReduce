/**
 * 
 */
package org.apache.hadoop.mapred.buffer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.stanzax.quatrain.io.ChannelWritable;

/**
 * @author Jinglei Ren
 *
 */
public abstract class OutputFileWritable extends OutputFile implements
		ChannelWritable {

	private static final Log LOG = LogFactory.getLog(
			OutputFileWritable.class.getName());
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
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#ostream
	 * */
	protected DataOutputStream ostream = null;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#istream
	 * */
	protected DataInputStream istream = null;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#conf
	 * */
	protected JobConf conf;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#destination
	 * */
	protected TaskAttemptID destination;
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#partition
	 * */
	protected int partition;
	
	protected OutputFileWritable(FileSystem rfs, JobConf conf, BufferRequest request) {
		this.rfs = rfs;
		this.conf = conf;
		this.destination = request.destination();
		this.partition = request.partition();
	}
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#open(BufferExchange.BufferType bufferType)
	 * */
	protected BufferExchange.Connect open(Socket socket, BufferExchange.BufferType bufferType) {
		if (socket == null || socket.isClosed()) {
		//	socket = new Socket(); // passed in from parameter
			try {
			//	socket.connect(this.address);
				ostream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
				istream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
				
				BufferExchange.Connect connection = 
					WritableUtils.readEnum(istream, BufferExchange.Connect.class);
				if (connection == BufferExchange.Connect.OPEN) {
					WritableUtils.writeEnum(ostream, bufferType);
					ostream.flush();
				}
				else {
					return connection;
				}
			} catch (IOException e) {
			//	if (socket != null && !socket.isClosed()) {
			//		try { socket.close();
			//		} catch (Throwable t) { }
			//	}
			//	socket = null;
				ostream = null;
				istream = null;
				return BufferExchange.Connect.ERROR;
			}
		}
		return BufferExchange.Connect.OPEN;
	}
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#transmit(OutputFile file)
	 * */
	protected BufferExchange.Transfer transmit() {
		try {
			open(rfs);
		} catch (IOException e) {
			/* We don't want to send anymore of this output! */
			return BufferExchange.Transfer.TERMINATE;
		}

		try {
			ostream.writeInt(Integer.MAX_VALUE); // Sending something
			OutputFile.Header header = seek(partition);

			OutputFile.Header.writeHeader(ostream, header);
			ostream.flush();

			BufferExchange.Transfer response = WritableUtils.readEnum(istream, BufferExchange.Transfer.class);
			if (BufferExchange.Transfer.READY == response) {
				LOG.debug(this + " sending " + header);
				write();
				return BufferExchange.Transfer.SUCCESS;
			}
			return response;
		} catch (IOException e) {
			// close(); // Quatrain takes charge of closing sockets.
			LOG.debug(e);
		}
		return BufferExchange.Transfer.RETRY;
	}
	
	/**
	 * @see org.apache.hadoop.mapred.buffer.net.BufferExchangeSource#write(OutputFile.Header header, DataInputStream fstream)
	 * */
	private void write() throws IOException {
//	private void write(OutputFile.Header header, DataInputStream fstream) throws IOException {
		long length = header.compressed();
		if (length == 0 && header.progress() < 1.0f) {
			return;
		}
		
		LOG.debug("Writing data for header " + header);
		long bytesSent = 0L;
		byte[] buf = new byte[64 * 1024];
		int n = dataIn.read(buf, 0, (int)Math.min(length, buf.length));
		while (n > 0) {
			bytesSent += n;
			length -= n;
			ostream.write(buf, 0, n);

			n = dataIn.read(buf, 0, (int) Math.min(length, buf.length));
		}
		ostream.flush();
		LOG.debug(bytesSent + " total bytes sent for header " + header);
	}
	
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

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#setValue(java.lang.Object)
	 */
	@Override
	public abstract void setValue(Object value);

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.ChannelWritable#getValue()
	 */
	@Override
	public abstract Object getValue();

}
