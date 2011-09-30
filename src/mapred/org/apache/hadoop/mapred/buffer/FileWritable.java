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

		System.out.print("@zhumeiqi_debug:Call write");
		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		TaskID taskid = header.owner().getTaskID();
		if (!cursor.containsKey(taskid)
				|| cursor.get(taskid) == header.ids().first()) {
			/*
			 * add @zhumeiqi ,the WritableUtils.only accept DataOutputStream
			 */
			DataOutputStream ostream = new DataOutputStream(Ostream);

			try {
				WritableUtils.writeEnum(ostream, BufferType.FILE);
				ostream.flush();
				System.out.println("@zhumeiqi_write:begin_write FILE_TYPE");
			} catch (Exception e) {
				e.printStackTrace();
				System.out.print("@zhumeiqi_debug:write"
						+ e.getClass().toString() + ":" + e.getMessage());
			} catch (Error e) {
				e.printStackTrace();
				System.out.print("@zhumeiqi_debug:write"
						+ e.getClass().getClass() + ":"
						+ e.getMessage().toString());
			}

			// }

			// if (result == Connect.OPEN) {
			LOG.debug("Transfer file " + file + ". Destination " + destination);
			Transfer response = transmit(ostream);

			if (response == Transfer.TERMINATE) {
				return OutputFileWritable.TERMINATE;
			}
			System.out.println("@zhumeiqi_write:begin_write transmit");
			/* Update my next cursor position. */
			int position = header.ids().last() + 1;
			// try {
			/* [NOTICE] Passed in by parameter instead of read in */
			// int next = istream.readInt();
			// if (position != next) {
			// LOG.debug("Assumed next position " + position + " != actual " +
			// next);
			// position = next;
			// }
			// } catch (IOException e) { e.printStackTrace(); LOG.error(e); }

			if (response == Transfer.SUCCESS) {
				if (header.eof()) {
					LOG.debug("Transfer end of file for source task " + taskid);
					// close(); // Quatrain takes charge of closing sockets.
					ostream.writeInt(0); // [NOTICE] Taken from method close().
				}
				cursor.put(taskid, position);
				LOG.debug("Transfer complete. New position "
						+ cursor.get(taskid) + ". Destination " + destination);
				return OutputFileWritable.SUCCESS;
			} else if (response == Transfer.IGNORE) {
				cursor.put(taskid, position); // Update my cursor position
				return OutputFileWritable.IGNORE;
			} else {
				LOG.debug("Unsuccessful send. Transfer response: " + response);
				return OutputFileWritable.ERROR;
			}
			/* [NOTICE] Some actions discarded. */
			// } else if (result == Connect.BUFFER_COMPLETE) {
			// cursor.put(taskid, Integer.MAX_VALUE);
			// return OutputFileWritable.SUCCESS;
		} else {
			return OutputFileWritable.RETRY;
		}
		// } else {
		// LOG.debug("Transfer ignore header " + header + " current position " +
		// cursor.get(taskid));
		// return OutputFileWritable.IGNORE;
		// }
	}

	@Override
	public long read(InputStream instream) throws IOException {

		// OutputFile.FileHeader.readHeader(in)
		System.out.println("@zhumeiqi_debug:Call read@@@@@@@");
	
		DataInputStream istream = null;
		try {
			istream = new DataInputStream(instream);
			WritableUtils.readEnum(istream, BufferType.class);
			System.out.println("@zhumeiqi_debug:call read ,isStream");
			file.setHeader(OutputFile.FileHeader.readHeader(istream));
			System.out.println("@zhumeiqi_debug:call read ,read_header");
		} catch (Exception e) {
			System.out.print("@zhumeiqi_debug" + e.getClass().toString() + ":"
					+ e.getStackTrace());
			throw new IOException(e);
		} catch (Error e) {
			System.out.print("@zhumeiqi_debug" + e.getClass().toString() + ":"
					+ e.getStackTrace());
		}

		OutputFile.FileHeader header = (OutputFile.FileHeader) file.header();
		/* Get my position for this source taskid. */
		Integer position = null;
		TaskID inputTaskID = header.owner().getTaskID();
		System.out.println("@zhumeiqi_debug:read from " + inputTaskID);
		synchronized (cursor) {
			if (!cursor.containsKey(inputTaskID)) {
				cursor.put(inputTaskID, -1);
			}
			position = cursor.get(inputTaskID);
		}

		/* I'm the only one that should be updating this position. */
		int pos = position.intValue() < 0 ? header.ids().first() : position
				.intValue();
		System.out.println("@zhumeiqi_read:read pos" + pos);
		synchronized (position) {
			if (header.ids().first() == pos) {
				/* [NOTICE] The following singnal is not sent. */
				// WritableUtils.writeEnum(ostream,
				// BufferExchange.Transfer.READY);
				// ostream.flush();
				LOG.debug("File handler " + hashCode()
						+ " ready to receive -- " + header);
				if (collector.read(istream, header)) {
					// updateProgress(header);
					synchronized (task) {
						task.notifyAll();
					}
				}
				position = header.ids().last() + 1;
				LOG.debug("File handler " + " done receiving up to position "
						+ position.intValue());
				System.out.print("@zhumeiqi_read:read over");
			} else {
				LOG.debug(this + " ignoring -- " + header);
				/* [NOTICE] Adjust this signal according to new protocol */
				// WritableUtils.writeEnum(ostream,
				// BufferExchange.Transfer.IGNORE);
			}
		}
		/*
		 * [NOTICE] The following info is not sent. Return value of this method
		 * is only used for log.
		 */
		/* Indicate the next spill file that I expect. */
		pos = position.intValue();
		// LOG.debug("Updating source position to " + pos);
		// ostream.writeInt(pos);
		// ostream.flush();
		return pos;
	}
}
