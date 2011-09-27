package org.apache.hadoop.mapred.monitor;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.monitor.impl.SystemMeasurement;

import jol.core.*;
import jol.types.basic.BasicTupleSet;
import jol.types.basic.Tuple;
import jol.types.basic.TupleSet;
import jol.types.exception.JolRuntimeException;
import jol.types.exception.UpdateException;

public class MonitorServer {
	public static final Log LOG = LogFactory.getLog(MonitorServer.class);

	  
	/* The channel used for accepting new connections. */
	private ServerSocketChannel server;
	
	private Thread acceptor;
	
	private JolSystem jolSystem;
	
	/* An executor for running incoming connections. */
	private Executor executor;
	
	private MeasurementTable mTable;
	
	public MonitorServer() throws IOException {
		try {
			this.jolSystem = jol.core.Runtime.create(jol.core.Runtime.DEBUG_WATCH, System.out);
			this.mTable = new MeasurementTable(jolSystem);
			this.jolSystem.catalog().register(this.mTable);
			
			URL monitor = ClassLoader.getSystemResource("../conf/monitor.olg");
			this.jolSystem.install("monitor", monitor);
			this.jolSystem.evaluate();

			String host = InetAddress.getLocalHost().getHostName();
			/* The server socket and selector registration */
			this.server = ServerSocketChannel.open();
			this.server.configureBlocking(true);
			this.server.socket().bind(new InetSocketAddress(host, 0));
			
			
			this.executor = Executors.newCachedThreadPool(); 
			
			LOG.info("Monitor server running at " + address());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	public String address() {
		try {
			String host = InetAddress.getLocalHost().getHostName();
			int    port = this.server.socket().getLocalPort();
			return host + ":" + port;
		} catch (Exception e) {
			return null;
		}
	}
	
	public boolean open() {
		this.acceptor = new Thread() {
			public void run() {
				try {
					while (server.isOpen()) {
						SocketChannel channel = server.accept();
						LOG.debug("Monitor server open connection.");
						channel.configureBlocking(true);
						/* Note: no buffered input stream due to memory pressure. */
						DataInputStream  istream = new DataInputStream(channel.socket().getInputStream());
						Measurement.Type type = WritableUtils.readEnum(istream, Measurement.Type.class);
						LOG.debug("Receiving measurement type " + type);
						if (type == Measurement.Type.SYSTEM) {
							LOG.debug("Executing SYSTEM handler.");
							executor.execute(new SystemMeasurementHandler(istream));
						}
						else {
							LOG.error("Closing unknown measurement stream. " + type);
							istream.close();
						}
					}
				} catch (IOException e) { 
					e.printStackTrace();
				}
				finally {
					LOG.info("Monitor server thread exiting.");
				}
			}
		};
		acceptor.setDaemon(true);
		acceptor.setPriority(Thread.MAX_PRIORITY);
		acceptor.start();
		return true;
	}
	
	
	private class SystemMeasurementHandler implements Runnable {
		DataInputStream istream = null;
		
		public SystemMeasurementHandler(DataInputStream istream) {
			this.istream = istream;
		}
		
		public void run() {
			try {
				TupleSet mTuples = new BasicTupleSet();
				
				boolean open = istream.readBoolean();
				while (open) {
					Measurement measurement = new SystemMeasurement();
					measurement.readFields(istream);
					mTuples.add(mTable.tuple(Measurement.Type.SYSTEM, measurement.attributes()));
					open = istream.readBoolean();
				}
				
				System.err.println("schedule measurements:");
				jolSystem.schedule("monitor", MeasurementTable.TABLENAME, mTuples, null);
				jolSystem.evaluate();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JolRuntimeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try { istream.close();
				} catch (IOException e) {
				}
			}
		}
		
	}

}
