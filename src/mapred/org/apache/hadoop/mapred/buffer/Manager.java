/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.buffer;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferExchangeSource;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.MapBufferRequest;
import org.apache.hadoop.mapred.buffer.net.ReduceBufferRequest;
import org.apache.hadoop.net.NetUtils;

/**
 * Manages the output buffers and buffer requests of all tasks running on this TaskTracker.
 * A task will generate some number of spill files and a final output. These spill files
 * are contained in {@link #output(OutputFile)} objects and passed to the controller via
 * RPC call from the task process.
 * 
 * Certain tasks will request buffers from upstream tasks. A ReduceTask will request its
 * partition from all map tasks in the same job. A PipelinedMapTask will request the output
 * of the corresponding ReduceTask in the upstream job. 
 * 
 * @author tcondie
 *
 */
public class Manager implements BufferUmbilicalProtocol {
	private static final Log LOG = LogFactory.getLog(Manager.class.getName());

	/**
	 * The BufferController will create a single object of this class to manage the
	 * transfer of all requests. 
	 * @author tcondie
	 *
	 */
	private class RequestTransfer extends Thread {
		/**
		 * Holds all requests that need to be transfered to other BufferControllers
		 * that reside on separate TaskTrackers. We group these requests by the
		 * address of the remote BufferController so we can transfer multiple requests
		 * in a single session. 
		 */
		private Map<InetSocketAddress, Set<BufferRequest>> transfers;

		public RequestTransfer() {
			this.transfers = new HashMap<InetSocketAddress, Set<BufferRequest>>();
		}

		/**
		 * Transfer a new request. This method does not block.
		 * @param request The request to transfer.
		 */
		public void transfer(BufferRequest request) {
			synchronized(transfers) {
				InetSocketAddress source =
					NetUtils.createSocketAddr(request.srcHost() + ":" + controlPort);
				if (!transfers.containsKey(source)) {
					transfers.put(source, new HashSet<BufferRequest>());
				}
				transfers.get(source).add(request);
				transfers.notify();
			}
		}

		/**
		 * The main transfer loop. 
		 */
		public void run() {
			/* I need to ensure that I don't hold the transfer lock while
			 * sending requests. I will dump my requests into these two
			 * HashSet objects then release the transfer lock.
			 */
			Set<InetSocketAddress> locations = new HashSet<InetSocketAddress>();
			Set<BufferRequest>     handle    = new HashSet<BufferRequest>();
			while (!isInterrupted()) {
				synchronized (transfers) {
					while (transfers.size() == 0) {
						try { transfers.wait();
						} catch (InterruptedException e) { }
					}
					locations.clear();
					/* Deal with these locations. */
					locations.addAll(transfers.keySet());
				}

				for (InetSocketAddress location : locations) {
					synchronized(transfers) {
						handle.clear();
						/* Handle these requests. */
						handle.addAll(transfers.get(location));
					}
					
					Socket socket = null;
					DataOutputStream out = null;
					try {
						socket = new Socket();
						socket.connect(location);
						out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						out.writeInt(handle.size());  // Tell remote end how many requests.
						for (BufferRequest request : handle) {
							BufferRequest.write(out, request); // Write the request to the socket.
							LOG.debug("Sent request " + request + " to " + location);
						}
						out.flush();
						out.close();
						out = null;

						synchronized (transfers) {
							/* Clear out the requests that were sent.  Other
							 * requests could have been added in the meantime. */
							transfers.get(location).removeAll(handle);
							if (transfers.get(location).size() == 0) {
								transfers.remove(location);
							}
						}
					} catch (IOException e) {
						LOG.warn("BufferController: Trying to connect to " + location + "."
						          + " Request transfer connection issue " + e);
					} finally {
						try {
							if (out != null) {
								out.close();
							}

							if (socket != null) {
								socket.close();
							}
						} catch (Throwable t) {
							LOG.error(t);
						}
					}
				}

			}
		}

	};


	/**
	 * Manages the output files generated by a given
	 * task attempt. Task attempts will always generate
	 * some number of intermediate spill files and a single
	 * final output, which is the merge of all spill files. 
	 * 
	 * This class is also responsible for calling request
	 * managers with those output files as they are generated.
	 * 
	 * @author tcondie
	 */
	private class FileManager implements Runnable {
		/* Am I active and busy? */
		private boolean open;
		private boolean busy;
		private boolean somethingToSend;

		/* The task attempt whose output files I am managing. */
		private TaskAttemptID taskid;

		/* Intermediate buffer outputs, in generation order. */
		private SortedSet<OutputFile> outputs;

		/* Requests for partitions in my buffer. 
		 * Exchange sources will be ordered by partition so 
		 * that we service them in the correct order (according
		 * to the order of the output file). */
		private SortedSet<BufferExchangeSource> sources;
		
		private float stallfraction = 0f;
		
		public FileManager(TaskAttemptID taskid) {
			this.taskid = taskid;
			this.outputs = new TreeSet<OutputFile>();
			this.sources = new TreeSet<BufferExchangeSource>();
			this.open = true;
			this.busy = false;
			this.somethingToSend = false;
		}

		@Override
		public String toString() {
			return "FileManager: buffer " + taskid;
		}

		@Override
		public int hashCode() {
			return this.taskid.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof FileManager) {
				return ((FileManager)o).taskid.equals(taskid);
			}
			return false;
		}

		/**
		 * Indicates how well I'm keeping up with the generation
		 * of new output files. 
		 * @return
		 */
		public float stallfraction() {
			return this.stallfraction;
		}

		/**
		 * Called with the task attempt is done generating output files. 
		 */
		public void close() {
			synchronized (this) {
				open = false;
				this.notifyAll();
				
				while (busy) {
					try { this.wait();
					} catch (InterruptedException e) { }
				}
			}
		}
		
		
		@Override
		public void run() {
			try {
				/* I don't want to block calls to new output files and/or requests.
				 * I will copy the outputs and requests that I'm about to service
				 * into these objects before servicing them. */
				SortedSet<OutputFile> out = new TreeSet<OutputFile>();
				SortedSet<BufferExchangeSource> src = new TreeSet<BufferExchangeSource>();
				while (open) {
					synchronized (this) {
						while (!somethingToSend && open) {
							LOG.debug(this + " nothing to send.");
							try { this.wait();
							} catch (InterruptedException e) { }
						}
						
						if (!open) return;
						LOG.debug(this + " something to send.");
						out.addAll(this.outputs); // Copy output files.
						src.addAll(this.sources); // Copy requests.
						somethingToSend = false;  // Assume we send everything.
						busy = true;
					}

					try {
						flush(out, src);
					} finally {
						synchronized (this) {
							this.busy = false;
							this.notifyAll();
						}
						
						for (BufferExchangeSource s : src) {
							s.close();
						}
						
						out.clear();
						src.clear();
						if (open) Thread.sleep(1000); // Have a smoke.
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				LOG.info(this + " exiting.");
			}
		}
		
		/**
		 * Register a new request for my output files.
		 * @param request The request manager that will accept
		 * the output files managed by this object.
		 * @throws IOException
		 */
		private void add(BufferExchangeSource source) throws IOException {
			synchronized (this) {
				this.sources.add(source);
				somethingToSend = true;
				this.notifyAll();
			}

		}

		/** 
		 * Add the output file. 
		 * @param file The new output file.
		 * @throws IOException
		 */
		private void add(OutputFile file) throws IOException {
			synchronized (this) {
				this.outputs.add(file);
				somethingToSend = true;
				this.notifyAll();
			}
		}


		/**
		 * Flush the output files to the request managers. 
		 * @param outputs The output files.
		 * @param requests The request managers.
		 * @return Those requests that have been fully satisifed.
		 */
		private void flush(SortedSet<OutputFile> outs, Collection<BufferExchangeSource> srcs) {
			float stalls = 0f;
			float src_cnt = srcs.size();
			for (OutputFile file : outs) {
				Iterator<BufferExchangeSource> siter = srcs.iterator();
				while (siter.hasNext()) {
					if (!open) return;
					BufferExchangeSource src = siter.next();
					if (!file.isServiced(src.destination())) {
						BufferExchange.Transfer result = src.send(file);
						if (result == BufferExchange.Transfer.TERMINATE) {
							LOG.info("Terminating " + this);
							close();
							return;
						}
						else if (BufferExchange.Transfer.RETRY == result) {
							siter.remove();
							stalls++;
							somethingToSend = true; // Try again later.
						}
						else if (BufferExchange.Transfer.IGNORE == result ||
								BufferExchange.Transfer.SUCCESS == result) {
							LOG.debug("Sent file " + file + " to " + src.destination());
							file.serviced(src.destination());
						}
					}
				}
				
				if (file.type() == OutputFile.Type.STREAM &&
						file.paritions() == file.serviced()) {
					/* Assume no speculations for streaming. */
					try {
						LOG.debug("Garbage collect stream " + file.header());
						file.delete(rfs);
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					synchronized(this) {
						this.outputs.remove(file);
					}
				}
			}
			
			this.stallfraction = stalls / src_cnt;
		}
	}

	/* The host TaskTracker object. */
    private TaskTracker tracker;

    /* The local file system, containing the output files. */
    private FileSystem localFs;
    
    private FileSystem rfs;

    /* Used to accept connections made be fellow BufferController
     * objects. The connections are used to transfer BufferRequest
     * objects. */
    private Thread acceptor;

    /* Manages the transfer of BufferRequest objects. */
    private RequestTransfer requestTransfer;

    /* Used to execute FileManager objects. */
	private Executor executor;

	/* The RPC interface server that tasks use to communicate
	 * with the BufferController via the BufferUmbilicalProtocol 
	 * interface. */
	private Server server;

	private ServerSocketChannel channel;

	/* The port number used by all BufferController objects for
	 * accepting BufferRequest objects. */
	private int controlPort;

	/* The host name. */
	private String hostname;

	/* Managers for job level requests (i.e., reduce requesting map outputs). */
	private Map<JobID, Set<BufferExchangeSource>> mapSources;

	/* Managers for task level requests (i.e., a map requesting the output of a reduce). */
	private Map<TaskID, Set<BufferExchangeSource>> reduceSources;

	/* Each task will have a file manager associated with it. */
	private Map<JobID, Map<TaskAttemptID, FileManager>> fileManagers;
	
	private BlockingQueue<OutputFile> queue;
	
	private Thread serviceQueue;

	public Manager(TaskTracker tracker) throws IOException {
		this.tracker   = tracker;
		this.localFs   = FileSystem.getLocal(tracker.conf());
		this.rfs       = ((LocalFileSystem)localFs).getRaw();
		this.requestTransfer = new RequestTransfer();
		this.executor  = Executors.newCachedThreadPool();
		this.mapSources    = new HashMap<JobID, Set<BufferExchangeSource>>();
		this.reduceSources = new HashMap<TaskID, Set<BufferExchangeSource>>();
		this.fileManagers  = new ConcurrentHashMap<JobID, Map<TaskAttemptID, FileManager>>();
		this.hostname      = InetAddress.getLocalHost().getCanonicalHostName();
		
		this.queue = new LinkedBlockingQueue<OutputFile>();
	}

	public static InetSocketAddress getControlAddress(Configuration conf) {
		try {
			int port = conf.getInt("mapred.buffer.manager.data.port", 9021);
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9021");
		}
	}

	public static InetSocketAddress getServerAddress(Configuration conf) {
		try {
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			int port = conf.getInt("mapred.buffer.manager.control.port", 9020);
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("localhost:9020");
		}
	}

	public void open() throws IOException {
		Configuration conf = tracker.conf();
		int maxMaps = conf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
		int maxReduces = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);

		InetSocketAddress serverAddress = getServerAddress(conf);
		this.server = RPC.getServer(this, serverAddress.getHostName(), serverAddress.getPort(),
				maxMaps + maxReduces, false, conf);
		this.server.start();

		this.requestTransfer.setPriority(Thread.MAX_PRIORITY);
		this.requestTransfer.start();

		/** The server socket and selector registration */
		InetSocketAddress controlAddress = getControlAddress(conf);
		this.controlPort = controlAddress.getPort();
		this.channel = ServerSocketChannel.open();
		this.channel.socket().bind(controlAddress);

		this.acceptor = new Thread() {
			@Override
			public void run() {
				while (!isInterrupted()) {
					SocketChannel connection = null;
					try {
						connection = channel.accept();
						DataInputStream in = new DataInputStream(connection.socket().getInputStream());
						int numRequests = in.readInt();
						for (int i = 0; i < numRequests; i++) {
							BufferRequest request = BufferRequest.read(in);
							if (request instanceof ReduceBufferRequest) {
								add((ReduceBufferRequest) request);
							}
							else if (request instanceof MapBufferRequest) {
								add((MapBufferRequest) request);
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					finally {
						try {
							if (connection != null) connection.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		};
		this.acceptor.setDaemon(true);
		this.acceptor.setPriority(Thread.MAX_PRIORITY);
		this.acceptor.start();
		
		this.serviceQueue = new Thread() {
			public void run() {
				List<OutputFile> service = new ArrayList<OutputFile>();
				while (!isInterrupted()) {
					try {
						OutputFile o = queue.take();
						service.add(o);
						queue.drainTo(service);
						for (OutputFile file : service) {
							try {
								if (file != null) add(file);
							} catch (Throwable t) {
								t.printStackTrace();
								LOG.error("Error service file: " + file + ". " + t);
							}
						}
					} catch (Throwable t) {
						t.printStackTrace();
						LOG.error(t);
					}
					finally {
						service.clear();
					}
				}
				LOG.info("Service queue thread exit.");
			}
		};
		this.serviceQueue.setPriority(Thread.MAX_PRIORITY);
		this.serviceQueue.setDaemon(true);
		this.serviceQueue.start();
	}

	public void close() {
		this.serviceQueue.interrupt();
		this.acceptor.interrupt();
		this.server.stop();
		this.requestTransfer.interrupt();
		try { this.channel.close();
		} catch (Throwable t) {}
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		return 0;
	}
	
	public void free(TaskAttemptID tid) {
		synchronized (this) {
			JobID jobid = tid.getJobID();
			if (fileManagers.containsKey(jobid)) {
				Map<TaskAttemptID, FileManager> fm_map = this.fileManagers.get(jobid);
				if (fm_map.containsKey(tid)) {
					fm_map.get(tid).close();
				}
			}
			
		}
	}

	public void free(JobID jobid) {
		synchronized (this) {
			LOG.info("BufferController freeing job " + jobid);

			/* Close all file managers. */
			if (this.fileManagers.containsKey(jobid)) {
				Map<TaskAttemptID, FileManager> fm_map = this.fileManagers.get(jobid);
				for (FileManager fm : fm_map.values()) {
					fm.close();
				}
				this.fileManagers.remove(jobid);
			}

			/* blow away map sources. */
			if (this.mapSources.containsKey(jobid)) {
				this.mapSources.remove(jobid);
			}

			/* blow away reduce sources. */
			Set<TaskID> rids = new HashSet<TaskID>();
			for (TaskID rid : this.reduceSources.keySet()) {
				if (rid.getJobID().equals(jobid)) {
					rids.add(rid);
				}
			}
			for (TaskID rid : rids) {
				this.reduceSources.remove(rid);
			}
		}
	}

	@Override
	public float stallFraction(TaskAttemptID owner) throws IOException {
		FileManager manager = null;

		if (fileManagers.containsKey(owner.getJobID())) {
			Map<TaskAttemptID, FileManager> fm_map = fileManagers.get(owner.getJobID());
			if (fm_map.containsKey(owner)) {
				manager = fm_map.get(owner);
			}
		}

		return manager != null ? manager.stallfraction() : 0f;
	}

	@Override
	public void output(OutputFile file) throws IOException {
		if (file != null) {
			this.queue.add(file);
		}
	}
	
	@Override
	public void request(BufferRequest request) throws IOException {
		if (!request.srcHost().equals(hostname)) {
			requestTransfer.transfer(request); // request is remote.
		}
		else {
			if (request instanceof ReduceBufferRequest) {
				add((ReduceBufferRequest) request);
			}
			else if (request instanceof MapBufferRequest) {
				add((MapBufferRequest) request);
			}
		}
	}
	

	/******************** PRIVATE METHODS ***********************/
	
	private void add(OutputFile file) throws IOException {
		TaskAttemptID taskid = file.header().owner();
		JobID jobid = taskid.getJobID();
		if (!fileManagers.containsKey(jobid)) {
			fileManagers.put(jobid, new ConcurrentHashMap<TaskAttemptID, FileManager>());
		}
		
		Map<TaskAttemptID, FileManager> fileManager = fileManagers.get(jobid);
		if (!fileManager.containsKey(taskid)) {
			LOG.debug("Create new FileManager for task " + taskid);
			synchronized (this) {
				FileManager fm = new FileManager(taskid);
				fileManager.put(taskid, fm);

				/* pick up any outstanding requests and begin service thread */
				register(fm); 
			}
		}
		fileManager.get(taskid).add(file);
	}

	private void add(ReduceBufferRequest request) throws IOException {
		if (request.srcHost().equals(hostname)) {
			LOG.debug("Register " + request);
			synchronized (this) {
				register(request);
			}
		}
		else {
			LOG.error("Request is remote!");
		}
	}

	private void add(MapBufferRequest request) throws IOException {
		if (request.srcHost().equals(hostname)) {
			LOG.debug("Register " + request);
			synchronized (this) {
				register(request);
			}
		}
		else {
			LOG.error("Request is remote!");
		}
	}

	private void register(MapBufferRequest request) throws IOException {
		JobID jobid = request.mapJobId();
		JobConf job = tracker.getJobConf(jobid);
		BufferExchangeSource source = BufferExchangeSource.factory(rfs, job, request);
		if (!this.mapSources.containsKey(jobid)) {
			this.mapSources.put(jobid, new HashSet<BufferExchangeSource>());
			this.mapSources.get(jobid).add(source);
		}
		else if (!this.mapSources.get(jobid).contains(source)) {
			this.mapSources.get(jobid).add(source);
		}
		else {
			LOG.debug("BufferController: request manager already exists." + request);
			source = null;
		}

		if (source != null) {
			if (this.fileManagers.containsKey(jobid)) {
				for (FileManager fm : this.fileManagers.get(jobid).values()) {
					fm.add(source);
				}
			}
		}
	}

	private void register(ReduceBufferRequest request) throws IOException {
		LOG.debug("BufferController register reduce request " + request);

		TaskID taskid = request.reduceTaskId();
		JobConf job = tracker.getJobConf(taskid.getJobID());
		BufferExchangeSource source = BufferExchangeSource.factory(rfs, job, request);

		if (!this.reduceSources.containsKey(taskid)) {
			this.reduceSources.put(taskid, new HashSet<BufferExchangeSource>());
			this.reduceSources.get(taskid).add(source);
		}
		else if (!this.reduceSources.get(taskid).contains(source)) {
			this.reduceSources.get(taskid).add(source);
		}
		else {
			LOG.debug(source + " already exists. request " + request);
			source = null;
		}

		if (source != null) {
			if (this.fileManagers.containsKey(taskid.getJobID())) {
				for (TaskAttemptID attempt : this.fileManagers.get(taskid.getJobID()).keySet()) {
					if (attempt.getTaskID().equals(taskid)) {
						FileManager fm = this.fileManagers.get(taskid.getJobID()).get(attempt);
						fm.add(source);
					}
				}
			}
		}
	}

	private void register(FileManager fm) throws IOException {
		JobID jobid = fm.taskid.getJobID();
		if (fm.taskid.isMap()) {
			if (this.mapSources.containsKey(jobid)) {
				for (BufferExchangeSource source : this.mapSources.get(jobid)) {
					fm.add(source);
				}
			}
		}
		else {
			TaskID taskid = fm.taskid.getTaskID();
			if (this.reduceSources.containsKey(taskid)) {
				for (BufferExchangeSource source : this.reduceSources.get(taskid)) {
					fm.add(source);
				}
			}
		}
		executor.execute(fm);
	}

}
