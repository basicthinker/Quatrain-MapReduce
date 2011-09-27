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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.TaskCompletionEvent.Status;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputFile.Header;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.buffer.impl.ValuesIterator;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.BufferExchangeSink;
import org.apache.hadoop.mapred.buffer.net.ReduceBufferRequest;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

public class PipelineMapTask extends MapTask implements InputCollector {
	  private static final Log LOG = LogFactory.getLog(PipelineMapTask.class.getName());
	
	
	private class ReduceOutputFetcher extends Thread {
		private TaskID reduceTaskId;

		private TaskUmbilicalProtocol trackerUmbilical;
		
		private BufferUmbilicalProtocol bufferUmbilical;

		private BufferExchangeSink sink;
		
		public ReduceOutputFetcher(TaskUmbilicalProtocol trackerUmbilical, 
				BufferUmbilicalProtocol bufferUmbilical, 
				BufferExchangeSink sink, 
				TaskID reduceTaskId) {
			this.trackerUmbilical = trackerUmbilical;
			this.bufferUmbilical = bufferUmbilical;
			this.sink = sink;
			this.reduceTaskId = reduceTaskId;
		}

		public void run() {
			boolean requestSent = false;
			int eid = 0;
			while (true) {
				try {
					ReduceTaskCompletionEventsUpdate updates = 
						trackerUmbilical.getReduceCompletionEvents(getJobID(), eid, Integer.MAX_VALUE);

					eid += updates.events.length;

					// Process the TaskCompletionEvents:
					// 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
					// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop fetching
					//    from those maps.
					// 3. Remove TIPFAILED maps from neededOutputs since we don't need their
					//    outputs at all.
					for (TaskCompletionEvent event : updates.events) {
						switch (event.getTaskStatus()) {
						case FAILED:
						case KILLED:
						case OBSOLETE:
						case TIPFAILED:
							return;
						case SUCCEEDED:
							if (requestSent) return;
						case RUNNING:
						{
							URI u = URI.create(event.getTaskTrackerHttp());
							String host = u.getHost();
							TaskAttemptID reduceAttemptId = event.getTaskAttemptId();
							if (reduceAttemptId.getTaskID().equals(reduceTaskId) && !requestSent) {
								LOG.debug("Map " + getTaskID() + " sending buffer request to reducer " + reduceAttemptId);
								BufferExchange.BufferType type = BufferExchange.BufferType.FILE;
								if (snapshots) type = BufferExchange.BufferType.SNAPSHOT;
								if (stream) type = BufferExchange.BufferType.STREAM;
								
								BufferRequest request = 
									new ReduceBufferRequest(host, getTaskID(), sink.getAddress(), type, reduceTaskId);
								
								try {
									bufferUmbilical.request(request);
									requestSent = true;
									if (event.getTaskStatus() == Status.SUCCEEDED) return;
								} catch (IOException e) {
									LOG.warn("BufferUmbilical problem sending request " + request + ". " + e);
								}
							}
						}
						break;
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) { }
			}
		}
	}
	
	private JOutputBuffer buffer = null;
	
	private Mapper mapper;
	
	private BufferUmbilicalProtocol bufferUmbilical;
	
	private Reporter reporter;
	  
	private Deserializer keyDeserializer;
	
	private Deserializer valDeserializer;
	
	private boolean snapshots = false;
	private boolean stream = false;
	
	public PipelineMapTask() {
		super();
	}
	
	public PipelineMapTask(String jobFile, TaskAttemptID taskId, int partition) {
		super(jobFile, taskId, partition, "", new BytesWritable());
	}
	
	@Override
	public int getNumberOfInputs() { return 1; }
	
	@Override
	public boolean isPipeline() {
		return !(jobCleanup || jobSetup || taskCleanup);
	}
	
	public TaskID pipelineReduceTask(JobConf job) {
		JobID reduceJobId = JobID.forName(job.get("mapred.job.pipeline"));
		return new TaskID(reduceJobId, false, getTaskID().getTaskID().id);
	}
	
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run(final JobConf job, final TaskUmbilicalProtocol umbilical, final BufferUmbilicalProtocol bufferUmbilical)
	throws IOException {
		this.reporter = getReporter(umbilical);
		this.bufferUmbilical = bufferUmbilical;
	    // start thread that will handle communication with parent
	    startCommunicationThread(umbilical);

		initialize(job, reporter);

	    // check if it is a cleanupJobTask
	    if (jobCleanup) {
	      runJobCleanupTask(umbilical);
	      return;
	    }
	    if (jobSetup) {
	      runJobSetupTask(umbilical);
	      return;
	    }
	    if (taskCleanup) {
	      runTaskCleanupTask(umbilical);
	      return;
	    }
	    
	    if (job.get("mapred.job.pipeline", null) == null) {
	    	throw new IOException("PipelineMapTask: mapred.job.pipeline is not defined!");
	    }
		setPhase(TaskStatus.Phase.PIPELINE); 

	    Class inputKeyClass = job.getInputKeyClass();
	    Class inputValClass = job.getInputValueClass();
	    SerializationFactory serializationFactory = new SerializationFactory(job);
	    keyDeserializer = serializationFactory.getDeserializer(inputKeyClass);
	    valDeserializer = serializationFactory.getDeserializer(inputValClass);
	    
		int numReduceTasks = job.getNumReduceTasks();
		if (numReduceTasks == 0) {
			throw new IOException("PipelineMaptask has no reduce tasks!");
		}
		
		snapshots  = job.getBoolean("mapred.job.input.snapshots", false);

	    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
	    
	    /* This object will be the sink's input buffer. */
		BufferExchangeSink sink = new BufferExchangeSink(job, this, this); 
		sink.open();
		
		/* Start the reduce output fetcher */
		TaskID reduceTaskId = pipelineReduceTask(job);
		ReduceOutputFetcher rof = new ReduceOutputFetcher(umbilical, bufferUmbilical, sink, reduceTaskId);
		rof.setDaemon(true);
		
		long timestamp = System.currentTimeMillis();
		synchronized (this) {
			LOG.info("PipelineMapTask: copy phase.");
			setPhase(TaskStatus.Phase.SHUFFLE); 
			rof.start();
			while (!sink.complete()) {
				setProgressFlag();
				try { this.wait();
				} catch (InterruptedException e) { }
			}
			LOG.info("PipelineMapTask: copy input took " + (System.currentTimeMillis() - timestamp) + " ms.");
		}
		
		setPhase(TaskStatus.Phase.MAP); 
		setProgressFlag();
		
		sink.close();
		timestamp = System.currentTimeMillis();
		getProgress().complete();
		setProgressFlag();
		
		/*
		OutputFile finalOutput = this.buffer.close();
		bufferUmbilical.output(finalOutput);
		*/
		getProgress().complete();
		
		LOG.info("PipelineMapTask: took " + (System.currentTimeMillis() - timestamp) + " ms to finalize final output.");

		done(umbilical);
	}
	
	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void free() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public synchronized boolean read(DataInputStream istream, Header header) throws IOException {
		CompressionCodec codec = null;
		Class<? extends CompressionCodec> codecClass = null;
		
		if (conf.getCompressMapOutput()) {
			codecClass = conf.getMapOutputCompressorClass(DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		}
		
		if (this.buffer == null) {
		    Class outputKeyClass = conf.getMapOutputKeyClass();
		    Class outputValClass = conf.getMapOutputValueClass();
			this.buffer = new JOutputBuffer(bufferUmbilical, this, conf, reporter, 
											getProgress(), false, 
						                    outputKeyClass, outputValClass, codecClass);
		} else {
			this.buffer.malloc();
		}
		
		IFile.Reader reader = new IFile.Reader(conf, istream, header.compressed(), codec, null);
		DataInputBuffer key = new DataInputBuffer();
		DataInputBuffer value = new DataInputBuffer();
		Object keyObject = null;
		Object valObject = null;
		while (reader.next(key, value)) {
			keyDeserializer.open(key);
			valDeserializer.open(value);
			keyObject = keyDeserializer.deserialize(keyObject);
			valObject = valDeserializer.deserialize(valObject);
			mapper.map(keyObject, valObject, buffer, reporter);
		}
		
		
		/* Note: do not close reader otherwise input stream will be closed as well. */
		if (header.type() == OutputFile.Type.SNAPSHOT) {
			LOG.info("PipelineMapTask forward snapshot. progress = " + header.progress());
			/* forward snapshot data. */
			getProgress().set(header.progress());
			OutputFile snapshot = buffer.snapshot();
			bufferUmbilical.output(snapshot);
		}
		else if (header.type() == OutputFile.Type.STREAM) {
			OutputFile.StreamHeader streamHeader = (OutputFile.StreamHeader) header;
			LOG.info("PipelineMapTask forward stream. sequence = " + streamHeader.sequence());
			buffer.stream(streamHeader.sequence(), false);
		}
		this.buffer.free();
		
		/* Get ready for the next round */
	    this.mapper = ReflectionUtils.newInstance(conf.getMapperClass(), conf);
		
		return true;
	}

	@Override
	public ValuesIterator valuesIterator() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
}
