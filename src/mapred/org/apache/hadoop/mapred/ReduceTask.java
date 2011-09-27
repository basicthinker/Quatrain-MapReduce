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
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.impl.Buffer;
import org.apache.hadoop.mapred.buffer.impl.JInputBuffer;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.buffer.impl.JSnapshotBuffer;
import org.apache.hadoop.mapred.buffer.impl.ValuesIterator;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.BufferExchangeSink;
import org.apache.hadoop.mapred.buffer.net.MapBufferRequest;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

/** A Reduce task. */
public class ReduceTask extends Task {

	private class MapOutputFetcher extends Thread {

		private TaskUmbilicalProtocol trackerUmbilical;
		
		private BufferUmbilicalProtocol bufferUmbilical;

		private BufferExchangeSink sink;
		
		private Reporter reporter;
		
		public MapOutputFetcher(TaskUmbilicalProtocol trackerUmbilical, BufferUmbilicalProtocol bufferUmbilical, Reporter reporter, BufferExchangeSink sink) {
			this.trackerUmbilical = trackerUmbilical;
			this.bufferUmbilical = bufferUmbilical;
			this.reporter = reporter;
			this.sink = sink;
		}

		public void run() {
			Set<TaskID> finishedMapTasks = new HashSet<TaskID>();
			Set<TaskAttemptID>  mapTasks = new HashSet<TaskAttemptID>();

			int eid = 0;
			while (!isInterrupted() && finishedMapTasks.size() < getNumberOfInputs()) {
				try {
					MapTaskCompletionEventsUpdate updates = 
						trackerUmbilical.getMapCompletionEvents(getJobID(), eid, Integer.MAX_VALUE, ReduceTask.this.getTaskID());

					reporter.progress();
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
						{
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							if (!mapTasks.contains(mapTaskId)) {
								mapTasks.remove(mapTaskId);
							}
						}
						break;
						case SUCCEEDED:
						{
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							finishedMapTasks.add(mapTaskId.getTaskID());
						}
						case RUNNING:
						{
							URI u = URI.create(event.getTaskTrackerHttp());
							String host = u.getHost();
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							if (!mapTasks.contains(mapTaskId)) {
								BufferExchange.BufferType type = BufferExchange.BufferType.FILE;
								if (inputSnapshots) type = BufferExchange.BufferType.SNAPSHOT;
								if (stream) type = BufferExchange.BufferType.STREAM;
								
								BufferRequest request = 
									new MapBufferRequest(host, getTaskID(), sink.getAddress(), type, mapTaskId.getJobID(), getPartition());
								try {
									bufferUmbilical.request(request);
									mapTasks.add(mapTaskId);
									if (mapTasks.size() == getNumberOfInputs()) {
										LOG.info("ReduceTask " + getTaskID() + " has requested all map buffers. " + 
												  mapTasks.size() + " map buffers.");
									}
								} catch (IOException e) {
									LOG.warn("BufferUmbilical problem in taking request " + request + ". " + e);
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
					int waittime = mapTasks.size() == getNumberOfInputs() ? 60000 : 1000;
					sleep(waittime);
				} catch (InterruptedException e) { return; }
			}
		}
	}

	static {                                        // register a ctor
		WritableFactories.setFactory
		(ReduceTask.class,
				new WritableFactory() {
			public Writable newInstance() { return new ReduceTask(); }
		});
	}

	private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
	
	protected JOutputBuffer outputBuffer = null;
	protected int numMaps;
    protected Class inputKeyClass;
    protected Class inputValClass;
    protected Class outputKeyClass;
    protected Class outputValClass;
	protected Class<? extends CompressionCodec> codecClass = null;

	protected CompressionCodec codec;
	
	protected InputCollector inputCollector = null;
	
	private boolean reducePipeline = false;
	
	private float   snapshotThreshold = 1f;
	private float   snapshotFreq    = 1f;
	private boolean inputSnapshots = false;
	private boolean stream = false;

	{ 
		getProgress().setStatus("reduce"); 
		setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
	}

	protected Progress copyPhase = getProgress().addPhase("copy sort");
	protected Progress reducePhase = getProgress().addPhase("reduce");
	private Counters.Counter reduceInputKeyCounter = 
		getCounters().findCounter(Counter.REDUCE_INPUT_GROUPS);
	private Counters.Counter reduceInputValueCounter = 
		getCounters().findCounter(Counter.REDUCE_INPUT_RECORDS);
	private Counters.Counter reduceOutputCounter = 
		getCounters().findCounter(Counter.REDUCE_OUTPUT_RECORDS);
	private Counters.Counter reduceCombineInputCounter =
		getCounters().findCounter(Counter.COMBINE_INPUT_RECORDS);
	private Counters.Counter reduceCombineOutputCounter =
		getCounters().findCounter(Counter.COMBINE_OUTPUT_RECORDS);

	public ReduceTask() {
		super();
	}

	public ReduceTask(String jobFile, TaskAttemptID taskId,
			int partition, int numMaps) {
		super(jobFile, taskId, partition);
		this.numMaps = numMaps;
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskTracker.TaskInProgress tip) throws IOException {
		return new ReduceTaskRunner(tip, tracker, this.conf);
	}

	@Override
	public boolean isMapTask() {
		return false;
	}
	
	@Override
	public boolean isPipeline() {
		if (!(jobCleanup || jobSetup || taskCleanup)) {
			return conf != null && 
				   conf.getBoolean("mapred.reduce.pipeline", false);
		}
		return false;
	}

	@Override
	public int getNumberOfInputs() { return numMaps; }

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		conf.setNumMapTasks(numMaps);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		write(out);
	}

	private void readObject(ObjectInputStream in) 
	throws IOException, ClassNotFoundException {
		readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(numMaps);                        // write the number of maps
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		numMaps = in.readInt();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical, final BufferUmbilicalProtocol bufferUmbilical)
	throws IOException {
		// start thread that will handle communication with parent
		startCommunicationThread(umbilical);

		final Reporter reporter = getReporter(umbilical); 
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
	    
	    float [] weights = {0.75f, 0.25f};
	    getProgress().setWeight(weights);
	    
	    this.inputKeyClass = job.getMapOutputKeyClass();
	    this.inputValClass = job.getMapOutputValueClass();
	    
	    this.outputKeyClass = job.getOutputKeyClass();
	    this.outputValClass = job.getOutputValueClass();
	    
		if (job.getCompressMapOutput()) {
			this.codecClass = conf.getMapOutputCompressorClass(DefaultCodec.class);
		}

		reducePipeline = job.getBoolean("mapred.reduce.pipeline", false);
		snapshotFreq   = job.getFloat("mapred.snapshot.frequency", 1f);
		snapshotThreshold = snapshotFreq;
		inputSnapshots  = job.getBoolean("mapred.job.input.snapshots", false);
		
		InputCollector inputCollector = null;
		if (inputSnapshots) {
			LOG.info("Task " + getTaskID() + " creating input snapshot buffer.");
			inputCollector = new JSnapshotBuffer(job, this, reporter, copyPhase, 
				                                 inputKeyClass, inputValClass, codecClass);
		}
		else {
			inputCollector = new JInputBuffer(job, this, reporter, copyPhase, 
				                              inputKeyClass, inputValClass, codecClass);
		}
		
		BufferExchangeSink sink = new BufferExchangeSink(job, inputCollector, this); 
		
		MapOutputFetcher fetcher = new MapOutputFetcher(umbilical, bufferUmbilical, reporter, sink);
		fetcher.setDaemon(true);
		fetcher.start();
		
		setPhase(TaskStatus.Phase.SHUFFLE); 
		stream = job.getBoolean("mapred.stream", false) ||
				 job.getBoolean("mapred.job.monitor", false);
		if (stream) {
			stream(job, inputCollector, sink, reporter, bufferUmbilical);
		}
		else {
			copy(job, inputCollector, sink, reporter, bufferUmbilical);
		}
		fetcher.interrupt();
		
		long begin = System.currentTimeMillis();
		try {
			setPhase(TaskStatus.Phase.REDUCE); 
			reduce(job, reporter, inputCollector, bufferUmbilical, sink.getProgress(), reducePhase);
		} finally {
			reducePhase.complete();
			setProgressFlag();
			inputCollector.free();
		}
		
		done(umbilical);
		LOG.info("Reduce task total time = " + (System.currentTimeMillis() - begin) + " ms.");
	}
	
	protected void stream(JobConf job, InputCollector inputCollector,
			BufferExchangeSink sink, Reporter reporter, BufferUmbilicalProtocol umbilical) throws IOException {
		int window = job.getInt("mapred.reduce.window", 1000);
		long starttime = System.currentTimeMillis();
		synchronized (this) {
			LOG.info("ReduceTask " + getTaskID() + ": in stream function.");
			sink.open();
			long windowTimeStamp = System.currentTimeMillis();
			while(!sink.complete()) {
				setProgressFlag();
				
				if (System.currentTimeMillis() > (windowTimeStamp + window)) {
					LOG.info("ReduceTask: " + getTaskID() + " perform stream window snapshot. window = " + 
							 (System.currentTimeMillis() - windowTimeStamp) + "ms.");
					windowTimeStamp = System.currentTimeMillis();
					reduce(job, reporter, inputCollector, umbilical, sink.getProgress(), null);
					inputCollector.free(); // Free current data
				}
				
				try { this.wait(window);
				} catch (InterruptedException e) { }
			}
			copyPhase.complete();
			setProgressFlag();
			LOG.info("ReduceTask " + getTaskID() + " copy phase completed in " + 
					 (System.currentTimeMillis() - starttime) + " ms.");
			sink.close();
		}
	}
	
	protected void copy(JobConf job, InputCollector inputCollector, 
			BufferExchangeSink sink, Reporter reporter, 
			BufferUmbilicalProtocol bufferUmbilical) 
	throws IOException {
		float maxSnapshotProgress = job.getFloat("mapred.snapshot.max.progress", 0.9f);

		long starttime = System.currentTimeMillis();
		synchronized (this) {
			LOG.info("ReduceTask " + getTaskID() + ": In copy function.");
			sink.open();
			while(!sink.complete()) {
				copyPhase.set(sink.getProgress().get());
				setProgressFlag();
				
				if (sink.getProgress().get() > snapshotThreshold && 
						 sink.getProgress().get() < maxSnapshotProgress) {
						snapshotThreshold += snapshotFreq;
						LOG.info("ReduceTask: " + getTaskID() + " perform snapshot. progress " + (snapshotThreshold - snapshotFreq));
						reduce(job, reporter, inputCollector, bufferUmbilical, sink.getProgress(), null);
						LOG.info("ReduceTask: " + getTaskID() + " done with snapshot. progress " + (snapshotThreshold - snapshotFreq));
				}
				try { this.wait();
				} catch (InterruptedException e) { }
			}
			copyPhase.complete();
			setProgressFlag();
			LOG.info("ReduceTask " + getTaskID() + " copy phase completed in " + 
					 (System.currentTimeMillis() - starttime) + " ms.");
			sink.close();
		}
	}
	
	private void reduce(JobConf job, InputCollector input, OutputCollector output, Reporter reporter, Progress progress) throws IOException {
		Reducer reducer = (Reducer)ReflectionUtils.newInstance(job.getReducerClass(), job);
		// apply reduce function
		try {
			int count = 0;
			ValuesIterator values = input.valuesIterator();
			while (values.more()) {
				count++;
				reducer.reduce(values.getKey(), values, output, reporter);
				values.nextKey();
				
		        if (progress != null) {
		        	progress.set(values.getProgress().get());
		        }
		        if (reporter != null) reporter.progress();
		        setProgressFlag();
			}
			values.close();
			LOG.info("Reducer called on " + count + " records.");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			//Clean up: repeated in catch block below
			reducer.close();
		}
	}
	
	private void reduce(JobConf job, final Reporter reporter, InputCollector inputCollector, 
			            BufferUmbilicalProtocol umbilical, 
			            Progress inputProgress,  Progress reduceProgress) throws IOException {
		boolean snapshot = snapshotFreq < 1f;
		
		if (reducePipeline) {
			inputCollector.flush();
			if (outputBuffer == null) {
				Progress progress = snapshot ? inputProgress : reducePhase; 
				outputBuffer = new JOutputBuffer(umbilical, this, job, reporter, progress, 
												 false, outputKeyClass, outputValClass, codecClass);
			} else {
				outputBuffer.malloc();
			}
			
			LOG.debug("ReduceTask: " + getTaskID() + " start pipelined reduce phase.");
			reduce(job, inputCollector, outputBuffer, reporter, reduceProgress);
			
			OutputFile outputFile = null;
			if (snapshot) {
				outputFile = outputBuffer.snapshot();
			} else {
				outputFile = outputBuffer.close();
			}
			LOG.debug("Register output file " + outputFile);
			umbilical.output(outputFile);
			outputBuffer.free();
			LOG.debug("Reduce phase complete.");
		} else {
			// make output collector
			String filename = snapshot ? 
					getSnapshotOutputName(getPartition(), inputProgress.get()) :
				    getOutputName(getPartition());

			FileSystem fs = FileSystem.get(job);
			final RecordWriter out = job.getOutputFormat().getRecordWriter(fs, job, filename, reporter);  
			OutputCollector outputCollector = new OutputCollector() {
				@SuppressWarnings("unchecked")
				public void collect(Object key, Object value)
				throws IOException {
					out.write(key, value);
					reduceOutputCounter.increment(1);
					// indicate that progress update needs to be sent
					reporter.progress();
				}
			};
			LOG.debug("ReduceTask: create final output file " + filename);
			reduce(job, inputCollector, outputCollector, reporter, reduceProgress);
			out.close(reporter);
		}
	}
}
