package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RamManager;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.IFile.InMemoryReader;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.StringUtils;

public class JInputBuffer<K extends Object, V extends Object>
extends Buffer<K, V> implements InputCollector<K, V> {
	private static final Log LOG = LogFactory.getLog(JInputBuffer.class.getName());

	/**
	 * This class contains the methods that should be used for metrics-reporting
	 * the specific metrics for shuffle. This class actually reports the
	 * metrics for the shuffle client (the ReduceTask), and hence the name
	 * ShuffleClientMetrics.
	 */
	class ShuffleClientMetrics implements Updater {
		private MetricsRecord shuffleMetrics = null;
		private int numFailedFetches = 0;
		private int numSuccessFetches = 0;
		private long numBytes = 0;
		private int numThreadsBusy = 0;
		ShuffleClientMetrics(JobConf conf) {
			MetricsContext metricsContext = MetricsUtil.getContext("mapred");
			this.shuffleMetrics = 
				MetricsUtil.createRecord(metricsContext, "shuffleInput");
			this.shuffleMetrics.setTag("user", conf.getUser());
			this.shuffleMetrics.setTag("jobName", conf.getJobName());
			this.shuffleMetrics.setTag("jobId", task.getJobID().toString());
			this.shuffleMetrics.setTag("taskId", task.getTaskID().toString());
			this.shuffleMetrics.setTag("sessionId", conf.getSessionId());
			metricsContext.registerUpdater(this);
		}
		public synchronized void inputBytes(long numBytes) {
			this.numBytes += numBytes;
		}
		public synchronized void failedFetch() {
			++numFailedFetches;
		}
		public synchronized void successFetch() {
			++numSuccessFetches;
		}
		public synchronized void threadBusy() {
			++numThreadsBusy;
		}
		public synchronized void threadFree() {
			--numThreadsBusy;
		}
		public void doUpdates(MetricsContext unused) {
			synchronized (this) {
				shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
				shuffleMetrics.incrMetric("shuffle_failed_fetches", 
						numFailedFetches);
				shuffleMetrics.incrMetric("shuffle_success_fetches", 
						numSuccessFetches);
				numBytes = 0;
				numSuccessFetches = 0;
				numFailedFetches = 0;
			}
			shuffleMetrics.update();
		}
	}

	/** Describes an input file; could either be on disk or in-memory. */
	private class JInput {
		final TaskID taskid;

		Path file;

		byte[] data;
		boolean inMemory;
		long compressedSize;

		public JInput(TaskID taskid, Path file, long compressedLength) {
			this.taskid = taskid;

			this.file = file;
			this.compressedSize = compressedLength;

			this.data = null;

			this.inMemory = false;
		}

		public JInput(TaskID taskid, byte[] data, int compressedLength) {
			this.taskid = taskid;

			this.file = null;

			this.data = data;
			this.compressedSize = compressedLength;

			this.inMemory = true;
		}

		public void discard() throws IOException {
			if (inMemory) {
				data = null;
			} else {
				localFileSys.delete(file, true);
			}
		}

		public void replace(Path file) throws IOException {
			if (inMemory || file == null) {
				this.file = file;
				this.inMemory = false;
			}
			else {
				localFileSys.rename(file, this.file);
			}
		}

		public FileStatus status() {
			if (inMemory) {
				return null;
			}
			try {
				return localFileSys.getFileStatus(file);
			} catch (IOException e) {
				LOG.error(e);
				e.printStackTrace();
				return null;
			}
		}
	}

	class ShuffleRamManager implements RamManager {
		/* Maximum percentage of the in-memory limit that a single shuffle can 
		 * consume*/ 
		private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;

		/* Maximum percentage of shuffle-threads which can be stalled 
		 * simultaneously after which a merge is triggered. */ 
		private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

		private final int maxSize;
		private final int maxSingleShuffleLimit;

		private int size;

		private Object dataAvailable = new Object();
		private int fullSize; 
		private int numPendingRequests;
		private int numClosed;
		private boolean closed;

		public ShuffleRamManager(Configuration conf) throws IOException {
			final float maxInMemCopyUse =
				conf.getFloat("mapred.job.shuffle.input.buffer.percent", 0.70f);
			if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
				throw new IOException("mapred.job.shuffle.input.buffer.percent" +
						maxInMemCopyUse);
			}
			maxSize = (int)Math.min(
					Runtime.getRuntime().maxMemory() * maxInMemCopyUse,
					Integer.MAX_VALUE);
			maxSingleShuffleLimit = (int)(maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
			LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize + " ( " + (maxSize * 1000000f) + " mb)" +
					", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
			reset();
		}
		
		public void reset() {
			size = 0;
			fullSize = 0;
			numPendingRequests = 0;
			numClosed = 0;
			closed = false;
		}

		public synchronized boolean reserve(int requestedSize) { 
			// Wait till the request can be fulfilled...
			while ((size + requestedSize) > maxSize) {

				// Track pending requests
				synchronized (dataAvailable) {
					++numPendingRequests;
					dataAvailable.notify();
				}

				// Wait for memory to free up
				try {
					wait();
				} catch (InterruptedException e) {
					return false;
				} finally {
					// Track pending requests
					synchronized (dataAvailable) {
						--numPendingRequests;
					}
				}
			}
			size += requestedSize;
			LOG.debug("JInputBuffer: reserve. size = " + size);
			return true;
		}

		public synchronized void unreserve(int requestedSize) {
			size -= requestedSize;
			LOG.debug("JInputBuffer: unreserve. size = " + size);

			synchronized (dataAvailable) {
				fullSize -= requestedSize;
				--numClosed;
			}

			// Notify the threads blocked on RamManager.reserve
			notifyAll();
		}

		public boolean waitForDataToMerge() throws InterruptedException {
			boolean done = false;
			synchronized (dataAvailable) {
				// Start in-memory merge if manager has been closed or...
				while (!closed
						&&
						// In-memory threshold exceeded and at least two segments
						// have been fetched
						(getPercentUsed() < maxInMemCopyPer || numClosed < 2)
						&&
						// More than "mapred.inmem.merge.threshold" map outputs
						// have been fetched into memory
						(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)) {
					dataAvailable.wait();
				}
				done = closed;
			}
			return done;
		}

		public void closeInMemoryFile(int requestedSize) {
			synchronized (dataAvailable) {
				fullSize += requestedSize;
				++numClosed;
				LOG.debug("JInputBuffer: closeInMemoryFile. percent used = " + getPercentUsed());
				dataAvailable.notify();
			}
		}

		public void close() {
			synchronized (dataAvailable) {
				closed = true;
				LOG.info("Closed ram manager");
				dataAvailable.notify();
			}
		}

		private float getPercentUsed() {
			return (float)fullSize/maxSize;
		}

		int getMemoryLimit() {
			return maxSize;
		}

		boolean canFitInMemory(long requestedSize) {
			return (requestedSize < Integer.MAX_VALUE && 
					requestedSize < maxSingleShuffleLimit);
		}
	}
	

	/* A reference to the RamManager for writing the map outputs to. */
	private ShuffleRamManager ramManager;

	/* A reference to the local file system for writing the map outputs to. */
	private FileSystem localFileSys;
	
	private FileSystem rfs;

	/* Number of files to merge at a time */
	private int ioSortFactor;

	private int spills;
	
	/**
	 * A reference to the throwable object (if merge throws an exception)
	 */
	private volatile Throwable mergeThrowable;

	/**
	 * When we accumulate maxInMemOutputs number of files in ram, we merge/spill
	 */
	private final int maxInMemOutputs;

	private final float maxInMemCopyPer;

	/**
	 * Maximum memory usage of map outputs to merge from memory into
	 * the reduce, in bytes.
	 */
	private final long maxInMemMerge;

	/**
	 * The object for metrics reporting.
	 */
	private ShuffleClientMetrics shuffleClientMetrics = null;

	/** 
	 * List of in-memory map-outputs.
	 */
	private final List<JInput> inputFilesInMemory =
		Collections.synchronizedList(new LinkedList<JInput>());

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and different
	// file paths, the first parameter is considered smaller than the second one.
	// In case of files with same size and path are considered equal.
	private Comparator<JInput> inputFileComparator = 
		new Comparator<JInput>() {
		public int compare(JInput a, JInput b) {
			FileStatus astat = a.status();
			FileStatus bstat = b.status();
			if (astat.getLen() < bstat.getLen())
				return -1;
			else if (astat.getLen() == bstat.getLen())
				return a.file.toString().compareTo(b.file.toString());
			else
				return 1;
		}
	};

	// A sorted set for keeping a set of map output files on disk
	private final SortedSet<JInput> inputFilesOnDisk = 
		new TreeSet<JInput>(inputFileComparator);

	private FileHandle outputHandle = null;


    private LocalFSMerger localFSMergerThread = null;
    private InMemFSMergeThread inMemFSMergeThread = null;
    
    private boolean open = true;


    public JInputBuffer(JobConf conf, Task task, 
    		Reporter reporter, Progress progress,
    		Class<K> keyClass, Class<V> valClass, 
    		Class<? extends CompressionCodec> codecClass)
    throws IOException {
    	super(conf, task, reporter, progress, keyClass, valClass, codecClass);
    	configureClasspath(conf);
    	this.spills = 0;

    	this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
    	this.outputHandle = new FileHandle(task.getJobID());
    	this.outputHandle.setConf(conf);

    	this.ioSortFactor = conf.getInt("io.sort.factor", 10);
    	this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold", 1000);
    	this.maxInMemCopyPer = conf.getFloat("mapred.job.shuffle.merge.percent", 0.66f);

    	float maxRedPer = conf.getFloat("mapred.job.reduce.input.buffer.percent", 0f);
    	if (maxRedPer > 1.0 || maxRedPer < 0.0) {
    		throw new IOException("mapred.job.reduce.input.buffer.percent" +
    				maxRedPer);
    	}
    	this.maxInMemMerge = (int)Math.min(
    			Runtime.getRuntime().maxMemory() * maxRedPer, Integer.MAX_VALUE);

    	// Setup the RamManager
    	ramManager = new ShuffleRamManager(conf);

    	this.localFileSys = FileSystem.getLocal(conf);
    	this.rfs = ((LocalFileSystem)this.localFileSys).getRaw();

    	//start the on-disk-merge thread
    	localFSMergerThread = new LocalFSMerger((LocalFileSystem)localFileSys, conf);
    	localFSMergerThread.start();

    	//start the in memory merger thread
    	inMemFSMergeThread = new InMemFSMergeThread();
    	inMemFSMergeThread.start();
    }
    
	@Override
	public void close() {
		this.open = false;
		this.ramManager.close();
		this.localFSMergerThread.interrupt();
		
		try {
			this.localFSMergerThread.join();
			this.inMemFSMergeThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public synchronized void free() {
		ramManager.reset();
		
		for (JInput memInput : inputFilesInMemory) {
			try {
				memInput.discard();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		for (JInput dskInput : inputFilesOnDisk) {
			try {
				dskInput.discard();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		inputFilesInMemory.clear();
		inputFilesOnDisk.clear();
	}
	
	public void flush() throws IOException {
		flush(0); // Perform full flush
	}
	
	private void flush(long leaveBytes) throws IOException {
		List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K,V>>();
		long mergeOutputSize = 0;
		TaskID taskid = null;
		
		synchronized (inputFilesInMemory) {
			if (inputFilesInMemory.size() == 0) {
				return;
			}

			//name this output file same as the name of the first file that is 
			//there in the current list of inmem files (this is guaranteed to
			//be absent on the disk currently. So we don't overwrite a prev. 
			//created spill). Also we need to create the output file now since
			//it is not guaranteed that this file will be present after merge
			//is called (we delete empty files as soon as we see them
			//in the merge method)

			//figure out the taskid that generated this input 
			taskid = inputFilesInMemory.get(0).taskid;
			mergeOutputSize = createInMemorySegments(inMemorySegments, leaveBytes);
		}

		Path outputPath = outputHandle.getInputFileForWrite(task.getTaskID(), taskid, spills++, mergeOutputSize);

		Writer writer = 
			new Writer(conf, localFileSys, outputPath,
					keyClass, valClass, codec, null);

		RawKeyValueIterator rIter = null;
		try {
			int segments = inMemorySegments.size();
			LOG.info("Initiating in-memory merge with " + 
					segments +
					" segments... total size = " + mergeOutputSize);

			rIter = Merger.merge(conf, localFileSys,
					keyClass, valClass,
					inMemorySegments, inMemorySegments.size(),
					new Path(task.getTaskID().toString()),
					conf.getOutputKeyComparator(), reporter,
					null, null);

			if (null == combinerClass) {
				Merger.writeFile(rIter, writer, reporter, conf);
			} else {
				CombineOutputCollector combineCollector = new CombineOutputCollector();
				combineCollector.setWriter(writer);
				combineAndSpill(combineCollector, rIter);
			}
			writer.close();
			
			LOG.info(task.getTaskID() + 
					" Merge of the " + segments +
					" files in-memory complete." +
					" Local file is " + outputPath + " of size " + 
					localFileSys.getFileStatus(outputPath).getLen());
			
			inMemorySegments.clear();
		} catch (Exception e) { 
			//make sure that we delete the ondisk file that we created 
			//earlier when we invoked cloneFileAttributes
			localFileSys.delete(outputPath, true);
			throw (IOException)new IOException
			("Intermediate merge failed").initCause(e);
		}

		// Note the output of the merge
		FileStatus status = localFileSys.getFileStatus(outputPath);
		addInputFilesOnDisk(new JInput(taskid, outputPath, status.getLen()));

		LOG.info("FLUSH: Merged " + inMemorySegments.size() + " segments, " +
				mergeOutputSize + " bytes to disk to satisfy " + "reduce memory limit");
	}
	
	@Override
	public ValuesIterator<K, V> valuesIterator() throws IOException {
		RawKeyValueIterator kvIter = this.createKVIterator(conf, rfs, reporter);
		return new ValuesIterator<K, V>(kvIter, comparator, keyClass, valClass, conf, reporter);
	}
	
	@Override
	public synchronized boolean read(DataInputStream istream, OutputFile.Header header)
	throws IOException {
		TaskID taskid = header.owner().getTaskID();
		long compressedLength = header.compressed();
		long decompressedLength = header.decompressed();
		
		if (compressedLength < 0 || decompressedLength < 0) {
			LOG.warn("JBuffer: invalid lengths in map output header: id: " +
					taskid + " compressed len: " + compressedLength +
					", decompressed len: " + decompressedLength);
			return false;
		}

		//We will put a file in memory if it meets certain criteria:
		//1. The size of the (decompressed) file should be less than 25% of 
		//    the total inmem fs
		//2. There is space available in the inmem fs

		// Check if this map-output can be saved in-memory
		boolean shuffleInMemory = ramManager.canFitInMemory(decompressedLength); 

		// Shuffle
		if (shuffleInMemory &&
			shuffleInMemory(taskid, istream,
					(int)decompressedLength,
					(int)compressedLength)) {
			LOG.info("Shuffeled " + decompressedLength + " bytes (" + 
					compressedLength + " raw bytes) " + 
					"into RAM from " + taskid);
		} else {
			LOG.info("Shuffling " + decompressedLength + " bytes (" + 
					compressedLength + " raw bytes) " + 
					"into Local-FS from " + taskid);

			Path filename = outputHandle.getInputFileForWrite(task.getTaskID(), taskid, spills++, decompressedLength);

			shuffleToDisk(taskid, istream, filename, compressedLength);
		}
		return true;
	}
	
	private boolean shuffleInMemory(
			TaskID taskid,
			InputStream ins,
			int decompressedLength,
			int compressedLength)
	throws IOException {
		// Reserve ram for the map-output
		boolean createdNow = ramManager.reserve(decompressedLength);
		
		if (!createdNow) {
			return false;
		}

		IFileInputStream checksumIn = new IFileInputStream(ins, compressedLength);
		ins = checksumIn;       

		// Are map-outputs compressed?
		if (codec != null) {
			decompressor.reset();
			ins = codec.createInputStream(ins, decompressor);
		}

		LOG.debug("JBufferInput: copy compressed " + compressedLength + 
				" (decompressed " + decompressedLength + ") bytes from map " + taskid);
		// Copy map-output into an in-memory buffer
		byte[] shuffleData = new byte[decompressedLength];
		JInput input = new JInput(taskid, shuffleData, decompressedLength);

		int bytesRead = 0;
		try {
			int n = ins.read(shuffleData, 0, shuffleData.length);
			while (n > 0) {
				bytesRead += n;
				shuffleClientMetrics.inputBytes(n);

				// indicate we're making progress
				reporter.progress();
				n = ins.read(shuffleData, bytesRead, shuffleData.length-bytesRead);
			}

			LOG.debug("Read " + bytesRead + " bytes from map-output for " + taskid);
		} catch (IOException ioe) {
			LOG.info("Failed to shuffle from " + taskid, 
					ioe);

			// Inform the ram-manager
			ramManager.closeInMemoryFile(decompressedLength);
			ramManager.unreserve(decompressedLength);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				LOG.info("Failed to discard map-output from " + taskid, 
						ignored);
			}
			input = null;

			// Close the streams
			IOUtils.cleanup(LOG, ins);

			// Re-throw
			throw ioe;
		} catch (Throwable t) {
			t.printStackTrace();
			LOG.error(t);
			input = null;
			return false;
		}
		

		// Close the in-memory file
		ramManager.closeInMemoryFile(decompressedLength);

		// Sanity check
		if (bytesRead != decompressedLength) {
			// Inform the ram-manager
			ramManager.unreserve(decompressedLength);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				// IGNORED because we are cleaning up
				LOG.info("Failed to discard map-output from " + taskid, 
						ignored);
			}
			input = null;

			throw new IOException("Incomplete map output received for " +
					taskid + " (" + 
					bytesRead + " instead of " + 
					decompressedLength + ")"
			);
		}

		if (input != null) {
			synchronized (inputFilesInMemory) {
				inputFilesInMemory.add(input);
			}
			return true;
		}
		return false;
	}

	private void shuffleToDisk(
			TaskID taskid,
			InputStream ins,
			Path filename,
			long mapOutputLength) 
	throws IOException {
		JInput input = new JInput(taskid, filename, mapOutputLength);

		// Copy data to local-disk
		OutputStream outs = null;
		int bytesRead = 0;
		try {
			outs = rfs.create(filename);

			byte[] buf = new byte[64 * 1024];
			int n = ins.read(buf, 0, (int) Math.min(buf.length, mapOutputLength));
			while (n > 0) {
				bytesRead += n;
				shuffleClientMetrics.inputBytes(n);
				outs.write(buf, 0, n);

				// indicate we're making progress
				reporter.progress();
				n = ins.read(buf, 0, (int) Math.min(buf.length, mapOutputLength - bytesRead));
			}

			LOG.info("Read " + bytesRead + " bytes from map-output for " + taskid);

			outs.close();
		} catch (IOException ioe) {
			LOG.info("Failed to shuffle from " + taskid, ioe);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				LOG.info("Failed to discard map-output from " + taskid, ignored);
			}
			input = null;

			// Close the streams
			IOUtils.cleanup(LOG, ins, outs);

			// Re-throw
			throw ioe;
		}

		// Sanity check
		if (bytesRead != mapOutputLength) {
			try {
				input.discard();
			} catch (Exception ioe) {
				// IGNORED because we are cleaning up
				LOG.info("Failed to discard map-output from " + taskid, ioe);
			} catch (Throwable t) {
				String msg = task.getTaskID() + " : Failed in shuffle to disk :" 
				+ StringUtils.stringifyException(t);
				LOG.error(msg);
				throw new IOException(t);
			}
			input = null;

			throw new IOException("Incomplete map output received for " +
					taskid + " (" + 
					bytesRead + " instead of " + 
					mapOutputLength + ")"
			);
		}

		if (input != null) addInputFilesOnDisk(input);
	}

	private void configureClasspath(JobConf conf)
	throws IOException {

		// get the task and the current classloader which will become the parent
		ClassLoader parent = conf.getClassLoader();   

		// get the work directory which holds the elements we are dynamically
		// adding to the classpath
		File workDir = new File(task.getJobFile()).getParentFile();
		ArrayList<URL> urllist = new ArrayList<URL>();

		// add the jars and directories to the classpath
		String jar = conf.getJar();
		if (jar != null) {      
			File jobCacheDir = new File(new Path(jar).getParent().toString());

			File[] libs = new File(jobCacheDir, "lib").listFiles();
			if (libs != null) {
				for (int i = 0; i < libs.length; i++) {
					urllist.add(libs[i].toURL());
				}
			}
			urllist.add(new File(jobCacheDir, "classes").toURL());
			urllist.add(jobCacheDir.toURL());

		}
		urllist.add(workDir.toURL());

		// create a new classloader with the old classloader as its parent
		// then set that classloader as the one used by the current jobconf
		URL[] urls = urllist.toArray(new URL[urllist.size()]);
		URLClassLoader loader = new URLClassLoader(urls, parent);
		conf.setClassLoader(loader);
	}

	private long createInMemorySegments(List<Segment<K, V>> inMemorySegments, long leaveBytes)
	throws IOException {
		long totalSize = 0L;
		synchronized (inputFilesInMemory) {
			// fullSize could come from the RamManager, but files can be
			// closed but not yet present in mapOutputsFilesInMemory
			long fullSize = 0L;
			for (JInput in : inputFilesInMemory) {
				fullSize += in.data.length;
			}
			while(fullSize > leaveBytes) {
				JInput in = inputFilesInMemory.remove(0);
				totalSize += in.data.length;
				fullSize -= in.data.length;
				Reader<K, V> reader = 
					new InMemoryReader<K, V>(ramManager, in.taskid,
							in.data, 0, in.data.length);
				Segment<K, V> segment = new Segment<K, V>(reader, true);
				inMemorySegments.add(segment);
				in.discard();
			}
		}
		return totalSize;
	}

	private Path[] getFiles(FileSystem fs)  throws IOException {
		List<Path> fileList = new ArrayList<Path>();
		for (JInput input : inputFilesOnDisk) {
			fileList.add(input.file);
		}
		return fileList.toArray(new Path[0]);
	}

	/**
	 * Create a RawKeyValueIterator from copied map outputs. All copying
	 * threads have exited, so all of the map outputs are available either in
	 * memory or on disk. We also know that no merges are in progress, so
	 * synchronization is more lax, here.
	 *
	 * The iterator returned must satisfy the following constraints:
	 *   1. Fewer than io.sort.factor files may be sources
	 *   2. No more than maxInMemReduce bytes of map outputs may be resident
	 *      in memory when the reduce begins
	 *
	 * If we must perform an intermediate merge to satisfy (1), then we can
	 * keep the excluded outputs from (2) in memory and include them in the
	 * first merge pass. If not, then said outputs must be written to disk
	 * first.
	 */
	@SuppressWarnings("unchecked")
	private RawKeyValueIterator 
	createKVIterator(JobConf job, FileSystem fs, Reporter reporter) throws IOException {
		
		final Path tmpDir = new Path(task.getTaskID().toString());
		TaskID taskid = null;
		
		if (!open && inputFilesInMemory.size() > 0) {
			flush(maxInMemMerge);
		}
		else if (open) {
			if (inputFilesInMemory.size() > 0) {
				taskid = inputFilesInMemory.get(0).taskid;
			} else if (inputFilesOnDisk.size() > 0) {
				taskid = inputFilesOnDisk.first().taskid;
			}
		}
		
		
		// Get all segments on disk
		List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
		long onDiskBytes = 0;
		synchronized (inputFilesOnDisk) {
			Path[] onDisk = getFiles(fs);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, file, codec, false));
			}
			inputFilesOnDisk.clear();
		}
		
		LOG.info("Merging " + diskSegments.size() + " files, " +
				onDiskBytes + " bytes from disk");
		Collections.sort(diskSegments, new Comparator<Segment<K,V>>() {
			public int compare(Segment<K, V> o1, Segment<K, V> o2) {
				if (o1.getLength() == o2.getLength()) {
					return 0;
				}
				return o1.getLength() < o2.getLength() ? -1 : 1;
			}
		});

		// Get all in-memory segments
		List<Segment<K,V>> finalSegments = new ArrayList<Segment<K,V>>();
		long inMemBytes = createInMemorySegments(finalSegments, 0);
		
		
		RawKeyValueIterator riter = null;
		
		// Create and return a merger
		if (0 != onDiskBytes) {
			// build final list of segments from merged backed by disk + in-mem
			final int numInMemSegments = finalSegments.size();
			finalSegments.addAll(diskSegments);

			LOG.info("Merging " + finalSegments.size() + " segments, " +
					(inMemBytes + onDiskBytes) + " bytes from memory");
			riter = Merger.merge(
					job, fs, keyClass, valClass, codec, finalSegments,
					ioSortFactor, numInMemSegments, tmpDir, comparator,
					reporter, false, null, null);
		}
		else {
			// All that remains is in-memory segments
			LOG.info("Merging " + finalSegments.size() + " segments, " +
					inMemBytes + " bytes from memory");
			riter = Merger.merge(job, fs, keyClass, valClass,
					finalSegments, finalSegments.size(), tmpDir,
					comparator, reporter, null, null);
		}
		
		if (open && taskid != null) {
			return new RawKVIteratorWriter(riter, taskid, inMemBytes + onDiskBytes);
		}
		return riter;
	}
	
	/**
	 * An iterator that concurrent writes to a new spill file while client
	 * is reading. This is used for online aggregation. Basically, the task
	 * has called for an iterator while the input buffer is open. We take all
	 * current in-memory and on-disk runs and create a sort-merge iterator. That
	 * sort-merge iterator is passed to this wrapper class, which simulates the
	 * iterator while concurrent writing to a new spill file. When close is called
	 * the new spill file is registered as a new run, using the given taskid, and
	 * will be available during the next iterator call.
	 *
	 */
	class RawKVIteratorWriter implements RawKeyValueIterator {
		private RawKeyValueIterator riter;
		
		private IFile.Writer<K, V> writer;
		
		private Path outputPath;
		
		private TaskID taskid;
		
		public RawKVIteratorWriter(RawKeyValueIterator riter, TaskID taskid, long bytes) throws IOException {
			this.riter = riter;
			this.taskid = taskid;
			this.outputPath = outputHandle.getInputFileForWrite(task.getTaskID(), taskid, spills++, bytes);
			this.writer = new IFile.Writer(conf, localFileSys, outputPath,
					                       keyClass, valClass, codec, null);;
		}

		@Override
		public void close() throws IOException {
			writer.close();
			
			// Register the output of the merge iterator
			FileStatus status = localFileSys.getFileStatus(outputPath);
			addInputFilesOnDisk(new JInput(taskid, outputPath, status.getLen()));
		}

		@Override
		public DataInputBuffer getKey() throws IOException {
			return riter.getKey();
		}

		@Override
		public Progress getProgress() {
			return riter.getProgress();
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			return riter.getValue();
		}

		@Override
		public boolean next() throws IOException {
			if (riter.next()) {
				writer.append(riter.getKey(), riter.getValue());
				return true;
			}
			return false;
		}
		
	}

	/*
	class RawKVIteratorReader extends IFile.Reader<K,V> {
		private final RawKeyValueIterator kvIter;

		public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
		throws IOException {
			super(null, null, size, null, null);
			this.kvIter = kvIter;
		}

		public boolean next(DataInputBuffer key, DataInputBuffer value)
		throws IOException {
			if (kvIter.next()) {
				final DataInputBuffer kb = kvIter.getKey();
				final DataInputBuffer vb = kvIter.getValue();
				final int kp = kb.getPosition();
				final int klen = kb.getLength() - kp;
				key.reset(kb.getData(), kp, klen);
				final int vp = vb.getPosition();
				final int vlen = vb.getLength() - vp;
				value.reset(vb.getData(), vp, vlen);
				bytesRead += klen + vlen;
				return true;
			}
			return false;
		}

		public long getPosition() throws IOException {
			return bytesRead;
		}

		public void close() throws IOException {
			kvIter.close();
		}
	}
	*/

	private void addInputFilesOnDisk(JInput input) throws IOException {
		synchronized (inputFilesOnDisk) {
			inputFilesOnDisk.add(input);
			inputFilesOnDisk.notifyAll();
			LOG.info("Total input files on disk " + inputFilesOnDisk.size());
		}
	}



	/** Starts merging the local copy (on disk) of the map's output so that
	 * most of the reducer's input is sorted i.e overlapping shuffle
	 * and merge phases.
	 */
	private class LocalFSMerger extends Thread {
		private LocalFileSystem localFileSys;
		private LocalDirAllocator lDirAlloc;
		private boolean exit;
		private int mergeFactor;


		public LocalFSMerger(LocalFileSystem fs, JobConf conf) {
			this.localFileSys = fs;
			this.lDirAlloc = new LocalDirAllocator("mapred.local.dir");
			this.exit = false;
			this.mergeFactor = conf.getInt("io.merge.factor", ioSortFactor);

			setName("Thread for merging on-disk files");
			setDaemon(true);
		}
		
		@Override
		public void interrupt() {
			synchronized (inputFilesOnDisk) {
				exit = true;
				inputFilesOnDisk.notifyAll();
			}
		}

		@SuppressWarnings("unchecked")
		public void run() {
			try {
				LOG.info(task.getTaskID() + " Thread started: " + getName());
				while(!exit){
					synchronized (inputFilesOnDisk) {
						while (!exit &&
								inputFilesOnDisk.size() < mergeFactor) {
							LOG.info(task.getTaskID() + " Thread waiting: " + getName() + 
									". Input files on disk " + inputFilesOnDisk.size() + 
									". Waiting for " + (mergeFactor));
							inputFilesOnDisk.wait();
						}
					}
					if(exit) {//to avoid running one extra time in the end
						break;
					}
					List<Path> mapFiles = new ArrayList<Path>();
					long approxOutputSize = 0;
					int bytesPerSum = 
						conf.getInt("io.bytes.per.checksum", 512);
					int files = inputFilesOnDisk.size();
					LOG.info(task.getTaskID() + "We have  " + 
							inputFilesOnDisk.size() + " map outputs on disk. " +
							"Triggering merge of " + files + " files");
					// 1. Prepare the list of files to be merged. This list is prepared
					// using a list of map output files on disk. Currently we merge
					// io.sort.factor files into 1.
					JInput last = null;
					synchronized (inputFilesOnDisk) {
						while (inputFilesOnDisk.size() > 0) {
							last = inputFilesOnDisk.first();
							FileStatus filestatus = last.status();
							inputFilesOnDisk.remove(last);
							mapFiles.add(filestatus.getPath());
							approxOutputSize += filestatus.getLen();
						}
					}

					// sanity check
					if (mapFiles.size() == 0 || last == null) {
						return;
					}

					// add the checksum length
					approxOutputSize += 
						ChecksumFileSystem.getChecksumLength(approxOutputSize, bytesPerSum);

					// 2. Start the on-disk merge process
					Path outputPath = 
						lDirAlloc.getLocalPathForWrite(mapFiles.get(0).toString(), 
								approxOutputSize, conf).suffix(".merged");
					Writer writer =  new Writer(conf, localFileSys, outputPath, 
											    keyClass, valClass, codec, null);
					RawKeyValueIterator iter  = null;
					Path tmpDir = new Path(task.getTaskID().toString());
					try {
						iter = Merger.merge(conf, localFileSys,
								keyClass, valClass,
								codec, mapFiles.toArray(new Path[mapFiles.size()]), 
								true, ioSortFactor, tmpDir, 
								conf.getOutputKeyComparator(), reporter,
								null, null);

						if (null == combinerClass) {
							Merger.writeFile(iter, writer, reporter, conf);
						} else {
							CombineOutputCollector combineCollector = new CombineOutputCollector();
							combineCollector.setWriter(writer);
							combineAndSpill(combineCollector, iter);
						}
						writer.close();
					} catch (Exception e) {
						localFileSys.delete(outputPath, true);
						throw new IOException (StringUtils.stringifyException(e));
					}
					last.replace(outputPath);
					addInputFilesOnDisk(last);

					LOG.info(task.getTaskID() +
							" Finished merging " + mapFiles.size() + 
							" map output files on disk of total-size " + 
							approxOutputSize + "." + 
							" Local output file is " + last.file + " of size " +
							localFileSys.getFileStatus(last.file).getLen());
				}
			} catch (Exception e) {
				LOG.warn(task.getTaskID()
						+ " Merging of the local FS files threw an exception: "
						+ StringUtils.stringifyException(e));
				if (mergeThrowable == null) {
					mergeThrowable = e;
				}
			} catch (Throwable t) {
				String msg = task.getTaskID() + " : Failed to merge on the local FS" 
				+ StringUtils.stringifyException(t);
				LOG.error(msg);
				mergeThrowable = t;
			}
		}
	}

	private class InMemFSMergeThread extends Thread {

		public InMemFSMergeThread() {
			setName("Thread for merging in memory files");
			setPriority(Thread.MAX_PRIORITY);
			setDaemon(true);
		}

		public void run() {
			LOG.info(task.getTaskID() + " Thread started: " + getName());
			try {
				boolean exit = false;
				do {
					exit = ramManager.waitForDataToMerge();
					if (!exit) {
						flush();
					}
				} while (!exit);
			} catch (Exception e) {
				LOG.warn(task.getTaskID() +
						" Merge of the inmemory files threw an exception: "
						+ StringUtils.stringifyException(e));
				mergeThrowable = e;
			} catch (Throwable t) {
				String msg = task.getTaskID() + " : Failed to merge in memory" 
				+ StringUtils.stringifyException(t);
				LOG.error(msg);
				mergeThrowable = t;
			}
		}
	}
}
