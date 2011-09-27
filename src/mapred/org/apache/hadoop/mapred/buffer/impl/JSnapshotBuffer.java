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

package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.util.Progress;

/**
 * This class manages the input to a task when in the form
 * of a snapshot. 
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class JSnapshotBuffer<K extends Object, V extends Object> 
       extends Buffer implements InputCollector<K, V> {
	
	private static final Log LOG = LogFactory.getLog(JSnapshotBuffer.class.getName());

	/**
	 * Contains the data and index files making up the snapshot. Each snapshot
	 * is tagged with a given progress. This class manages all snapshots taken
	 * from the output of a given task. For now, this class only keeps track
	 * of the most recent data and index snapshot files.
	 */
	public class Snapshot {
		/* Have we applied this snapshot to the running task? */
		public boolean fresh = false;

		/* The identifier of the task that produce this snapshot. */
		private TaskID taskid;

		/* Data and index files (living on the local fs). */
		private Path data  = null;

		/* How many bytes are in the data file */
		private long length = 0;

		/* Snapshot progress. */
		private float progress = 0f;

		/* Used for naming snapshot files. */
		private int runs = 0;

		public Snapshot(TaskID taskid) {
			this.taskid = taskid;
		}

		public String toString() {
			return "JBufferSnapshot " + taskid + ": progress = " + progress;
		}

		public Path data() {
			synchronized (this) {
				return this.data;
			}
		}
		
		public void discard() {
			synchronized (this) {
				if (this.data != null) {
					try {
						localFs.delete(data, true);
					} catch (IOException e) {
						e.printStackTrace();
						LOG.error("Snapshot discard error: " + e);
					} finally {
						data = null;
					}
				}
			}
		}

		/**
		 * Create a new snapshot.
		 * @throws IOException
		 */
		public void
		read(DataInputStream istream, OutputFile.SnapshotHeader header)
		throws IOException {
			if (this.progress < header.progress()) {
				long bytes = header.compressed();
				Path filename = 
					fileHandle.getInputSnapshotFileForWrite(task.getTaskID(), taskid, runs++, bytes);
				// Copy data to local-disk
				OutputStream output = null;
				output = localFs.create(filename);

				byte[] buf = new byte[64 * 1024];
				int n = istream.read(buf, 0, (int) Math.min(bytes, buf.length));
				while (n > 0) {
					bytes -= n;
					output.write(buf, 0, n);
					n = istream.read(buf, 0, (int) Math.min(bytes, buf.length));
				}
				output.close();
				
				synchronized (this) {
					if (this.progress < header.progress()) {
						this.data = filename;
						this.progress = header.progress();
						this.length = header.decompressed();
					}
				}
			}
		}
	}
	
	/* Number of files to merge at a time */
	private final int ioSortFactor;

	/* The local filesystem handle. */
	private final FileSystem localFs;

	/* Used to name new snapshot files. */
	private final FileHandle fileHandle;

	/* Keeps track of the most recent snapshot taken
	 * from each input task. */
	private Map<TaskID, Snapshot> snapshots;

	public JSnapshotBuffer(JobConf conf, Task task, Reporter reporter, Progress progress,
			Class<K> keyClass, Class<V> valClass, 
			Class<? extends CompressionCodec> codecClass) 
	throws IOException {
		super(conf, task, reporter, progress, keyClass, valClass, codecClass);
		this.fileHandle = new FileHandle(task.getJobID());;
		this.fileHandle.setConf(conf);

		this.snapshots = new HashMap<TaskID, Snapshot>();
		this.localFs = FileSystem.getLocal(conf);
		
		this.ioSortFactor = conf.getInt("io.sort.factor", 10);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public boolean read(DataInputStream istream, OutputFile.Header header) throws IOException {
		synchronized (this.snapshots) {
			TaskID taskid = header.owner().getTaskID();
			if (!this.snapshots.containsKey(taskid)) {
				this.snapshots.put(taskid, new Snapshot(taskid));
			}

			Snapshot snapshot = this.snapshots.get(taskid);
			if (snapshot.progress < header.progress()) {
				/* read the snapshot data */
				snapshot.read(istream, (OutputFile.SnapshotHeader) header);
				return true;
			}
			return false;
		}
	}

	@Override
	public void flush() throws IOException {
		// TODO Finish when in-memory buffers implemented
	}

	@Override
	public void free() {
		for (Snapshot snapshot : this.snapshots.values()) {
			snapshot.discard();
		}
	}

	@Override
	public ValuesIterator<K, V> valuesIterator() throws IOException {
		synchronized (this.snapshots) {
			RawKeyValueIterator kvIter = this.createKVIterator(conf, localFs, reporter);
			return kvIter != null ? 
					new ValuesIterator<K, V>(kvIter, comparator, keyClass, valClass, conf, reporter) :
					null;
		}
	}
	
	@SuppressWarnings("unchecked")
	private RawKeyValueIterator 
	createKVIterator(JobConf job, FileSystem fs, Reporter reporter) throws IOException {
		// segments required to vacate memory
		final Path tmpDir = new Path(task.getTaskID().toString());

		// segments on disk
		List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
		long onDiskBytes = 0L;
		for (Snapshot snapshot : this.snapshots.values()) {
			Path data = snapshot.data();
			if (data != null && fs.exists(data)) {
				onDiskBytes += fs.getFileStatus(data).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, data, codec, false));
			}
			else if (data != null && !fs.exists(data)) {
				LOG.warn("Snapshot data is missing from file system! " + data);
			}
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

		if (0 != onDiskBytes) {
			return Merger.merge(
					job, fs, keyClass, valClass, codec, diskSegments,
					ioSortFactor, diskSegments.size(), tmpDir, comparator,
					reporter, false, null, null);
		}
		return null;
	}

}
