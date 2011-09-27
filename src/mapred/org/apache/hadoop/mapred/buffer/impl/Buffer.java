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

import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class Buffer<K extends Object, V extends Object> {
	
	protected static class CombineOutputCollector<K extends Object, V extends Object>
	implements OutputCollector<K, V> {
		private IFile.Writer<K, V> writer = null;

		public CombineOutputCollector() {
		}
		public synchronized void setWriter(IFile.Writer<K, V> writer) {
			this.writer = writer;
		}

		public synchronized void reset() {
			this.writer = null;
		}

		public synchronized void collect(K key, V value)
		throws IOException {
			if (writer != null) writer.append(key, value);
		}
	}
	
	@SuppressWarnings("unchecked")
	protected void combineAndSpill(CombineOutputCollector<K, V> combineCollector, RawKeyValueIterator kvIter) throws IOException {
		Reducer combiner =
			(Reducer)ReflectionUtils.newInstance(combinerClass, conf);
		try {
			ValuesIterator values = new ValuesIterator(
					kvIter, comparator, keyClass, valClass, conf, reporter);
			while (values.more()) {
				combiner.reduce(values.getKey(), values, combineCollector, reporter);
				values.nextKey();
				// indicate we're making progress
				reporter.progress();
			}
		} finally {
			combiner.close();
		}
	}
	
	protected final JobConf conf;
	
	protected final Task task;
	
	protected final Reporter reporter;
	
	protected final Progress progress;
	
	protected final Class<K> keyClass;
	
	protected final Class<V> valClass;
	
	protected final RawComparator<K> comparator;
	
	protected final Class<? extends Reducer> combinerClass;
	
	protected final CompressionCodec codec;
	
	protected final Decompressor decompressor;
	
	protected Buffer(JobConf conf, Task task, Reporter reporter, Progress progress, 
			         Class<K> keyClass, Class<V> valClass,
			         Class<? extends CompressionCodec> codecClass) {
		this.conf = conf;
		this.task = task;
		this.reporter = reporter;
		this.progress = progress;
		this.keyClass = keyClass;
		this.valClass = valClass;
		this.comparator = conf.getOutputKeyComparator();
		this.combinerClass = conf.getCombinerClass();
		
		if (codecClass != null) {
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			decompressor = CodecPool.getDecompressor(codec);

		} else {
			codec = null;
			decompressor = null;
		}
	}
}
