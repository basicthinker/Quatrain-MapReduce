package org.apache.hadoop.mapred.monitor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ClockInputFormat implements
		InputFormat<LongWritable, LongWritable> {
	
	public static class ClockRecordReader implements RecordReader<LongWritable, LongWritable> {
		@Override
		public void close() throws IOException {
		}

		@Override
		public LongWritable createKey() {
			// TODO Auto-generated method stub
			return new LongWritable(0);
		}

		@Override
		public LongWritable createValue() {
			// TODO Auto-generated method stub
			return new LongWritable(System.currentTimeMillis());
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public float getProgress() throws IOException {
			return 0;
		}

		@Override
		public boolean next(LongWritable key, LongWritable value)
		throws IOException {
			key.set(key.get() + 1);
			value.set(System.currentTimeMillis());
			return true;
		}
	}

	@Override
	public RecordReader<LongWritable, LongWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new ClockRecordReader();
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		return null;
	}

}
