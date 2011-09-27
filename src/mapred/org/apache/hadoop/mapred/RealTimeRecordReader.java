package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class RealTimeRecordReader implements RecordReader<LongWritable, LongWritable> {
	
	private long sequence;
	
	public RealTimeRecordReader() {
		this.sequence = 0L;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable(sequence);
	}

	@Override
	public LongWritable createValue() {
		return new LongWritable(System.currentTimeMillis());
	}

	@Override
	public long getPos() throws IOException {
		return sequence;
	}

	@Override
	public float getProgress() throws IOException {
		return 0;
	}

	@Override
	public boolean next(LongWritable key, LongWritable value) throws IOException {
		key.set(sequence++);
		value.set(System.currentTimeMillis());
		return true;
	}

}
