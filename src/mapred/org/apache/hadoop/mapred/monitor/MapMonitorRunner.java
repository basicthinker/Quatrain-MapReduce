package org.apache.hadoop.mapred.monitor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;

public class MapMonitorRunner<V> extends MapRunner<LongWritable, LongWritable, Text, Measurement<V>> {
	
	@Override
	public void run(RecordReader<LongWritable, LongWritable> input, 
			OutputCollector<Text, Measurement<V>> output, Reporter reporter)
	throws IOException {
		try {
			JOutputBuffer<Text, Measurement<V>> buffer = (JOutputBuffer<Text, Measurement<V>>) output;
			// allocate key & value instances that are re-used for all entries
			LongWritable key = input.createKey();
			LongWritable value = input.createValue();

			while (input.next(key, value)) {
				// map pair to output
				mapper.map(key, value, output, reporter);
				buffer.stream(key.get(), true);
			}
		} finally {
			mapper.close();
		}
	}



}
