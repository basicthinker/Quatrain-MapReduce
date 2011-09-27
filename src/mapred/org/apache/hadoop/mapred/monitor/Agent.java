package org.apache.hadoop.mapred.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;

public interface Agent<V, M extends Measurement<V>> {
	
	public List<M> measure();
}
