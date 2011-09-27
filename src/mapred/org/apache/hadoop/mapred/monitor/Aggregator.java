package org.apache.hadoop.mapred.monitor;

import java.util.Collection;
import java.util.Iterator;

public interface Aggregator<V, M extends Measurement<V>> {
	
	public Collection<M> aggregate(Iterator<Measurement<V>> measurements);
	
}
