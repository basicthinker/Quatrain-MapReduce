package org.apache.hadoop.mapred.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public abstract class Measurement<V> implements Writable {
	public static enum Type{SYSTEM, LOG};
	
	@Override
	public String toString() {
		if (key() == null) return "";
		
		StringBuilder sb = new StringBuilder();
		sb.append(key().toString() + " : <");
		List attributes = attributes();
		if (attributes != null && attributes.size() > 0) {
			sb.append(attributes.get(0));
			for (int i = 1; i < attributes.size(); i++) {
				sb.append(", " + attributes.get(i));
			}
		}
		sb.append(">");
		return sb.toString();
	}
	
	public abstract Text key();
	
	public abstract V value();
	
	public abstract List attributes();
}
