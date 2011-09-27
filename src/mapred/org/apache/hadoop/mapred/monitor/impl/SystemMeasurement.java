package org.apache.hadoop.mapred.monitor.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.monitor.Measurement;

public class SystemMeasurement extends Measurement<Text> {
	private final Pattern sep = Pattern.compile(":");
	
	private Text key;
	
	private Text value;
	
	public SystemMeasurement() {}
	
	public SystemMeasurement(String key, String host, String type, Date date, Float measure) {
		this.key = new Text(key);
		this.value = new Text(host + sep.pattern() + type + sep.pattern() + 
				date.getTime() + sep.pattern() + Float.toString(measure));
	}
	
	@Override
	public Text key() {
		return this.key;
	}

	@Override
	public Text value() {
		return this.value;
	}
	
	@Override
	public List attributes() {
		String[] values = sep.split(this.value.toString());
		List attributes = new ArrayList();
		attributes.add(values[0]);
		attributes.add(values[1]);
		attributes.add(new Date(Long.parseLong(values[2])));
		attributes.add(Float.parseFloat(values[3]));
		return attributes;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key = new Text();
		this.value = new Text();
		
		this.key.readFields(in);
		this.value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
	}

}
