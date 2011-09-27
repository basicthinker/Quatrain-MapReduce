package org.apache.hadoop.mapred.monitor;

import jol.core.JolSystem;
import jol.types.basic.Tuple;
import jol.types.table.BasicTable;
import jol.types.table.Key;
import jol.types.table.TableName;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;

public class MeasurementTable extends BasicTable {
	public static final TableName TABLENAME = new TableName("monitor", "measurement");
	
	/** The primary key */
	public static final Key PRIMARY_KEY = new Key(0, 1, 2);
	
	/** An enumeration of all clock table fields. */
	public enum Field{HOST, TYPE, NAME, TIMESTAMP, VALUE};
	
	/** The table schema types. */
	public static final Class[] SCHEMA = {
		String.class,  // Host
		String.class,  // Measurement type
		String.class,  // Measurement name
		String.class,  // Timestamp
		Float.class    // Value
	};
	
	private SimpleDateFormat df;
	
	public MeasurementTable(JolSystem context) {
		super((jol.core.Runtime) context, TABLENAME, PRIMARY_KEY, SCHEMA);
		df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
	}
	
	public Tuple tuple(Measurement.Type type, List measurement) {
		String host = (String) measurement.get(0);
		String name = (String) measurement.get(1);
		Date   date = (Date) measurement.get(2);
		Float value = (Float) measurement.get(3);
		
		return new Tuple(host, type.name(), name, df.format(date), value);
	}
}
