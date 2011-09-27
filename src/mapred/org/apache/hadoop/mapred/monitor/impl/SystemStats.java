package org.apache.hadoop.mapred.monitor.impl;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.monitor.Agent;
import org.apache.hadoop.mapred.monitor.Aggregator;
import org.apache.hadoop.mapred.monitor.Measurement;
import org.apache.hadoop.mapred.monitor.impl.SystemMeasurement;
import org.apache.hadoop.mapred.monitor.MonitorClient;

public class SystemStats extends MonitorClient {
	
	private final Pattern whiteSpace = Pattern.compile("\\s+");
	
	public static enum SystemStatEntry {
		USER(0, "normal processes executing in user mode"),
		NICE(1, "niced processes executing in user mode"),
		SYSTEM(2, "processes executing in kernel mode"),
		IDLE(3, "twiddling thumbs"),
		IOWAIT(4, "waiting for I/O to complete"),
		IRQ(5, "servicing interrupts"),
		SOFTIRQ(6, "softirq: servicing softirqs"),
		GUEST(7, "guest virtual machine time"),
        LOAD_1(8, "1 minute load average"), 
        LOAD_5(9, "5 minute load average"),
        LOAD_15(10, "15 minute load average"),
		PAGEIN(11, "pagein"),
		PAGEOUT(12, "pageout"),
		SWAPIN(13, "swaps in"),
		SWAPOUT(14, "swaps out"),
		BYTES_RECEIVE(15, "bytes received"),
		PACKETS_RECEIVE(16, "packets received"),
		ERRORS_RECEIVE(17, "receive errors"),
		BYTES_SEND(18, "bytes send"),
		PACKETS_SEND(19, "packets send"),
		ERRORS_SEND(20, "send errors");
		
		public final int offset;
		public final String name;
		private SystemStatEntry(int o, String n) {
			this.offset = o; this.name = n;
		}
	}
	
	private static SystemStatEntry[] entries = SystemStatEntry.values();

	
	private class AgentImpl implements Agent<Text, SystemMeasurement> {
		private final Number[] values;

		public AgentImpl() {
			this.values = new Number[SystemStatEntry.values().length];
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			SystemStatEntry[] entries = SystemStatEntry.values();
			for (int i = 0; i < values.length; i++) {
				sb.append("Measure " + entries[i].name + " = " + values[i] + "\n");
			}
			return sb.toString();
		}

		@Override
		public List<SystemMeasurement> measure() {
			try {
				long timestamp = System.currentTimeMillis();
				stat();
				List<SystemMeasurement> measurements = new ArrayList<SystemMeasurement>();
				for (int i = 0; i < values.length; i++) {
					measurements.add(
							new SystemMeasurement(hostname, 
									hostname, entries[i].name(), 
									new Date(timestamp), values[i].floatValue()));
				}
				return measurements;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		private void stat() throws IOException {
			File f = new File("/proc/stat");
			BufferedReader in = new BufferedReader(new FileReader(f));
			String info = in.readLine();
			if(!info.startsWith("cpu  ")) { throw new IllegalStateException("Can't parse /proc/stat!"); }
			info = info.substring(5);
			String[] tok = whiteSpace.split(info);
			for(int i = 0; i < tok.length; i++) {
				values[i] = Long.parseLong(tok[i]);
			}
			in.close();
			
			/* Load averages */ 
			f = new File("/proc/loadavg");
			in = new BufferedReader(new FileReader(f));
			info = in.readLine();
			tok = whiteSpace.split(info);
			values[SystemStatEntry.LOAD_1.offset] = Double.parseDouble(tok[0]);
			values[SystemStatEntry.LOAD_5.offset] = Double.parseDouble(tok[1]);
			values[SystemStatEntry.LOAD_15.offset] = Double.parseDouble(tok[2]);

	                    
			f = new File("/proc/vmstat");
			in = new BufferedReader(new FileReader(f));
			while ((info = in.readLine()) != null) {
				tok = whiteSpace.split(info);
				if (tok[0].equals("pgpgin")) {
					values[SystemStatEntry.PAGEIN.offset] = Long.parseLong(tok[1]);
				} else if (tok[0].equals("pgpgout")) {
					values[SystemStatEntry.PAGEOUT.offset] = Long.parseLong(tok[1]);
				} else if (tok[0].equals("pswpin")) {
					values[SystemStatEntry.SWAPIN.offset] = Long.parseLong(tok[1]);
				} else if (tok[0].equals("pswpout")) {
					values[SystemStatEntry.SWAPOUT.offset] = Long.parseLong(tok[1]);
				}
			}
			in.close();
			
			f = new File("/proc/net/dev");
			in = new BufferedReader(new FileReader(f));
			String netinfo = null;
			while((netinfo = in.readLine()) != null) {
				if(netinfo.contains("eth0:")) { 
					netinfo = netinfo.substring(netinfo.indexOf(':') + 1);
					tok = whiteSpace.split(netinfo);
					values[SystemStatEntry.BYTES_RECEIVE.offset] = Long.parseLong(tok[1]);
					values[SystemStatEntry.PACKETS_RECEIVE.offset] = Long.parseLong(tok[2]);
					values[SystemStatEntry.ERRORS_RECEIVE.offset] = Long.parseLong(tok[3]);
						
					values[SystemStatEntry.BYTES_SEND.offset] = Long.parseLong(tok[9]);
					values[SystemStatEntry.PACKETS_SEND.offset] = Long.parseLong(tok[10]);
					values[SystemStatEntry.ERRORS_SEND.offset] = Long.parseLong(tok[11]);
					break;
				}
			}
			in.close();
		}
	}
	
	private class AggregatorImpl implements Aggregator<Text, Measurement<Text>> {
		private final Pattern sep = Pattern.compile("\\s+");
		
		@Override
		public Collection<Measurement<Text>> aggregate(Iterator<Measurement<Text>> measurements) {
			Map<String, Collection<Number>> m_map = new HashMap<String, Collection<Number>>();
			
			while (measurements.hasNext()) {
				Measurement<Text> measurement = measurements.next();
				
				List attributes = measurement.attributes();
				String host = (String) attributes.get(0);
				String type = (String) attributes.get(1);
				// long timestamp = Long.parseLong((String) attributes.get(2));
				float value = (Float) attributes.get(3);
				
				String name = host + " " + type;
				if (!m_map.containsKey(name)) {
					m_map.put(name, new ArrayList<Number>());
				}
				m_map.get(name).add(value);
			}
			
			Date date = new Date(System.currentTimeMillis());
			Collection<Measurement<Text>> aggregates = new ArrayList<Measurement<Text>>();
			for (String key : m_map.keySet()) {
				float avg = average(m_map.get(key));
				String[] keys = sep.split(key);
				aggregates.add(new SystemMeasurement(key, keys[0], keys[1], date, avg));
			}
			return aggregates;
		}
		
		public float average(Collection<Number> values) {
			if (values.size() == 0) return 0f;
			
			float sum = 0f;
			for (Number value : values) {
				sum += value.floatValue();
			}
			return sum / (float) values.size();
		}
	}
	
	
	private final String hostname;
	
	private Agent agent;
	
	private Aggregator aggregator;
	
	public SystemStats() {
		this.agent = new AgentImpl();
		this.aggregator = new AggregatorImpl();
		
		String name = "localhost:" + this;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			name = addr.getHostName();
		} catch (UnknownHostException e) { }
		this.hostname = name;
	}
	
	@Override
	public Measurement.Type type() {
		return Measurement.Type.SYSTEM;
	}
	
	@Override
	public Agent agent() {
		return this.agent;
	}

	@Override
	public Aggregator aggregator() {
		return this.aggregator;
	}
	
	
	public static void main(String[] args) throws Exception {
		SystemStats stats = new SystemStats();
		stats.agent().measure();
		System.err.println(stats.agent().toString());
	}


}
