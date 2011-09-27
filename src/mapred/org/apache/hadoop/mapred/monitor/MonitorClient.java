package org.apache.hadoop.mapred.monitor;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.monitor.Measurement;
import org.apache.hadoop.mapred.monitor.impl.SystemMeasurement;
import org.apache.hadoop.mapred.monitor.impl.SystemStats;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class MonitorClient<V> extends Configured implements Tool,
		Mapper<LongWritable, LongWritable, Text, Measurement<V>>,
		Reducer<Text, Measurement<V>, Text, Measurement<V>> {


	private transient InetSocketAddress jolAddress = null;
	private transient DataOutputStream ostream = null;

	private final static MonitorClient factory(Measurement.Type t) {
		if (t == Measurement.Type.SYSTEM) {
			return new SystemStats();
		}
		return null;
	}
	
	public abstract Measurement.Type type();
	
	public abstract Agent<V, Measurement<V>> agent();
	
	public abstract Aggregator<V, Measurement<V>> aggregator();
	
	private boolean connect() throws IOException {
		if (this.jolAddress != null && this.ostream == null) {
			System.err.println("Monitor client connect to server at " + this.jolAddress);
			Socket socket = new Socket();
			socket.connect(this.jolAddress);
			this.ostream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			WritableUtils.writeEnum(this.ostream, type());
			this.ostream.flush();
			if (socket.isConnected()) System.err.println("Monitor client connected to server.");
			return socket.isConnected();
		}
		return this.ostream != null;
	}
	
	private boolean send(Measurement measurement) {
		if (this.ostream != null) {
			try {
				System.err.println("Send measurement: " + measurement.value());
				this.ostream.writeBoolean(true);
				measurement.write(this.ostream);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
				try {
					close();
				} catch (IOException e1) {
					ostream = null;
				}
			}
		}
		return false;
	}
	
	@Override
	public void configure(JobConf job) {
		String address = job.get("mapred.monitor.jol.address", null);
		if (address != null) {
			this.jolAddress = NetUtils.createSocketAddr(address);
		}
	}

	@Override
	public void close() throws IOException {
		if (ostream != null) {
			System.err.println("Closing stream.");
			ostream.writeBoolean(false);
			ostream.flush();
			ostream.close();
			ostream = null;
		}
	}
	
	@Override
	public final void map(LongWritable key, LongWritable value,
			OutputCollector<Text, Measurement<V>> output, Reporter reporter)
	throws IOException {
		List<Measurement<V>> measurements = agent().measure();
		for (Measurement<V> measurement : measurements) {
			output.collect(measurement.key(), measurement);
			reporter.progress();
		}
	}

	@Override
	public final void reduce(Text key, Iterator<Measurement<V>> values,
			OutputCollector<Text, Measurement<V>> output, Reporter reporter)
	throws IOException {
		boolean connected = connect();
		
		Collection<Measurement<V>> measurements = aggregator().aggregate(values);
		for (Measurement<V> measurement : measurements) {
			output.collect(measurement.key(), measurement);
			if (connected) connected = send(measurement);
			reporter.progress();
		}
	}

	static int printUsage() {
		System.out.println("monitor SYSTEM|LOG [-w <window>] [-m <maps>] [-r <reduces>] <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MonitorClient.class);
		conf.setJobName("monitor");

		conf.setBoolean("mapred.job.monitor", true);
		conf.setBoolean("mapred.map.pipeline", false);
		conf.setInt("mapred.reduce.window", 1000); // default 1 second.
		
		conf.setMapRunnerClass(MapMonitorRunner.class);
		conf.setInputFormat(ClockInputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-w".equals(args[i])) {
					conf.setInt("mapred.reduce.window", Integer.parseInt(args[++i]));
				} else if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		Measurement.Type type = Measurement.Type.valueOf(other_args.get(0));
		if (type == Measurement.Type.SYSTEM) {
			conf.setMapperClass(SystemStats.class);
			conf.setReducerClass(SystemStats.class);
			
			// the keys are words (strings)
			conf.setOutputKeyClass(Text.class);
			// the values are counts (ints)
			conf.setOutputValueClass(SystemMeasurement.class);
		}
		else {
			System.out.print("Unknown monitor: " + other_args.get(0));
			System.exit(0);
		}
		
		
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}


	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			printUsage();
		}
		
		Measurement.Type type = Measurement.Type.valueOf(args[0]);
		MonitorClient monitor = MonitorClient.factory(type);
		if (args.length == 1) {
			monitor.agent().measure();
			System.err.println(monitor.agent().toString());
			System.exit(0);
		}
		
		
		int res = ToolRunner.run(new Configuration(), MonitorClient.factory(type), args);
		System.exit(res);
	}
	
}
