/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CQ extends Configured implements Tool {
	public static enum SystemStatEntry {
		USER(0, "normal processes executing in user mode"),
		NICE(1, "niced processes executing in user mode"),
		SYSTEM(2, "processes executing in kernel mode"),
		IDLE(3, "twiddling thumbs"),
		IOWAIT(4, "waiting for I/O to complete"),
		IRQ(5, "servicing interrupts"),
		SOFTIRQ(6, "softirq: servicing softirqs"),
		//STEAL(7, "involuntary wait (usually host virtual machine) time"),
		GUEST(7, "guest virtual machine time"),
		PAGEIN(8, "pagein"),
		PAGEOUT(9, "pageout"),
		SWAPIN(10, "swaps in"),
		SWAPOUT(11, "swaps out"),
		NET(12, "kbs/s in and out");
		/*
                        LOAD_1(9, "1 minute load average") ,
                                                               LOAD_5(10, "5 minute load average"),
                                                               LOAD_15(11, "15 minute load average");
		 */
		public final int offset;
		public final String name;
		private SystemStatEntry(int o, String n) {
			this.offset = o; this.name = n;
		}
	}

	public static class SystemStats {
		public int getInt(SystemStatEntry e) {
			return cols[e.offset].intValue();
		}
		public String getStrByOffset(int o) {
			return cols[o].toString();
		}

		public float getFloat(SystemStatEntry e) {
			return cols[e.offset].floatValue();
		}

		final Pattern whiteSpace = Pattern.compile("\\s+");
		Number cols[];
		public SystemStats() throws IOException {
			File f = new File("/proc/stat");
			BufferedReader in = new BufferedReader(new FileReader(f));
			String info = in.readLine();
			if(!info.startsWith("cpu  ")) { throw new IllegalStateException("Can't parse /proc/stat!"); }
			info = info.substring(5);
			String[] tok = whiteSpace.split(info);
			cols = new Number[tok.length + 5];
			for(int i = 0; i < tok.length; i++) {
				try {
					cols[i] = Long.parseLong(tok[i]);
				} catch(Exception e) {
					// deal w/ index out of bound, number format, etc by ignoring them
				}
			}
			in.close();
			/*
                        f = new File("/proc/loadavg");
                        in = new BufferedReader(new FileReader(f));
                        info = in.readLine();
                        tok = whiteSpace.split(info);
                        cols[cols.length-3] = Double.parseDouble(tok[0]);
                        cols[cols.length-2] = Double.parseDouble(tok[1]);
                        cols[cols.length-1] = Double.parseDouble(tok[2]);
			 */
			f = new File("/proc/vmstat");
			in = new BufferedReader(new FileReader(f));
			int vmstats = 0;
			while ((info = in.readLine()) != null) {
				tok = whiteSpace.split(info);
				if (tok[0].equals("pgpgin")) {
					cols[cols.length-5] = Long.parseLong(tok[1]);
					vmstats++;
				} else if (tok[0].equals("pgpgout")) {
					cols[cols.length-4] = Long.parseLong(tok[1]);
					vmstats++;
				} else if (tok[0].equals("pswpin")) {
					cols[cols.length-3] = Long.parseLong(tok[1]);
					vmstats++;
				} else if (tok[0].equals("pswpout")) {
					cols[cols.length-2] = Long.parseLong(tok[1]);
					vmstats++;
				}
			}
			if (vmstats != 4) {
				throw new RuntimeException("missing readings from proc");
			}

			in.close();
			Process p = Runtime.getRuntime().exec("/usr/bin/ifstat 1/1 1");

			BufferedReader stdInput = new BufferedReader(new
					InputStreamReader(p.getInputStream()));

			stdInput.readLine();
			stdInput.readLine();
			String s = stdInput.readLine();
			System.err.println("INPUT " + s);
			tok = whiteSpace.split(s);
			//System.err.println("tok0: *" + tok[1] +"*, tok1 *" + tok[2] +"*");
			int i = 0;
			if (tok[0].equals("")) {
				i = 1;
			}
			double kbs = Double.parseDouble(tok[i]) + Double.parseDouble(tok[i+1]);
			System.err.println("NEt: " + kbs);
			cols[cols.length-1] = kbs;
			stdInput.close();

		}
		boolean verbose = true;
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("<" + super.toString() + ": ");
			for(SystemStatEntry e: SystemStatEntry.values()) {
				System.err.println("offset: "+ e.offset);
				System.err.println("val: "+cols[e.offset]);
				sb.append("\n\t(" + e + ", " + cols[e.offset] + ( verbose ? ", " + e.name  : "" )+ ")");
			}
			sb.append(">");
			return sb.toString();
		}

		public int totalJiffies() {
			int sum = 0;
			for (SystemStatEntry e: SystemStatEntry.values()) {
				if (e.offset < SystemStatEntry.PAGEIN.offset) {
					sum += cols[e.offset].intValue();
				}
			}
			return sum;
		}

	}


	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		public void sleep(int s) {
			try {
				Thread.sleep(s);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		public void blockForce(OutputCollector o) {
			JOutputBuffer jb = (JOutputBuffer) o;
			try {
				jb.force();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void configure(JobConf jc) {
			try {
				java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
				String hn = localMachine.getHostName();
				System.out.println("CQ mapper started on " + hn);
			} catch (java.net.UnknownHostException e) {
				throw new RuntimeException(e);
			}
		}
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
			String hn = localMachine.getHostName();

			float su = 0;
			float ss = 0;
			int st = 0;
			int in = 0;
			int out = 0;
			int pin = 0;
			int pout = 0;
			int iow = 0;
			while (true) {
				SystemStats stat = new SystemStats();
				word.set(hn);
				// I was having a lot of trouble with ArrayWritable...

				// calc load here.
				float u = stat.getFloat(SystemStatEntry.USER);
				float s = stat.getFloat(SystemStatEntry.SYSTEM);
				int j = stat.totalJiffies();
				float l = ((u-su) + (s-ss)) / (float)(j-st);

				int swappedin = stat.getInt(SystemStatEntry.SWAPIN);
				int swappedout = stat.getInt(SystemStatEntry.SWAPOUT);

				int pagein = stat.getInt(SystemStatEntry.PAGEIN);
				int pageout = stat.getInt(SystemStatEntry.PAGEOUT);

				int iowaits = stat.getInt(SystemStatEntry.IOWAIT);

				int sir = (in == 0) ? 0 : swappedin - in;
				int sor = (out == 0) ? 0 : swappedout - out;

				System.err.println("swap info: " + (swappedin - in) + " in, "+(swappedout-out) + " out");

				Text v = new Text(((u-su) % 100) + "," + ((s-ss)%100) + "," + (j-st) + "," + (pagein-pin) + "," + (pageout-pout) + "," + (sir)  + "," + (sor) + "," + stat.getFloat(SystemStatEntry.NET) + "," + (iowaits - iow) + "," + System.currentTimeMillis());
				output.collect(word, v);
				blockForce(output);
				System.err.println("M load: " + l );

				System.err.println("M readings: " + (u-su) + "," + (s-ss) + "," + (j-st));
				System.err.println("ie, "+u+","+su+" : "+j+","+st);
				su = u; ss = s; st = j; in = swappedin; out = swappedout; pin = pagein; pout = pageout; iow = iowaits;
				reporter.progress();
				sleep(4000);
			}


		}
	}

	public static class HostState {
		public double user;
		public double system;
		public double jiffies;
		public int swaps;
		public int pages;
		public double net;
		public long tstamp;
		public int iow;

		public float CPUW = 100;
		public float SW = 1;

		public float PW = 0;

		public HostState() {
			user = system = jiffies = swaps = pages = 0;
		}
		public HostState(double u, double s, double t, int pa, int sw, double n, int io, long ts) {
			user = u;
			system = s;
			jiffies = t;
			swaps = sw;
			pages = pa;
			tstamp = ts;
			net = n;
			iow = io;
		}
		public String toString() {
			return "@" + tstamp + ", user=" + user + ", system=" + system + ", total="+jiffies;
		}
		public int linearCombo() {
			double load = (user + system) / jiffies;
			return (int)(CPUW * load + SW * swaps + PW * pages);
		}
		public int cpuReading() {
			return (int) (((user + system) / jiffies) * 100);
		}
		public int ioWaitReading() {
			return (int) ((iow / jiffies) * 100);
		}
	}

	public static class SummaryStats {
		long sum;
		long sumsq;
		int cnt;

		public SummaryStats() {
			sum = sumsq = cnt = 0;
		}
		void reading(int d) {
			//System.err.println("READING: " + d);
			sum += d;
			sumsq += d * d;
			cnt++;
		}
		void readingPerc(double d) {
			int i = (int)(d * 100);
			reading(i);
		}
		double avg() {
			if (cnt != 0) {
				System.err.println("dividing " + sum + " by "+ cnt);
				return sum / cnt;
			} else {
				//throw new RuntimeException("divide by zero, fool");
				System.err.println("divide by zero. " + sum + " / " + cnt);
				return 0;
			}
		}

		double stdev() {
			if (cnt != 0) {
				double avg = this.avg();
				double avgsquare = avg * avg;
				double squareavg = sumsq / cnt;
				//System.err.println("squareavg: " + squareavg + ", sumsq: " + sumsq + ", cnt: "+ cnt + ", sum: " + sum);
				double variance = (squareavg - avgsquare);
				//System.err.println("VARIANCE: " + variance);

				return Math.sqrt(variance);
			} else {
				//throw new RuntimeException("divide by zero, fool");
				return 0;
			}
		}

	}

	public static class CQState {
		HashMap<String, ArrayList<HostState>>  m;
		public CQState() {
			//System.err.println("constructor for CQState called!\n");
			m = new HashMap<String, ArrayList<HostState>>();
		}
		public void add(String host, HostState upd) {
			ArrayList<HostState> hlist = m.get(host);
			if (hlist == null) {
				hlist = new ArrayList<HostState>();
				m.put(host, hlist);
			}
			hlist.add(upd);
		}
		public double hostAvg(String host, int interval) {
			// very expensive prototype implementation....
			SummaryStats ssd = new SummaryStats();
			ArrayList<HostState> newList = new ArrayList<HostState>();
			long now = System.currentTimeMillis();
			ArrayList<HostState> ha = m.get(host);
			for (HostState h : ha) {
				if (now - h.tstamp < (1000 * interval)) {
					newList.add(h);
					//ssd.readingPerc((h.user + h.system) / h.jiffies);
					///ssd.reading(h.linearCombo());
					ssd.reading(h.cpuReading());
					//ssd.reading((int)h.ioWaitReading());
					//cnt++;
				}
			}
			//shouldn't be clobbering here...
			//m.put(host, newList);
			return ssd.avg();
		}


		public SummaryStats notHostAvg(String host, int interval) {
			long now = System.currentTimeMillis();
			SummaryStats ssd = new SummaryStats();
			for (String h : m.keySet()) {
				if (h.equals(host)) {
					continue;
				}
				ArrayList<HostState> newList = new ArrayList<HostState>();
				ArrayList<HostState> ha = m.get(h);
				for (HostState hs : ha) {
					if (now - hs.tstamp < (1000 * interval)) {
						newList.add(hs);
						ssd.reading(hs.linearCombo());
					}
				}
				m.put(h, newList);
			}
			return ssd;
		}


		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (String k : m.keySet()) {
				sb.append("key: " +  k + "\n");
				ArrayList<HostState> ah = m.get(k);
				for (HostState s : ah) {
					sb.append("\t" + s.toString() + "\n");
				}
			}
			return sb.toString();
		}

	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, IntWritable> {

		static float suser = 0;
		static float ssystem = 0;
		static float stotal = 0;
		static CQState cqs = new CQState();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			System.err.println("REDUCER: " + (int)(System.currentTimeMillis() / 1000));
			while (values.hasNext()) {
				Text v = values.next();
				System.err.println("OK, got output: " + v.toString());
				String[] items = v.toString().split(",");

				double user = Double.parseDouble(items[0]);
				double system = Double.parseDouble(items[1]);
				double total = Double.parseDouble(items[2]);

				int pages = Integer.parseInt(items[3]) + Integer.parseInt(items[4]);

				int swaps = Integer.parseInt(items[5]) + Integer.parseInt(items[6]);
				double netkbs = Double.parseDouble(items[7]);
				int iowait = Integer.parseInt(items[8]);
				long maptime = Long.parseLong(items[9]);
				double load = ((user - suser) + (system - ssystem)) / (total - stotal);

				// it might be a mistake to poll time here.  but at least we don't have
				// to worry about multiple clocks...
				long time = System.currentTimeMillis();

				//System.err.println("user state change: " + suser + " --> " + user);
				HostState hs = new HostState(user, system, total, pages, swaps, netkbs, iowait, time);
				cqs.add(key.toString(), hs);


				double avg = cqs.hostAvg(key.toString(), 5);
				System.out.println(System.currentTimeMillis() + "\t" + maptime + "\t" + key.toString() + "\t" + avg);

			}

			/*
                                System.err.println("Host "+ key.toString());
                                double avg = cqs.hostAvg(key.toString(), 5);
                                //double globalAvg = cqs.notHostAvg(key.toString(), 120);
                                SummaryStats globalStats = cqs.notHostAvg(key.toString(), 120);
                                System.err.println("20 second moving avg: " + avg);
                                System.err.println("global 120 second moving avg: " + globalStats.avg() + ", stdev " + globalStats.stdev());
                                boolean alert = false;
                                if (avg > (globalStats.avg() + 2 * globalStats.stdev())) {
                                  if (avg > 10) {
                                    System.out.println(System.currentTimeMillis() + " " + key.toString() + "ALERT!!!\n");
                                    System.err.println(key.toString() + "ALERT!!!\n");
                                    alert = true;
                                  }
                                }
                                System.err.println("--------------------------\n");
			 */
			//System.out.println(System.currentTimeMillis() + "\t" + key.toString() + "\t" + avg + "\t" + globalStats.avg() + "\t" + globalStats.stdev() + "\t" + alert);
		}
	}

	static int printUsage() {
		System.out.println("cq [-w <window>] [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CQ.class);
		conf.setJobName("cq");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		/* DO NOT USE A COMBINER
                   conf.setCombinerClass(Reduce.class);
		 */
		conf.setReducerClass(Reduce.class);

		conf.setNumReduceTasks(1);
		//conf.setNumMapTasks(4);

		conf.setBoolean("mapred.job.monitor", true);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-w".equals(args[i])) {
					conf.setInt("mapred.reduce.window", Integer.parseInt(args[++i]));
					conf.setBoolean("mapred.map.pipeline", false);
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
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		conf.setBoolean("mapred.job.monitor", true);
		JobClient.runJob(conf);
		return 0;
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CQ(), args);
		System.exit(res);
	}
}
