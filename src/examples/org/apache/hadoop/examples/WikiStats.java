package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SimpleTimeZone;
import java.util.StringTokenizer;
import java.util.TimeZone;

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
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiStats extends Configured implements Tool {
	private static String[] daytable = {"", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
	private static String[] monthtable = {"January", "February", "March", "April", "May", "June", "July", 
		                                  "August", "September", "October", "November", "December"};
	
	private static enum KeyType{LANGHOUR, LANGDAY, LANGMONTH, LANG};
	private static enum ValType{CLICKS, SIZE};
	
	private static HashMap<String, String> languages = new HashMap<String, String>();
	
	static {
		languages.put("en", "english");
		languages.put("ja", "japanese");
		languages.put("de", "german");
		languages.put("es", "spanish");
		languages.put("fr", "french");
		languages.put("it", "italian");
		languages.put("pl", "polish");
		languages.put("ru", "russian");
		languages.put("pt", "portuguese");
		languages.put("nl", "dutch");
	}

	public static class LangMap extends MapReduceBase
	implements Mapper<Object, Text, Text, LongWritable> {

		private final static LongWritable value = new LongWritable(1);
		private final static Text lang = new Text();
		private KeyType keytype = KeyType.LANG;
		private ValType valtype = ValType.CLICKS;
		private String inputFile = null;
		private String date = null;
		private String time = null;
		private Calendar calendar;

		
		public LangMap() {
			// get the supported ids for GMT-08:00 (Pacific Standard Time)
			String[] ids = TimeZone.getAvailableIDs(-8 * 60 * 60 * 1000);
			// if no ids were returned, something is wrong. get out.
			if (ids.length == 0)
				System.exit(0);

			// create a Pacific Standard Time time zone
			SimpleTimeZone pdt = new SimpleTimeZone(-8 * 60 * 60 * 1000, ids[0]);

			// set up rules for daylight savings time
			pdt.setStartRule(Calendar.APRIL, 1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);
			pdt.setEndRule(Calendar.OCTOBER, -1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);

			// create a GregorianCalendar with the Pacific Daylight time zone
			// and the current date and time
			this.calendar = new GregorianCalendar(pdt);
		}

		@Override
		public void configure(JobConf conf) {
			super.configure(conf);
			this.keytype = KeyType.valueOf(conf.get("wikistats.keytype", KeyType.LANG.name()));
			this.valtype = ValType.valueOf(conf.get("wikistats.valtype", ValType.CLICKS.name()));
			inputFile = conf.get("map.input.file", null);
			System.out.println("Input file: " + inputFile);
			if (inputFile != null) {
				inputFile = inputFile.substring(inputFile.lastIndexOf("pagecounts"), 
						                        inputFile.length() - ".gz".length());
				date = inputFile.substring(inputFile.indexOf('-') + 1, inputFile.lastIndexOf('-'));
				time = inputFile.substring(inputFile.lastIndexOf('-') + 1, inputFile.length()); 
				time = time.substring(0, 2);
				System.out.println("\tDate " + date);
				System.out.println("\tTime " + time);
			}
		}

		public void map(Object key, Text value, 
				OutputCollector<Text, LongWritable> output, 
				Reporter reporter) throws IOException {
			parse(value.toString(), output);
		}

		private void parse(String line, OutputCollector<Text, LongWritable> output) {
			try {
				StringTokenizer itr = new StringTokenizer(line);
				String proj = itr.nextToken();
				String name = itr.nextToken();
				String clks = itr.nextToken();
				String size = itr.nextToken();
				if (proj.length() < 2) return;

				/* Set the language */
				String lang = proj.substring(0,2);
				if (!languages.containsKey(lang)) {
					return;
				}
				else {
					lang = languages.get(lang);
				}
				
				/* Set the hour? */
				if (keytype == KeyType.LANGHOUR) {
					lang = time + ":" + lang;
				}
				else if (keytype == KeyType.LANGDAY) {
					Integer year = Integer.parseInt(date.substring(0, 4));
					Integer day = Integer.parseInt(date.substring(4, 6));
					Integer month = Integer.parseInt(date.substring(6, 8));
					this.calendar.set(Calendar.YEAR, year);
					this.calendar.set(Calendar.DAY_OF_MONTH, day);
					this.calendar.set(Calendar.MONTH, month);
					
					lang = daytable[calendar.get(Calendar.DAY_OF_WEEK)] + ":" + lang;
				}
				else if (keytype == KeyType.LANGMONTH) {
					Integer year = Integer.parseInt(date.substring(0, 4));
					Integer day = Integer.parseInt(date.substring(4, 6));
					Integer month = Integer.parseInt(date.substring(6, 8));
					this.calendar.set(Calendar.YEAR, year);
					this.calendar.set(Calendar.DAY_OF_MONTH, day);
					this.calendar.set(Calendar.MONTH, month);
					
					lang = monthtable[calendar.get(Calendar.MONTH)] + ":" + lang;
				}
				
				this.lang.set(lang);

				long c = Long.parseLong(clks);
				long s = Long.parseLong(size);

				if (valtype == ValType.CLICKS) {
					this.value.set(c);
				}
				else if (valtype == ValType.SIZE) {
					this.value.set(s);
				}

				output.collect(this.lang, this.value);
			} catch (Throwable t) {
				t.printStackTrace();
				System.err.println("Bad line: " + line);
			}
		}
	}

	public static class LangReduce extends MapReduceBase
	implements Reducer<Text, LongWritable, Text, Text> {
		private final static Text value = new Text();

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			long sum = 0;
			long count = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				count++;
			}
			
			if (count > 0) {
				value.set(Long.toString(sum) + " " + Long.toString(sum /count) + " " + Long.toString(count));
				output.collect(key, value);
			}
		}
	}


	public static class GenericReduce extends MapReduceBase
	implements Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, 
				Reporter reporter) throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class DayReduce extends MapReduceBase
	implements Reducer<IntWritable, LongWritable, Text, LongWritable> {
		private Text day = new Text();

		public void reduce(IntWritable key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, 
				Reporter reporter) throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			String dayOfWeek = daytable[key.get()];
			this.day.set(dayOfWeek);
			output.collect(day, new LongWritable(sum));
		}
	}



	static int printUsage() {
		System.out.println("wikistats [-k <keytype>] [-v <valtype>] [-p <pipeline>] [-s <snapfreq>] [-m <maxsnapprogress>] [-r reduces] input output");
		System.out.println("keytypes = LANG, LANGHOUR, LANGDAY, LANGMONTH. valtypes = CLICKS, SIZE");
		System.out.println("languages = en english, de german, fr french, jp japanese");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WikiStats.class);
		conf.setJobName("wikistats");

		KeyType keytype = KeyType.LANG;
		ValType valtype = ValType.CLICKS;
		
		boolean average = false;

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-s".equals(args[i])) {
					conf.setFloat("mapred.snapshot.frequency", Float.parseFloat(args[++i]));
					conf.setBoolean("mapred.map.pipeline", true);
				} else if ("-m".equals(args[i])) {
					conf.setFloat("mapred.snapshot.max.progress", Float.parseFloat(args[++i]));
				} else if ("-p".equals(args[i])) {
					conf.setBoolean("mapred.map.pipeline", true);
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-a".equals(args[i])) {
					average = true;
				} else if ("-l".equals(args[i])) {
					conf.set("wikistats.lang", args[++i]);
				} else if ("-k".equals(args[i])) {
					keytype = KeyType.valueOf(args[++i]);
					conf.set("wikistats.keytype", keytype.name());
				} else if ("-v".equals(args[i])) {
					valtype = ValType.valueOf(args[++i]);
					conf.set("wikistats.valtype", valtype.name());
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

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
			
		if (conf.getFloat("mapred.snapshot.frequency", 1f) < 1f) {
			conf.setBoolean("mapred.map.pipeline", false);
		}
			
		conf.setMapperClass(LangMap.class);        
		conf.setReducerClass(LangReduce.class);
		conf.setClass("mapred.map.combiner.class", GenericReduce.class, GenericReduce.class);
		
		
		JobClient.runJob(conf);
		return 0;
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikiStats(), args);
		System.exit(res);
	}

}
