package org.apache.hadoop.mapred.buffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.net.NetUtils;

import org.stanzax.quatrain.sample.SampleServer;
import org.stanzax.quatrain.server.MrServer;

public class QuatrainManager extends MrServer {

	private FileSystem rfs = null;

	public QuatrainManager(String address, int port, int handlerCount,
			Configuration conf, FileSystem rfs) throws IOException {
		super(address, port, handlerCount, conf);
		// TODO Auto-generated constructor stub
		this.rfs = rfs;
	}

	enum STATUS {
		UPDATED, // 有新文件
		STALL, // 文件已经被传输完
		KILL, // 被杀
		SUCCESS
		// 成功结束
	}

	private TaskTracker tracker = null;
	/*
	 * 记录每个task的中间结果文件
	 */
	private ConcurrentHashMap<TaskAttemptID, ArrayList<OutputFileWritable>> files = new ConcurrentHashMap<TaskAttemptID, ArrayList<OutputFileWritable>>();
	// 记录在该节点上跑的所有map
	private ConcurrentHashMap<JobID, Set<TaskAttemptID>> allTasks = new ConcurrentHashMap<JobID, Set<TaskAttemptID>>();
	/*
	 * 记录所有
	 */
	private ConcurrentHashMap<TaskAttemptID, STATUS> taskStatus = new ConcurrentHashMap<TaskAttemptID, STATUS>();
	// 记录所有节点上job对应的

	private Map<String, Set<String>> servedIds = new ConcurrentHashMap<String, Set<String>>();

	private static final Log LOG = LogFactory.getLog(QuatrainManager.class
			.getName());

	public static InetSocketAddress getServerAddress(Configuration conf) {
		try {
			String address = InetAddress.getLocalHost().getCanonicalHostName();
			int port = conf.getInt("mapred.buffer.QuatrainManager.port", 3122);
			address += ":" + port;
			return NetUtils.createSocketAddr(address);
		} catch (Throwable t) {
			return NetUtils.createSocketAddr("127.0.0.1:3122");
		}
	}

	public synchronized void output(TaskAttemptID mapID, OutputFile outputFile) {

		synchronized(taskStatus){
			this.taskStatus.put(mapID, STATUS.UPDATED);
		}
		
		// OutputFileWritable file = new OutputFileWritable(ouputFile);
		JobID job = mapID.getJobID();
		Set<TaskAttemptID> maps = new HashSet<TaskAttemptID>();
		this.allTasks.putIfAbsent(job, maps);
		
		// this.files.putIfAbsent(mapID, new
		// ArrayList<OutputFileWritable>());
		maps = this.allTasks.get(job);
		maps.add(mapID);
		synchronized (files) {
			if (!this.files.containsKey(mapID)) {
				this.files.put(mapID, new ArrayList<OutputFileWritable>());
			}
		
			ArrayList<OutputFileWritable> tmFils = this.files.get(mapID);
		// outputFile.header().
			OutputFileWritable outfile = new FileWritable(outputFile, rfs, null);
			tmFils.add(outfile);
		}
		preturn(new DoubleWritable(0));

	}

	public void requestFile(TaskAttemptID redcueID, TaskAttemptID mapID,
			IntWritable partition) {
		while (true) {
			ArrayList<OutputFileWritable> src = new ArrayList<OutputFileWritable>();
			synchronized (files) {
				src=files.get(mapID);
			}
			if(src==null) continue;
			int size = src.size();
			for (int i = 0; i < size; i++) {
				OutputFileWritable outPutfile = src.get(i);
				OutputFile file = outPutfile.file;
				synchronized (file) {
					if (!file.isServiced(redcueID)) {
						outPutfile.setPartition(partition.get());
						preturn(outPutfile);
						file.serviced(redcueID);
					}
				}
			}
			synchronized (taskStatus) {
				STATUS s = this.taskStatus.get(mapID);
				if (s != null
						&& (s.equals(STATUS.KILL) || s.equals(STATUS.SUCCESS))) {
					this.taskStatus.remove(mapID);
					break;
				}
			}
		}

		return;
	}

	public void finish(TaskAttemptID taskID) {
		this.taskStatus.put(taskID, STATUS.SUCCESS);
	}

	public void killJob(JobID jid) {
		Set<TaskAttemptID> tasks = this.allTasks.get(jid);
		if (tasks == null)
			return;
		Iterator it = tasks.iterator();
		while (it.hasNext()) {
			TaskAttemptID tid = (TaskAttemptID) it.next();
			this.taskStatus.put(tid, STATUS.KILL);
			this.files.remove(tid);
		}
		synchronized (this) {
			this.allTasks.remove(jid);
		}

	}

	public void close() {
		this.stop();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// Set log options, combination of NONE, ACTION and STATE
			SampleServer server = new SampleServer("localhost", 3122, 10, null);
			server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
