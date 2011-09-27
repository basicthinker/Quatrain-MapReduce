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

package org.apache.hadoop.mapred.buffer.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;

/**
 * Tasks that take the output of a map task as their input 
 * will request that output via this class. Naturally, reduce
 * tasks use this request form. Since they are interested
 * in their partition from all map task, we only need the
 * job identifier of those map tasks and the partition number
 * assigned to the reduce task.
 *
 */
public class MapBufferRequest extends BufferRequest {

	/* The job identifier of the map task(s). */
	private JobID mapJobId;

	/* The partition that we're requesting. */
	private int mapPartition;

	/* Used to implement hash code. */
	private String code;

	public MapBufferRequest() {
	}

	/**
	 * Constructor.
	 * @param sourceHost The host name that is running the map task.
	 * @param destTaskId The task attempt identifier making the request.
	 * @param destinationAddress The (JBufferSink) address of the requesting task.
	 * @param mapJobId The job identifier of the map task(s).
	 * @param mapPartition The map partition being requested. 
	 */
	public MapBufferRequest(String sourceHost, TaskAttemptID destTaskId, 
			InetSocketAddress destinationAddress, BufferExchange.BufferType type, JobID mapJobId, int mapPartition) {
		super(sourceHost, destTaskId, destinationAddress, type);
		this.mapJobId = mapJobId;
		this.mapPartition = mapPartition;
		this.code = sourceHost + ":" + destTaskId + ":" + mapJobId + ":" + mapPartition;
	}

	@Override
	public int hashCode() {
		return code.hashCode();
	}

	public boolean equals(Object o) {
		if (o instanceof MapBufferRequest) {
			return this.code.equals(((MapBufferRequest)o).code);
		}
		return false;
	}

	@Override
	public String toString() {
		return destination() + " requesting map buffers from job " + mapJobId.toString() + " partition " + mapPartition;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.mapJobId = new JobID();
		this.mapJobId.readFields(in);
		this.mapPartition = in.readInt();
		this.code = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		this.mapJobId.write(out);
		out.writeInt(this.mapPartition);
		WritableUtils.writeString(out, this.code);
	}

	public JobID mapJobId() {
		return this.mapJobId;
	}

	public int partition() {
		return this.mapPartition;
	}
}
