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
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;

/**
 * Tasks interested in taking the output of a reduce task as
 * their input do so via this request type. 
 *
 */
public class ReduceBufferRequest extends BufferRequest {
	
	/* The reduce task identifier. */
	private TaskID reduceTaskId;
	
	/* Used to efficiently implement hashCode() */
	private String code;
	
	public ReduceBufferRequest() {
	}
	
	/**
	 * Constructor
	 * @param sourceHost The host name running the reduce task.
	 * @param mapTaskId The (pipeline) map task identifier.
	 * @param destinationAddress The (JBufferSink) address of the requesting map task.
	 * @param reduceTaskId The identifier of the reduce task whose output is being requested.
	 */
	public ReduceBufferRequest(String sourceHost, TaskAttemptID mapTaskId, 
			                   InetSocketAddress destinationAddress, BufferExchange.BufferType type,
			                   TaskID reduceTaskId) {
		super(sourceHost, mapTaskId, destinationAddress, type);
		this.reduceTaskId = reduceTaskId;
		
		this.code = sourceHost + ":" + mapTaskId + ":" + reduceTaskId;
	}
	
	@Override
	public int hashCode() {
		return code.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof ReduceBufferRequest) {
			return this.code.equals(((ReduceBufferRequest)o).code);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return destination() + " requesting buffer from reduce " + this.reduceTaskId.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.reduceTaskId = new TaskID();
		this.reduceTaskId.readFields(in);
		this.code = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		this.reduceTaskId.write(out);
		WritableUtils.writeString(out, this.code);
	}
	
	public TaskID reduceTaskId() {
		return this.reduceTaskId;
	}

	@Override
	public int partition() {
		return 0;
	}

}
