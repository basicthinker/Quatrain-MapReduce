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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.TaskAttemptID;

/**
 * The class that all requests must extend. 
 * 
 * A request can be for either a Map or a Reduce output buffer.
 * The request is used to communicate the destination of the task
 * interested in the buffer and what partition it is interested in
 * receiving. 
 *
 */
public abstract class BufferRequest implements Writable {
	/** The (task) type of output buffer we are requesting. */
	private static enum Type{MAP, REDUCE};

	/** On what host does the buffer reside. */
	protected String srcHost;

	/** The identifier of the task making the request. */
	protected TaskAttemptID destTaskId;

	/** The IP address of the task making the request. */
	protected InetSocketAddress destAddress;
	
	protected BufferExchange.BufferType bufferType;

	public BufferRequest() {
	}

	public BufferRequest(String sourceHost, TaskAttemptID destTaskId, 
			InetSocketAddress destinationAddress, BufferExchange.BufferType bufferType) {
		this.srcHost = sourceHost;
		this.destTaskId = destTaskId;
		this.destAddress = destinationAddress;
		this.bufferType = bufferType;
	}


	public static BufferRequest read(DataInput in) throws IOException {
		Type type = WritableUtils.readEnum(in, Type.class);
		BufferRequest request = null;
		if (type == Type.MAP) {
			request = new MapBufferRequest();
		}
		else {
			request = new ReduceBufferRequest();
		}
		request.readFields(in);
		return request;
	}

	public static void write(DataOutput out, BufferRequest request) throws IOException {
		Type type = request instanceof MapBufferRequest ? Type.MAP : Type.REDUCE;
		WritableUtils.writeEnum(out, type);
		request.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		/* The hostname on which the source executes */
		this.srcHost = WritableUtils.readString(in);

		/* The destination task id. */
		this.destTaskId = new TaskAttemptID();
		this.destTaskId.readFields(in);

		/* The address on which the destination executes */
		String host = WritableUtils.readString(in);
		int    port = WritableUtils.readVInt(in);
		this.destAddress = new InetSocketAddress(host, port);
		
		this.bufferType = WritableUtils.readEnum(in, BufferExchange.BufferType.class);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		/* The hostname on which the source executes */
		WritableUtils.writeString(out, this.srcHost);

		/* The destination task id. */
		this.destTaskId.write(out);

		/* The address on which the destination executes */
		WritableUtils.writeString(out, destAddress.getHostName());
		WritableUtils.writeVInt(out, destAddress.getPort());
		
		WritableUtils.writeEnum(out, this.bufferType);
	}

	public abstract int partition();

	public BufferExchange.BufferType bufferType() {
		return this.bufferType;
	}
	
	public TaskAttemptID destination() {
		return this.destTaskId;
	}

	public InetSocketAddress destAddress() {
		return this.destAddress;
	}

	public String srcHost() {
		return this.srcHost;
	}
}
