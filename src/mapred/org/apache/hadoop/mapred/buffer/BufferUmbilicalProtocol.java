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

package org.apache.hadoop.mapred.buffer;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;

/**
 * The RPC interface used by tasks to communicate with the BufferController.
 * 
 * @author tcondie
 */
public interface BufferUmbilicalProtocol extends VersionedProtocol {
	long versionID = 0;

	/**
	 * Used to make new requests for map/reduce buffers.
	 * @param request The buffer request.
	 * @throws IOException
	 */
	void request(BufferRequest request) throws IOException;

	/**
	 * Statistic on how well pipelining is keeping up with the production
	 * of a task's output.
	 * @param owner The task producing output.
	 * @return Stall fraction
	 * @throws IOException
	 */
	float stallFraction(TaskAttemptID owner) throws IOException;

	/**
	 * Register a new output file (e.g., snapshot, spill file, or final output)
	 * with the BufferController.
	 * @param buffer The output file.
	 * @throws IOException
	 */
	void output(OutputFile buffer) throws IOException;
}
