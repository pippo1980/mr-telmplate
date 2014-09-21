/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.sirius.hadoop.job.onlinetime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Arrays;

/**
 * Created by pippo on 14-9-19.
 */
public class InputUpload {

	static {
		System.setProperty("HADOOP_HOME", InputUpload.class.getResource("/").getFile());
		System.setProperty("HADOOP_USER_NAME", "hdfs");
	}

	public static final Path input_path = new Path("/subscriber_status");

	private static URI hdfs = null;
	private static Configuration configuration = new Configuration();

	public static void main(String[] args) throws Exception {
		hdfs = new URI("hdfs://hadoop1:8020");
		FileSystem fs = FileSystem.get(hdfs, configuration);

		if (fs.exists(input_path)) {
			fs.delete(input_path, true);
		}

		System.out.println(Arrays.toString(fs.listStatus(new Path("/"))));

		fs.copyFromLocalFile(false,
				true,
				new Path("/Users/pippo/Downloads/subscriber_status.statics.input"),
				input_path);
	}
}
