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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by pippo on 14-9-19.
 */
public class OnlineTimeJob extends Configured implements Tool {

		private static final Path out = new Path("/subscriber_status_out");

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "onlinetime");
		job.setJarByClass(OnlineTimeJob.class);

		//mapp
		job.setMapperClass(StatusMapper.class);
		job.setMapOutputKeyClass(StatusKey.class);
		job.setMapOutputValueClass(OnlineRecord.class);

		//custom partition
		job.setPartitionerClass(StatusKeyPartitioner.class);

		//reduce
		job.setGroupingComparatorClass(StatusKeyGroupComparator.class);
		job.setReducerClass(StatusReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		//input
		FileInputFormat.setInputPaths(job, new Path("/subscriber_status/subscriber_status.json"));

		//output
		FileOutputFormat.setOutputPath(job, out);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		FileSystem.get(new Configuration()).delete(out, true);
		int code = ToolRunner.run(new Configuration(), new OnlineTimeJob(), args);
		System.exit(code);
	}
}
