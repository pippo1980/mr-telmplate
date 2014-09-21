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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by pippo on 14-9-19.
 */
public class StatusReducer extends Reducer<StatusKey, OnlineRecord, Text, LongWritable> {

	private static final Text outKey = new Text();

	private static final LongWritable total = new LongWritable(0);

	@Override
	protected void reduce(StatusKey key, Iterable<OnlineRecord> values, Context context)
			throws IOException, InterruptedException {

		OnlineRecord lastOnline = new OnlineRecord();
		long _total = 0;
		for (OnlineRecord record : values) {

			if (lastOnline.onlineTime < 0 && record.onlineTime > 0) {
				lastOnline.onlineTime = record.onlineTime;
				continue;
			}

			if (lastOnline.onlineTime > 0 && record.offlineTime > 0) {
				record.onlineTime = lastOnline.onlineTime;

				record.cost = record.offlineTime - record.onlineTime;
				_total += record.cost;
				lastOnline.onlineTime = -1;
			}
		}

		outKey.set(key.userId);
		total.set(_total / 60 / 60);
		context.write(outKey, total);
	}
}
