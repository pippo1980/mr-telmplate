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

package com.sirius.hadoop.test.onlinetime;

import com.sirius.hadoop.job.onlinetime.OnlineRecord;
import com.sirius.hadoop.job.onlinetime.StatusKey;
import com.sirius.hadoop.job.onlinetime.StatusKeyGroupComparator;
import com.sirius.hadoop.job.onlinetime.StatusMapper;
import com.sirius.hadoop.job.onlinetime.StatusReducer;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Created by pippo on 14-9-19.
 */
public class MRTest {

	private MapReduceDriver<Object, Text, StatusKey, OnlineRecord, Text, LongWritable> onlineTimeMRDriver;

	@Before
	public void init() {
		onlineTimeMRDriver = MapReduceDriver.newMapReduceDriver(new StatusMapper(), new StatusReducer());
		onlineTimeMRDriver.setKeyGroupingComparator(new StatusKeyGroupComparator());
		String[] userIds = new String[10];
		for (int i = 0; i < userIds.length; i++) {
			userIds[i] = UUID.randomUUID().toString();
		}

		Set<String> values = new TreeSet<>();

		for (String userId : userIds) {

			long start = System.currentTimeMillis();

			for (int i = 0; i < 1000; i++) {
				start += RandomUtils.nextLong(1000, 1000 * 60 * 5);
				values.add(String.format("{'u':'%s','t':'%s','ct':%s}", userId, "online", start));

				start += RandomUtils.nextLong(1000, 1000 * 60 * 5);
				values.add(String.format("{'u':'%s','t':'%s','ct':%s}", userId, "offline", start));
			}

		}

		for (String value : values) {
			onlineTimeMRDriver.addInput(new Text(UUID.randomUUID().toString()), new Text(value));
		}
	}

	@Test
	public void test() throws IOException {
		List<Pair<Text, LongWritable>> results = onlineTimeMRDriver.run();

		for (Pair<Text, LongWritable> result : results) {
			System.out.println(result.toString());
		}
	}

}
