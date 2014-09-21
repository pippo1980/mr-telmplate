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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by pippo on 14-9-19.
 */
public class StatusMapper extends Mapper<Object, Text, StatusKey, OnlineRecord> {

	private static final Log log = LogFactory.getLog(StatusMapper.class);

	private static final ObjectMapper mapper = new ObjectMapper();

	private static final StatusKey statusKey = new StatusKey();

	private static final OnlineRecord record = new OnlineRecord();

	static {
		mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		log.info(value.toString());

		Map<String, Object> m;
		try {
			m = mapper.readValue(value.toString(), Map.class);
		} catch (IOException e) {
			throw new RuntimeException(value.toString(), e);
		}

		statusKey.userId = (String) ObjectUtils.defaultIfNull(m.get("u"), StringUtils.EMPTY);
		statusKey.time = (Long) ObjectUtils.defaultIfNull(m.get("ct"), -1L);

		if ("online".equals(m.get("t"))) {
			record.onlineTime = statusKey.time;
			record.offlineTime = -1;
		} else {
			record.onlineTime = -1;
			record.offlineTime = statusKey.time;
		}

		context.write(statusKey, record);
	}

}
