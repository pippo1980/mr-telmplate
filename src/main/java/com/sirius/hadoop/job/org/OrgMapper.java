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

package com.sirius.hadoop.job.org;

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
public class OrgMapper extends Mapper<Object, Text, Text, OrgInfo> {

	private static final Log log = LogFactory.getLog(OrgMapper.class);

	private static final ObjectMapper mapper = new ObjectMapper();

	private static final Text out_key = new Text();

	private static final OrgInfo out = new OrgInfo();

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

		String type = (String) m.get("_class");
		switch (type) {
			case "com.myctu.platform.repository.domain.model.organization.OrgTreeNode":
				tree(m, context);
				break;
			case "com.myctu.platform.repository.domain.model.user.UserOrg":
				user_org(m, context);
				break;
		}
	}

	private void tree(Map<String, Object> value, Context context) {
//		String id = value.get("_id");
//		String name = value.get("name");
//		String[] ancestors = (String[]) value.get("ancestors");
	}

	private void user_org(Map<String, Object> value, Context context) {

	}

}
