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

package com.sirius.hadoop.job;

import com.sirius.hadoop.job.onlinetime.OnlineTimeJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Created by pippo on 14-9-19.
 */
public class JobDriver {

	private static final Log LOG = LogFactory.getLog(JobDriver.class);

	private static final ProgramDriver PROGRAM_DRIVER = new ProgramDriver();

	public static void regist(String name, Class<?> job, String description) {
		try {
			PROGRAM_DRIVER.addClass(name, job, description);
		} catch (Throwable throwable) {
			LOG.error(String.format("regist job:[%s] due to error", job), throwable);
		}
	}

	static {
		JobDriver.regist("onlinetime", OnlineTimeJob.class, "onlinetime");
	}

	public static void main(String[] args) throws Throwable {
		int code = -1;

		try {
			code = PROGRAM_DRIVER.run(args);
		} catch (Throwable throwable) {
			LOG.error("run due to error", throwable);
		}

		System.exit(code);
	}
}
