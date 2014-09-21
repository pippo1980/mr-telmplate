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

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by pippo on 14-9-19.
 */
public class StatusKeyGroupComparator extends WritableComparator {

	public StatusKeyGroupComparator() {
		super(StatusKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Validate.notNull(a, "key can not be null!!");
		Validate.notNull(b, "key can not be null!!");

		StatusKey key1 = (StatusKey) a;
		StatusKey key2 = (StatusKey) b;

		Validate.notNull(key1.userId, "key:[%s] user id is null", key1);
		Validate.notNull(key2.userId, "key:[%s] user id is null", key2);

		return key1.userId.compareTo(key2.userId);
	}
}
