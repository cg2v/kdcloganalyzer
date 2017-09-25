/*******************************************************************************
 * Copyright (c) 2015 Chaskiel Grundman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package edu.cmu.cc.hadoop.kerberos.loganalyzer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class UserTimeMapper extends
		Mapper<Object, KDCLogRecord, Text, UserTimeRec> {
	protected void map(Object key, KDCLogRecord value, Context context)
			throws IOException, InterruptedException {
		if (!value.isValid()) {
			context.getCounter("Record Type","Invalid").increment(1);
			context.getCounter("Rejected Records","Invalid").increment(1);
			return;
		}
		context.getCounter("Record Type", value.getRequestType().name()).increment(1);
		if (!value.isSuccess()) {
			context.getCounter("Rejected Records","Failed").increment(1);
			if (value.getErrorClass() != null && value.getErrorClass() != KDCLogErrorClass.NO_ERROR) {
				context.getCounter("Kerberos Errors", value.getErrorClass().name()).increment(1);
			} else {
				context.getCounter("Kerberos Errors", "Missing Pre-authentication").increment(1);
			}
			return;
		}
		if (value.getRequestType() != ReqType.AUTH) {
			context.getCounter("Rejected Records","Request Type").increment(1);
			return;
		}
		if (value.isReferral()) {
			context.getCounter("Rejected Records","Referral").increment(1);
			return;	
		}

		Text user = new Text(value.getClient());
		UserTimeRec data = new UserTimeRec();
		data.setFirstts(value.getTime());
		data.setLastts(value.getTime());
		data.setCount(1);
		context.write(user, data);
	}

}
