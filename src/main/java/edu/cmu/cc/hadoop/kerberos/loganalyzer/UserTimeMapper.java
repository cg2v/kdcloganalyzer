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


public class UserTimeMapper extends Mapper<Object, KDCLogRecord, Text, UserTimeRec> {
	protected void map(Object key, KDCLogRecord value, Context context) throws IOException, InterruptedException
	{
		if (!value.isValid() || !value.isSuccess() || value.isReferral() ||
				value.getRequestType() != ReqType.AUTH)
			return;
		
		Text user = new Text(value.getClient());
		UserTimeRec data = new UserTimeRec();
		data.setFirstts(value.getTime());
		data.setLastts(value.getTime());
		data.setCount(1);
		context.write(user, data);
	}

}
