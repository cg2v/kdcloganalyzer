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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class UserTimeReducer extends Reducer<Text, UserTimeRec, Text, Text> {

	protected void reduce(Text key, Iterable<UserTimeRec> values, Context context) throws IOException, InterruptedException {
		UserTimeRec result = new UserTimeRec();
		for (UserTimeRec value : values)
			result.merge(value);
		context.write(key, new Text(result.tsv()));
	}
}
