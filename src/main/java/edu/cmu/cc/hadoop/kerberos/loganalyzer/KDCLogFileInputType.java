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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/*
 * 
 * sample output
 * 2015-11-22T15:25:20 AS-REQ cg2v@SQUILL.DEMENTIA.ORG from IPv4:127.0.0.1 for krbtgt/SQUILL.DEMENTIA.ORG@SQUILL.DEMENTIA.ORG
 * 2015-11-22T15:25:20 Client sent patypes: ENC-TS, REQ-ENC-PA-REP
 * 2015-11-22T15:25:20 Looking for PK-INIT(ietf) pa-data -- cg2v@SQUILL.DEMENTIA.ORG
 * 2015-11-22T15:25:20 Looking for PK-INIT(win2k) pa-data -- cg2v@SQUILL.DEMENTIA.ORG
 * 2015-11-22T15:25:20 Looking for ENC-TS pa-data -- cg2v@SQUILL.DEMENTIA.ORG
 * 2015-11-22T15:25:20 ENC-TS Pre-authentication succeeded -- cg2v@SQUILL.DEMENTIA.ORG using aes256-cts-hmac-sha1-96
 * 2015-11-22T15:25:20 ENC-TS pre-authentication succeeded -- cg2v@SQUILL.DEMENTIA.ORG
 * 2015-11-22T15:25:20 AS-REQ authtime: 2015-11-22T15:25:20 starttime: unset endtime: 2015-11-23T15:25:20 renew till: unset
 * 2015-11-22T15:25:20 Client supported enctypes: aes256-cts-hmac-sha1-96, aes128-cts-hmac-sha1-96, des3-cbc-sha1, arcfour-hmac-md5, 25, 26, des-cbc-crc, des-cbc-md5, des-cbc-md4, using aes256-cts-hmac-sha1-96/aes256-cts-hmac-sha1-96
 * 2015-11-22T15:25:20 Requested flags: renewable-ok, proxiable, forwardable
 * 2015-11-22T15:25:20 sending 752 bytes to IPv4:127.0.0.1
*/

public class KDCLogFileInputType extends FileInputFormat<LongWritable, KDCLogRecord> {

	@Override
	public RecordReader<LongWritable, KDCLogRecord> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	  public List<InputSplit> getSplits(JobContext job) 
			  throws IOException { 
			    List<InputSplit> splits = new ArrayList<InputSplit>(); 
			     
			    for (FileStatus status : listStatus(job)) { 
			    	
			    }
			    return splits;
	  }
}
