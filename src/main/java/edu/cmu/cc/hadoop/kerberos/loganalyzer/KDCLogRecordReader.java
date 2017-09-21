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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class KDCLogRecordReader extends
				    RecordReader<LongWritable, KDCLogRecord> {
    private static final Log LOG = LogFactory.getLog(KDCLogRecordReader.class);

    private final static String TIMESTAMP = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}";
    private final static String ATOM = "[-/_\\.a-zA-Z0-9]+";
    private final static String REALM = "[-\\.a-zA-Z0-9]+";
    private final Pattern matchInitial = Pattern.compile
	("(" + TIMESTAMP +
	 ")\\s+((?:AS|TGS)-REQ)\\s+(" + ATOM +
	 ")@(" + REALM + ")\\s+from\\s+(IPv4:[\\d\\.]+|IPv6:[\\p{XDigit}\\.:]+)\\s+for\\s+(" + ATOM +
	 ")@(" + REALM + ")");
    private final String matchPreauthSuccess = ".*\\bPre-authentication succeeded\\b.*";
    private final String matchBadPassword = ".*\\bFailed to decrypt PA-DATA --.*";
    private final String matchBadClient = ".*\\bUNKNOWN --.*";
    private final String matchExpiredClient = ".*\\bClient expired\\b*";
    private final String matchKeyExpiredClient = ".*\\bClient's key has expired\\b.*";
    private final String matchKeyExpiredServer = ".*\\bServer's key has expired\\b.*";
    private final String matchNotServer = ".*\\bPrincipal may not act as server\\b.*";
    private final String matchNotClient = ".*\\bPrincipal may not act as client\\b.*";
    private final String matchTicketExpired = ".*\\bTicket expired";
    private final String matchTimeSkew = ".*\\bToo large time skew";
    private final String matchNoPreauthKey = ".*\\bNo key matches pa-data\\b.*";
    private final String matchBadTGS = ".*\\bkrb_rd_req:.*";
    private final String matchFailedTGS = ".*\\bFailed building TGS-REP\\b.*";
    private final String matchBadServer = ".*\\bServer not found in database:?\\b.*";
    private final String matchBadClient2 = ".*\\bClient no longer in database?:\\b.*";
    private final String matchBadClient3 = ".*\\bClient not found in database:?\\b.*";
    private final String matchBadU2UEtype = ".*\\bAddition ticket have not matching etypes\\b.*";
    private final String matchBadServerEtype = ".*\\bServer \\(.*\\) has no support.*\\betypes\\b.*";
    private final String matchUnsatisfiableRenew =".*\\bBad request for renewable ticket";
    private final String matchNotRenewable = ".*\\request to renew non-renewable ticket";
    private final String matchNotForwardable = ".*\\bRequest to forward non-forwardable ticket";
    private final Pattern matchNoVerifyTGS = Pattern.compile("(" + TIMESTAMP + ")\\s+(Failed to verify AP-REQ:.*)");
    private final Pattern matchFailedVerify = Pattern.compile("(" + TIMESTAMP + ")\\s+(Failed to verify (checksum|authenticator).*)");
    private final String matchReferral =".*\\b[Rr]eturning a referral to realm.*";

    private final Pattern matchSending = Pattern.compile
	(TIMESTAMP + "\\s+sending\\s+\\d+\\s+bytes\\s+to\\s+" +
	 "(?:IPv4:[\\d\\.]+|IPv6:[\\p{XDigit}\\.:]+)");

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private KDCLogRecord value;
    private Text buffer;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;

    @Override
    public void close() throws IOException {
	try {
	    if (in != null) {
		in.close();
	    }
	} finally {
	    if (decompressor != null) {
		CodecPool.returnDecompressor(decompressor);
	    }
	}
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
					       InterruptedException {
	return key;
    }

    @Override
    public KDCLogRecord getCurrentValue() throws IOException,
						 InterruptedException {
	return value;
    }

    private boolean isCompressedInput() {
	return (codec != null);
    }

    private int maxBytesToConsume(long pos) {
	return isCompressedInput()
	    ? Integer.MAX_VALUE
	    : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
	long retVal;
	if (isCompressedInput() && null != filePosition) {
	    retVal = filePosition.getPos();
	} else {
	    retVal = pos;
	}
	return retVal;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
	if (start == end) {
	    return 0.0f;
	} else {
	    try {
		return Math.min(1.0f, (getFilePosition() - start)
				/ (float) (end - start));
	    } catch (IOException ioe) {
		throw new RuntimeException(ioe);
	    }
	}
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
	throws IOException, InterruptedException {
	FileSplit split = (FileSplit) arg0;
	Configuration job = arg1.getConfiguration();
	this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
					Integer.MAX_VALUE);
	start = split.getStart();
	end = start + split.getLength();
	final Path file = split.getPath();
	compressionCodecs = new CompressionCodecFactory(job);
	codec = compressionCodecs.getCodec(file);

	// open the file and seek to the start of the split
	FileSystem fs = file.getFileSystem(job);
	FSDataInputStream fileIn = fs.open(split.getPath());

	if (isCompressedInput()) {
	    decompressor = CodecPool.getDecompressor(codec);
	    if (codec instanceof SplittableCompressionCodec) {
		final SplitCompressionInputStream cIn =
		    ((SplittableCompressionCodec)codec).createInputStream
		    (fileIn, decompressor, start, end,
		     SplittableCompressionCodec.READ_MODE.BYBLOCK);
		in = new LineReader(cIn, job);
		start = cIn.getAdjustedStart();
		end = cIn.getAdjustedEnd();
		filePosition = cIn;
	    } else {
		in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
		filePosition = fileIn;
	    }
	} else {
	    fileIn.seek(start);
	    in = new LineReader(fileIn, job);
	    filePosition = fileIn;
	}
	// If this is not the first split, we always throw away first record
	// because we always (except the last split) read one extra line in
	// next() method.
	if (start != 0) {
	    start += in.readLine(new Text(), 0, maxBytesToConsume(start));
	}
	this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
	// FIXME not done
	if (key == null) {
	    key = new LongWritable();
	}
	key.set(pos);
	if (value == null) {
	    value = new KDCLogRecord();
	}
	if (buffer == null) {
	    buffer = new Text();
	}
	int newSize = 0;
	// We always read one extra line, which lies outside the upper
	// split limit i.e. (end - 1)
 	boolean complete = false;
	while (complete == false && getFilePosition() <= end) {
	    newSize = in.readLine(buffer, maxLineLength,
				  Math.max(maxBytesToConsume(pos),
					   maxLineLength));
	    if (newSize == 0) {
		break;
	    }
	    pos += newSize;
	    if (newSize < maxLineLength) {
		String current = buffer.toString();
		Matcher m = matchInitial.matcher(current);

		if (m.find()) {
		    String ts = m.group(1);
		    String reqtype = m.group(2);
		    String client = m.group(3);
		    String crealm = m.group(4);
		    String ip = m.group(5);
		    String server = m.group(6);
		    String srealm = m.group(7);

		    value.setTime(ts);
		    switch (reqtype) {
		    case "AS-REQ":
			value.setRequestType(ReqType.AUTH);
			break;
		    case "TGS-REQ":
			value.setRequestType(ReqType.TGS);
			value.setSuccess(true); // TGS-REQ assumed to succeed
			break;
		    default:
			LOG.error("Request type " + reqtype + " not recognized");
			value.setRequestType(ReqType.UNKNOWN);
			break;
		    }
		    value.setClient(client);
		    value.setCRealm(crealm);
		    value.setService(server);
		    value.setSRealm(srealm);
		    value.setClientip(ip);
		    value.setValid(true);
		    continue;
		}
		Matcher m1 = matchSending.matcher(current);
		if (m1.find()) {
		    complete = true;
		} else if (Pattern.matches(matchPreauthSuccess, current)) {
		    value.setSuccess(true); // AS-REQ only succeeds with preauth
		} else if (Pattern.matches(matchBadPassword, current) ||
			   Pattern.matches(matchBadClient, current) ||
			   Pattern.matches(matchExpiredClient, current) ||
			   Pattern.matches(matchTimeSkew, current) ||
			   Pattern.matches(matchNoPreauthKey, current) ||
			   Pattern.matches(matchBadTGS, current) ||
			   Pattern.matches(matchBadClient2, current) ||
			   Pattern.matches(matchBadClient3, current) ||
			   Pattern.matches(matchBadServer, current) ||
			   Pattern.matches(matchBadServerEtype, current) ||
			   Pattern.matches(matchBadU2UEtype, current)||
			   Pattern.matches(matchUnsatisfiableRenew, current) ||
			   Pattern.matches(matchKeyExpiredClient, current) ||
			   Pattern.matches(matchKeyExpiredServer, current) ||
			   Pattern.matches(matchNotServer, current) ||
			   Pattern.matches(matchNotClient, current) ||
			   Pattern.matches(matchTicketExpired, current) ||
			   Pattern.matches(matchNotForwardable, current) ||
			   Pattern.matches(matchNotRenewable, current) ||
			   Pattern.matches(matchFailedTGS, current)) {
		    value.setErrorIfUnset(current);
		} else if (Pattern.matches(matchReferral, current)) {
		    value.setReferral(true);
		} else {
		    Matcher m2 = matchNoVerifyTGS.matcher(current);
		    Matcher m3 = matchFailedVerify.matcher(current);
		    if (m2.find()) {
			value.setTime(m2.group(1));
			value.setErrorIfUnset(m2.group(2));
		    } else if (m3.find()) {
			value.setTime(m3.group(1));
			value.setErrorIfUnset(m3.group(2));
		    }
		}
	    } else {
		// line too long. try again
		LOG.info("Skipped line of size " + newSize + " at pos " +
			 (pos - newSize));
	    }
	}
	if (!complete) {
	    key = null;
	    value = null;
	    return false;
	} else {
	    return true;
	}
    }
}
