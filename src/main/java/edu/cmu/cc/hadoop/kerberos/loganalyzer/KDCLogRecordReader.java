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
    private final String matchPreauthSuccess = "Pre-authentication succeeded";
    private final String matchBadPassword = "Failed to decrypt PA-DATA --";
    private final String matchBadClient = "UNKNOWN --";
    private final String matchExpiredClient = "Client expired";
    private final String matchKeyExpiredClient = "Client's key has expired";
    private final String matchKeyExpiredServer = "Server's key has expired";
    private final String matchNotServer = "Principal may not act as server";
    private final String matchNotClient = "Principal may not act as client";
    private final String matchTicketExpired = "Ticket expired";
    private final String matchTimeSkew = "Too large time skew";
    private final String matchNoPreauthKey = "No key matches pa-data";
    private final String matchBadTGS = "krb_rd_req:";
    private final String matchFailedTGS = "Failed building TGS-REP";
    private final String matchBadServer = "Server not found in database";
    private final String matchBadClient2 = "Client no longer in database";
    private final String matchBadClient3 = "Client not found in database";
    private final String matchBadU2UEtype = "Addition ticket have not matching etypes";
    private final String matchUnsatisfiableRenew = "Bad request for renewable ticket";
    private final String matchNotRenewable = "equest to renew non-renewable ticket";
    private final String matchNotForwardable = "equest to forward non-forwardable ticket";
    private final Pattern matchBadServerEtype = Pattern.compile(".*\\bServer \\(.*\\) has no support.*\\betypes\\b.*");
    private final Pattern matchNoVerifyTGS = Pattern.compile("(" + TIMESTAMP + ")\\s+(Failed to verify AP-REQ:.*)");
    private final Pattern matchFailedVerify = Pattern.compile("(" + TIMESTAMP + ")\\s+(Failed to verify (checksum|authenticator).*)");
    private final String matchReferral = "eturning a referral to realm";
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
    private boolean positionIsAbsolute = false;

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
		long tstart = start;
		if (getFilePosition() < start || positionIsAbsolute) {
			positionIsAbsolute = true;
			tstart = 0;
		}
		return Math.min(1.0f, (getFilePosition() - tstart)
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
	value = new KDCLogRecord();

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
		} else if (current.contains(matchPreauthSuccess)) {
		    value.setSuccess(true); // AS-REQ only succeeds with preauth
		} else if (current.contains(matchBadPassword)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.BAD_PASSWORD);
	    } else if (current.contains(matchBadClient) ||
				   current.contains(matchBadClient2) ||
				   current.contains(matchBadClient3) ||
				   current.contains(matchBadServer)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.BAD_NAME);
	    } else if (current.contains(matchExpiredClient) ||
				   current.contains(matchKeyExpiredClient) ||
				   current.contains(matchKeyExpiredServer) ||
				   current.contains(matchNotServer) ||
				   current.contains(matchNotClient)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.UNUSABLE_NAME);
	    } else if (current.contains(matchBadTGS) ||
			   current.contains(matchTimeSkew) ||
			   current.contains(matchNoPreauthKey) ||
			   matchBadServerEtype.matcher(current).matches() ||
			   current.contains(matchBadU2UEtype) ||
			   current.contains(matchUnsatisfiableRenew) ||
			   current.contains(matchTicketExpired)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.BAD_AUTHENTICATION);
	    } else if (current.contains(matchNotForwardable) ||
			   current.contains(matchNotRenewable)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.BAD_PARAMETERS);
	    } else if (current.contains(matchFailedTGS)) {
			value.setErrorIfUnset(current, KDCLogErrorClass.UNKNOWN);
		} else if (current.contains(matchReferral)) {
			value.setReferral(true);
		} else {
		    Matcher m2 = matchNoVerifyTGS.matcher(current);
		    Matcher m3 = matchFailedVerify.matcher(current);
		    if (m2.find()) {
			value.setTime(m2.group(1));
			value.setErrorIfUnset(m2.group(2), KDCLogErrorClass.BAD_AUTHENTICATION);
		    } else if (m3.find()) {
			value.setTime(m3.group(1));
			value.setErrorIfUnset(m3.group(2), KDCLogErrorClass.BAD_AUTHENTICATION);
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
