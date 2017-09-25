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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserTimeRec implements WritableComparable<UserTimeRec> {

	private String firstts;
	private String lastts;
	private int count;
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		firstts = arg0.readUTF();
		lastts = arg0.readUTF();
		count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(firstts);
		arg0.writeUTF(lastts);
		arg0.write(count);
	}

	@Override
	public int compareTo(UserTimeRec arg0) {
		if (firstts == null) {
			if (arg0.firstts != null)
				return -1;
		} else {
			if (!firstts.equals(arg0.firstts))
				return firstts.compareTo(arg0.firstts);
		}
		if (lastts == null) {
			if (arg0.lastts != null)
				return -1;
		} else {
			if (!lastts.equals(arg0.lastts))
				return lastts.compareTo(arg0.lastts);
		}
		return count - arg0.count;
	}

	public String getFirstts() {
		return firstts;
	}

	public void setFirstts(String firstts) {
		this.firstts = firstts;
	}

	public String getLastts() {
		return lastts;
	}

	public void setLastts(String lastts) {
		this.lastts = lastts;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + ((firstts == null) ? 0 : firstts.hashCode());
		result = prime * result + ((lastts == null) ? 0 : lastts.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserTimeRec other = (UserTimeRec) obj;
		if (count != other.count)
			return false;
		if (firstts == null) {
			if (other.firstts != null)
				return false;
		} else if (!firstts.equals(other.firstts))
			return false;
		if (lastts == null) {
			if (other.lastts != null)
				return false;
		} else if (!lastts.equals(other.lastts))
			return false;
		return true;
	}
	public void merge(UserTimeRec arg0) {
		if (firstts == null || firstts.compareTo(arg0.firstts) < 0)
			firstts = arg0.firstts;
		if (lastts == null || lastts.compareTo(arg0.lastts) > 0)
			lastts = arg0.lastts;
		count = count + arg0.count;
	}

	public String tsv() {
		return String.format("%s\t%s\t%d", firstts, lastts, count);
	}
}
