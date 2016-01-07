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

public class KDCLogRecord implements WritableComparable<KDCLogRecord> {
	private boolean valid;
	private String ts;
	private ReqType rt;
	private String client;
	private String crealm;
	private String service;
	private String srealm;
	private String clientip;
	private boolean success;
	private boolean referral;
	private String error;

	public boolean isValid() {
		return valid;
	}
	
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	public String getTime() {
		return ts;
	}

	public void setTime(String ts) {
		this.ts = ts;
	}

	public ReqType getRequestType() {
		return rt;
	}

	public void setRequestType(ReqType rt) {
		this.rt = rt;
	}

	public String getClient() {
		return client;
	}

	public void setClient(String user) {
		this.client = user;
	}

	public String getCRealm() {
		return crealm;
	}

	public void setCRealm(String crealm) {
		this.crealm = crealm;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getSRealm() {
		return srealm;
	}

	public void setSRealm(String srealm) {
		this.srealm = srealm;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public boolean isReferral() {
		return referral;
	}

	public void setReferral(boolean referral) {
		this.referral = referral;
	}

	public String getClientip() {
		return clientip;
	}

	public void setClientip(String clientip) {
		this.clientip = clientip;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((clientip == null) ? 0 : clientip.hashCode());
		result = prime * result + ((rt == null) ? 0 : rt.hashCode());
		result = prime * result + ((service == null) ? 0 : service.hashCode());
		result = prime * result + ((ts == null) ? 0 : ts.hashCode());
		result = prime * result + ((client == null) ? 0 : client.hashCode());
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
		KDCLogRecord other = (KDCLogRecord) obj;
		if (valid != other.valid)
			return false;
		if (!valid) {
			if (error == null) {
				if (other.error != null)
					return false;
			} else {
				return error.equals(other.error);
			}
		}
		if (clientip == null) {
			if (other.clientip != null)
				return false;
		} else if (!clientip.equals(other.clientip))
			return false;
		if (rt != other.rt)
			return false;
		if (service == null) {
			if (other.service != null)
				return false;
		} else if (!service.equals(other.service))
			return false;
		if (ts == null) {
			if (other.ts != null)
				return false;
		} else if (!ts.equals(other.ts))
			return false;
		if (client == null) {
			if (other.client != null)
				return false;
		} else if (!client.equals(other.client))
			return false;
		return true;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
	public void setErrorIfUnset(String error) {
		if (this.error == null) {
			this.error = error;
		}
		this.success = false;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		ts=arg0.readUTF();
		valid=arg0.readBoolean();
		if (valid) { // valid record contains all fields. may or may not have error.
			String rts=arg0.readUTF();
			rt=ReqType.valueOf(rts);
			client=arg0.readUTF();
			service=arg0.readUTF();
			clientip=arg0.readUTF();
			referral=arg0.readBoolean();
			success=arg0.readBoolean();
		} else { // invalid record contains only error message
			rt=ReqType.UNKNOWN;
			client=null;
			service=null;
			clientip=null;
			referral=false;
			success=false;
		}
		if (!success)
			error=arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {

		arg0.writeUTF(ts);
		arg0.writeBoolean(valid);
		if (valid) {
			arg0.writeUTF(rt.name());
			arg0.writeUTF(client);
			arg0.writeUTF(service);
			arg0.writeUTF(clientip);
			arg0.writeBoolean(referral);
			arg0.writeBoolean(success);
		}
		if (!valid || !success)
			arg0.writeUTF(error);
	}

	@Override
	public int compareTo(KDCLogRecord arg0) {
		if (!ts.equals(arg0.ts)) {
			if (ts == null)
				return -1;
			if (arg0.ts == null)
				return 1;
			return ts.compareTo(arg0.ts);
		}
		if (!rt.equals(arg0.rt))
			return rt.compareTo(arg0.rt);
		if (!ts.equals(arg0.ts)) {
			if (ts == null)
				return -1;
			if (arg0.ts == null)
				return 1;
			return ts.compareTo(arg0.ts);
		}
		if (!ts.equals(arg0.ts)) {
			if (ts == null)
				return -1;
			if (arg0.ts == null)
				return 1;
			return ts.compareTo(arg0.ts);
		}
		if (!ts.equals(arg0.ts)) {
			if (ts == null)
				return -1;
			if (arg0.ts == null)
				return 1;
			return ts.compareTo(arg0.ts);
		}
		return 0;
	}
}