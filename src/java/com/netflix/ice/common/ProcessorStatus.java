/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.common;

import java.util.Collection;

import com.google.gson.Gson;

public class ProcessorStatus implements Comparable<ProcessorStatus> {
	public static final String prefix = "processorStatus_";
	public static final String suffix = ".json";
	
	public String month;
	public Collection<Report> reports;
	public String lastProcessed;
	public boolean reprocess;
	public String elapsedTime; // How long it took to process the month
	
	public static class Report {
		public String accountName;
		public String accountId;
		public String key;
		public String lastModified;
		
		public Report(String accountName, String accountId, String key, String lastModified) {
			this.accountName = accountName;
			this.accountId = accountId;
			this.key = key;
			this.lastModified = lastModified;
		}

		public String getAccountName() {
			return accountName;
		}

		public String getAccountId() {
			return accountId;
		}

		public String getKey() {
			return key;
		}

		public String getLastModified() {
			return lastModified;
		}
	}
	
	public ProcessorStatus(String month, Collection<Report> reports, String lastProcessed, String elapsedTime) {
		this.month = month;
		this.reports = reports;
		this.lastProcessed = lastProcessed;
		this.reprocess = false;
		this.elapsedTime = elapsedTime;
	}

	public ProcessorStatus(String json) {
		Gson gson = new Gson();
		ProcessorStatus ps = gson.fromJson(json, this.getClass());
		this.month = ps.month;
		this.reports = ps.reports;
		this.lastProcessed = ps.lastProcessed;
		this.reprocess = ps.reprocess;
		this.elapsedTime = ps.elapsedTime;
	}
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}

	public String getMonth() {
		return month;
	}

	public Collection<Report> getReports() {
		return reports;
	}

	public String getLastProcessed() {
		return lastProcessed;
	}

	public boolean isReprocess() {
		return reprocess;
	}
	
	public String getElapsedTime() {
		return elapsedTime;
	}

	@Override
	public int compareTo(ProcessorStatus o) {
		return month.compareTo(o.month);
	}
}
