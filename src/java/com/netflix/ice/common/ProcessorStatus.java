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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.gson.Gson;

public class ProcessorStatus {
	public String month;
	public Collection<Report> reports;
	public String lastProcessed;
	public boolean reprocess;
	
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
	}
	
	public ProcessorStatus(String month, Collection<Report> reports, String lastProcessed) {
		this.month = month;
		this.reports = reports;
		this.lastProcessed = lastProcessed;
		this.reprocess = false;
	}

	public ProcessorStatus(String json) {
		Gson gson = new Gson();
		ProcessorStatus ps = gson.fromJson(json, this.getClass());
		this.month = ps.month;
		this.reports = ps.reports;
		this.lastProcessed = ps.lastProcessed;
		this.reprocess = ps.reprocess;
	}
	
	public String toJSON() {
		Gson gson = new Gson();
    	return gson.toJson(this);
	}

	public DateTime getLastProcessed() {
		return new DateTime(lastProcessed, DateTimeZone.UTC);
	}
}
