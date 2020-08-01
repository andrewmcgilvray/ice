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
package com.netflix.ice.processor;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netflix.ice.common.AwsUtils;

public class CostAndUsageReport extends MonthlyReport {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    public static final DateTimeFormatter billingPeriodDateFormat = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ").withZone(DateTimeZone.UTC);
    
	private Manifest manifest = null;
	
	public CostAndUsageReport(S3ObjectSummary s3ObjectSummary, BillingBucket billingBucket, MonthlyReportProcessor processor) {
		super(s3ObjectSummary, billingBucket, processor);
		
		// Download the manifest
        String fileKey = s3ObjectSummary.getKey();
        logger.info("trying to download " + fileKey + "...");
        byte[] manifestBytes = AwsUtils.readManifest(s3ObjectSummary.getBucketName(), billingBucket.s3BucketRegion, fileKey,
                billingBucket.accountId, billingBucket.accessRoleName, billingBucket.accessExternalId);
        Gson gson = new GsonBuilder().create();
        boolean downloaded = false;
        try {
        	manifest = gson.fromJson(new String(manifestBytes, "UTF-8"), Manifest.class);
        	downloaded = true;
        }
        catch (Exception e) {
        	logger.error("Exception: " + e);
        }
        if (downloaded)
        	logger.info("downloaded " + fileKey);
	}
		
	/**
	 * Constructor used for testing only
	 */
	public CostAndUsageReport(S3ObjectSummary s3ObjectSummary, File manifest, MonthlyReportProcessor processor) {
		super(s3ObjectSummary, new BillingBucket(null, null, null, null, null, null, "", ""), processor);
		if (manifest == null)
			return;
		
        Reader reader;
        try {
			reader = new BufferedReader(new FileReader(manifest));
	        Gson gson = new GsonBuilder().create();
		    this.manifest = gson.fromJson(reader, Manifest.class);
		} catch (FileNotFoundException e) {
			fail("Failed to parse manifest file" + e);
			return;
		}
	}
	
	public class Column {
		public String category;
		public String name;
	}
	
	public class BillingPeriod {
		public String start;
		public String end;
	}
	
	public class Manifest {
		public String assemblyId;
		public String account;
		public Column[] columns;
		public String charset;
		public String compression;
		public String contentType;
		public String reportId;
		public String reportName;
		public BillingPeriod billingPeriod;
		public String bucket;
		public String[] reportKeys;
		public String[] additionalArtifactKeys;
		
		public boolean hasTags() {
			for (Column column: columns) {
				if (column.category.equals("resourceTags")) {
					return true;
				}
			}
			return false;
		}

		/*
		 * Scan the column names looking for a match and return the index.
		 * Return -1 if not found.
		 */
		public int getColumnIndex(String category, String name) {
			for (int i = 0; i < columns.length; i++) {
				if (columns[i].category.equals(category)) {
					if (columns[i].name.equals(name))
						return i;
				}
			}
			return -1;
		}

		public int getCategoryStartIndex(String category) {
			for (int i = 0; i < columns.length; i++) {
				if (columns[i].category.equals(category))
					return i;
			}
			return -1;
		}
		
		public int getCategoryEndIndex(String category) {
			boolean found = false;
			int i;
			for (i = 0; i < columns.length; i++) {
				if (columns[i].category.equals(category))
					found = true;
				else if (found)
					return i;
			}
			// we get here if we didn't find the category or the category was the last one.
			return found ? i : -1;
		}
		
		public String[] getCategoryHeader(String category) {
			for (int i = 0; i < columns.length; i++) {
				if (columns[i].category.equals(category)) {
					List<String> header = Lists.newArrayList();
					for (int j = i; j < columns.length && columns[j].category.equals(category); j++) {
						header.add(columns[j].category + "/" + columns[j].name);
					}
					return header.toArray(new String[header.size()]);
				}
			}
			return new String[]{};
		}
	}
	
	public DateTime getStartTime() {
		return billingPeriodDateFormat.parseDateTime(manifest.billingPeriod.start);
	}

	public DateTime getEndTime() {
		return billingPeriodDateFormat.parseDateTime(manifest.billingPeriod.end);
	}

	public int getCategoryStartIndex(String category) {
		return manifest.getCategoryStartIndex(category);
	}
	
	public int getCategoryEndIndex(String category) {
		return manifest.getCategoryEndIndex(category);
	}
	
	public String[] getCategoryHeader(String category) {
		return manifest.getCategoryHeader(category);
	}

	public int getColumnIndex(String category, String name) {
		return manifest.getColumnIndex(category, name);
	}

	@Override
	public boolean hasTags() {
		return manifest == null ? false : manifest.hasTags();
	}

	@Override
	public String[] getReportKeys() {
		return manifest.reportKeys;
	}
}
