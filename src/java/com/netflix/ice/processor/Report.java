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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.netflix.ice.processor.config.S3BucketConfig;

public class Report {
	protected String payerAccountId;
	protected S3ObjectSummary s3ObjectSummary;
	protected S3BucketConfig s3BucketConfig;

	public Report() {
	}

	public Report withPayerAccountId(String payerAccountId) {
		this.payerAccountId = payerAccountId;
		return this;
	}
	
	public Report withS3ObjectSummary(S3ObjectSummary s3ObjectSummary) {
		this.s3ObjectSummary = s3ObjectSummary;
		return this;
	}
	
	public Report withS3BucketConfig(S3BucketConfig s3BucketConfig) {
		this.s3BucketConfig = s3BucketConfig;
		return this;
	}
	
	public Report(S3ObjectSummary s3ObjectSummary, S3BucketConfig s3BucketConfig) {
        this.s3ObjectSummary = s3ObjectSummary;
        this.s3BucketConfig = s3BucketConfig;
	}

	public long getLastModifiedMillis() {
		return s3ObjectSummary.getLastModified().getTime();
	}

	public String getReportKey() {
		return s3ObjectSummary.getKey();
	}

	public S3ObjectSummary getS3ObjectSummary() {
		return s3ObjectSummary;
	}

	public S3BucketConfig getS3BucketConfig() {
		return s3BucketConfig;
	}

}