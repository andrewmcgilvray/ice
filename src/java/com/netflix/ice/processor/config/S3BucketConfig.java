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
package com.netflix.ice.processor.config;

public class S3BucketConfig {
	// S3 bucket access information for various reports
	private String name;
	private String region;
	private String prefix;
	private String accountId;
	private String accessRole;
	private String externalId;
	
	public S3BucketConfig() {
	}
	
    public S3BucketConfig withName(String name) {
    	this.name = name;
    	return this;
    }
    
    public S3BucketConfig withRegion(String region) {
    	this.region = region;
    	return this;
    }
    
    public S3BucketConfig withPrefix(String prefix) {
    	this.prefix = prefix;
    	return this;
    }
    
    public S3BucketConfig withAccountId(String accountId) {
    	this.accountId = accountId;
    	return this;
    }
    
    public S3BucketConfig withAccessRole(String accessRole) {
    	this.accessRole = accessRole;
    	return this;
    }
    
    public S3BucketConfig withExternalId(String externalId) {
    	this.externalId = externalId;
    	return this;
    }
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getAccountId() {
		return accountId;
	}
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	public String getAccessRole() {
		return accessRole;
	}
	public void setAccessRole(String accessRole) {
		this.accessRole = accessRole;
	}
	public String getExternalId() {
		return externalId;
	}
	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}
}
