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
