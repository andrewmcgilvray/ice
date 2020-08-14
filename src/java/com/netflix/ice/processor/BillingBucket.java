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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;

public class BillingBucket {
    public String accountId;
    public String s3BucketName;
    public String s3BucketRegion;
    public String s3BucketPrefix;
    public String accessRoleName;
    public String accessExternalId;
    public String rootName;
    public String configBasename;
    
    public BillingBucket() {
    	this.rootName = "";
    	this.configBasename = "";
    }
    
    public BillingBucket withAccountId(String accountId) {
    	this.accountId = accountId;
    	return this;
    }
    
    public BillingBucket withS3BucketName(String s3BucketName) {
    	this.s3BucketName = s3BucketName;
    	return this;
    }
    
    public BillingBucket withS3BucketRegion(String s3BucketRegion) {
    	this.s3BucketRegion = s3BucketRegion;
    	return this;
    }
    
    public BillingBucket withS3BucketPrefix(String s3BucketPrefix) {
    	this.s3BucketPrefix = s3BucketPrefix;
    	return this;
    }
    
    public BillingBucket withAccessRoleName(String accessRoleName) {
    	this.accessRoleName = accessRoleName;
    	return this;
    }
    
    public BillingBucket withAccessExternalId(String accessExternalId) {
    	this.accessExternalId = accessExternalId;
    	return this;
    }
    
    public BillingBucket withRootName(String rootName) {
    	this.rootName = rootName;
    	return this;
    }
    
    public BillingBucket withConfigBasename(String configBasename) {
    	this.configBasename = configBasename;
    	return this;
    }
    
    /**
     * Constructor for deserializing from JSON or YAML
     * 
     * @param in String to parse.
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public BillingBucket(String in) throws JsonParseException, JsonMappingException, IOException {
    	BillingBucket bb = null;
    	
		if (in.trim().startsWith("{")) {
			Gson gson = new Gson();
			bb = gson.fromJson(in, getClass());
		}
		else {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			bb = mapper.readValue(in, getClass());			
		}
    	this.s3BucketName = bb.s3BucketName;
    	this.s3BucketRegion = bb.s3BucketRegion;
    	this.s3BucketPrefix = bb.s3BucketPrefix;
    	this.accountId = bb.accountId;
    	this.accessRoleName = bb.accessRoleName;
    	this.accessExternalId = bb.accessExternalId;
    	this.rootName = bb.rootName;
    	this.configBasename = bb.configBasename;
    }
}